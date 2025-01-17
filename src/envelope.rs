use std::marker::PhantomData;

use catty::{Receiver, Sender};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

#[cfg(feature = "timeout")]
use futures_util::future::Either;

use crate::context::Context;
use crate::{Actor, Handler, Message};

/// A message envelope is a struct that encapsulates a message and its return channel sender (if applicable).
/// Firstly, this allows us to be generic over returning and non-returning messages (as all use the
/// same `handle` method and return the same pinned & boxed future), but almost more importantly it
/// allows us to erase the type of the message when this is in dyn Trait format, thereby being able to
/// use only one channel to send all the kinds of messages that the actor can receives. This does,
/// however, induce a bit of allocation (as envelopes have to be boxed).
pub(crate) trait MessageEnvelope: Send {
    /// The type of actor that this envelope carries a message for
    type Actor;

    /// Handle the message inside of the box by calling the relevant `AsyncHandler::handle` or
    /// `Handler::handle` method, returning its result over a return channel if applicable. The
    /// reason that this returns a future is so that we can propagate any `Handler` responder
    /// futures upwards and `.await` on them in the manager loop. This also takes `Box<Self>` as the
    /// `self` parameter because `Envelope`s always appear as `Box<dyn Envelope<Actor = ...>>`,
    /// and this allows us to consume the envelope, meaning that we don't have to waste *precious
    /// CPU cycles* on useless option checks.
    ///
    /// # Doesn't the return type induce *Unnecessary Boxing* for synchronous handlers?
    /// To save on boxing for non-asynchronously handled message envelopes, we *could* return some
    /// enum like:
    ///
    /// ```not_a_test
    /// enum Return<'a> {
    ///     Fut(BoxFuture<'a, ()>),
    ///     Noop,
    /// }
    /// ```
    ///
    /// But this is actually about 10% *slower* for `do_send`. I don't know why. Maybe it's something
    /// to do with branch (mis)prediction or compiler optimisation. If you think that you can get
    /// it to be faster, then feel free to open a PR with benchmark results attached to prove it.
    fn handle<'a>(
        self: Box<Self>,
        act: &'a mut Self::Actor,
        ctx: &'a mut Context<Self::Actor>,
    ) -> BoxFuture<'a, ()>;
}

/// An envelope that returns a result from a message. Constructed by the `AddressExt::do_send` method.
pub(crate) struct ReturningEnvelope<A, M: Message> {
    message: M,
    result_sender: Sender<M::Result>,
    #[cfg(feature = "timeout")]
    timed_out_sender: Sender<TimedOut<M>>,
    phantom: PhantomData<fn() -> A>,
    #[cfg(feature = "metrics")]
    queue_timer: prometheus::HistogramTimer,
}

impl<A: Actor, M: Message> ReturningEnvelope<A, M> {
    #[cfg(not(feature = "timeout"))]
    pub(crate) fn new(message: M) -> (Self, Receiver<M::Result>) {
        let (tx, rx) = catty::oneshot();
        let envelope = ReturningEnvelope {
            message,
            result_sender: tx,
            phantom: PhantomData,
            #[cfg(feature = "metrics")]
            queue_timer: QUEUEING_DURATION_HISTOGRAM
                .with(&std::collections::HashMap::from([
                    (ACTOR_LABEL, std::any::type_name::<A>()),
                    (MESSAGE_LABEL, std::any::type_name::<M>()),
                ]))
                .start_timer(),
        };

        (envelope, rx)
    }

    #[cfg(feature = "timeout")]
    pub(crate) fn new(message: M) -> (Self, Receiver<M::Result>, Receiver<TimedOut<M>>) {
        let (tx, rx) = catty::oneshot();
        let (tx_timed_out, rx_timed_out) = catty::oneshot();
        let envelope = ReturningEnvelope {
            message,
            result_sender: tx,
            timed_out_sender: tx_timed_out,
            phantom: PhantomData,
            #[cfg(feature = "metrics")]
            queue_timer: QUEUEING_DURATION_HISTOGRAM
                .with(&std::collections::HashMap::from([
                    (ACTOR_LABEL, std::any::type_name::<A>()),
                    (MESSAGE_LABEL, std::any::type_name::<M>()),
                ]))
                .start_timer(),
        };

        (envelope, rx, rx_timed_out)
    }
}

impl<A: Handler<M>, M: Message> MessageEnvelope for ReturningEnvelope<A, M> {
    type Actor = A;

    fn handle<'a>(
        self: Box<Self>,
        act: &'a mut Self::Actor,
        ctx: &'a mut Context<Self::Actor>,
    ) -> BoxFuture<'a, ()> {
        let Self {
            message,
            result_sender,
            #[cfg(feature = "metrics")]
            queue_timer,
            #[cfg(feature = "timeout")]
            timed_out_sender,
            ..
        } = *self;

        #[cfg(feature = "timeout")]
        let handler_timeout = ctx.handler_timeout();

        Box::pin(async move {
            #[cfg(feature = "metrics")]
            queue_timer.observe_duration();

            #[cfg(feature = "metrics")]
            let _processing_timer_will_automatically_observe_on_drop =
                PROCESSING_DURATION_HISTOGRAM
                    .with(&std::collections::HashMap::from([
                        (ACTOR_LABEL, std::any::type_name::<A>()),
                        (MESSAGE_LABEL, std::any::type_name::<M>()),
                    ]))
                    .start_timer();

            let handle_fut = act.handle(message, ctx).map(move |r| {
                // We don't actually care if the receiver is listening
                let _ = result_sender.send(r);
            });

            #[cfg(not(feature = "timeout"))]
            handle_fut.await;

            #[cfg(feature = "timeout")]
            match handler_timeout {
                None => handle_fut.await,
                Some(timeout) => {
                    let handler_with_timeout = futures_util::future::select(
                        futures_timer::Delay::new(timeout),
                        handle_fut,
                    );

                    if let Either::Left(((), _handler)) = handler_with_timeout.await {
                        // Inform the sender of the message that handling it has timed out
                        let _ = timed_out_sender.send(TimedOut {
                            phantom: PhantomData,
                        });

                        #[cfg(feature = "tracing")]
                        {
                            let msg_type = std::any::type_name::<M>();
                            let actor_type = std::any::type_name::<A>();
                            let timeout_seconds = timeout.as_secs();

                            tracing::warn!(%msg_type, %actor_type, %timeout_seconds, "Handler execution timeout");
                        }
                    };
                }
            }
        })
    }
}

/// An envelope that does not return a result from a message. Constructed  by the `AddressExt::do_send`
/// method.
pub(crate) struct NonReturningEnvelope<A, M: Message> {
    message: M,
    phantom: PhantomData<fn() -> A>,
    #[cfg(feature = "metrics")]
    queue_timer: prometheus::HistogramTimer,
}

impl<A: Actor, M: Message> NonReturningEnvelope<A, M> {
    pub(crate) fn new(message: M) -> Self {
        NonReturningEnvelope {
            message,
            phantom: PhantomData,
            #[cfg(feature = "metrics")]
            queue_timer: QUEUEING_DURATION_HISTOGRAM
                .with(&std::collections::HashMap::from([
                    (ACTOR_LABEL, std::any::type_name::<A>()),
                    (MESSAGE_LABEL, std::any::type_name::<M>()),
                ]))
                .start_timer(),
        }
    }
}

impl<A: Handler<M>, M: Message> MessageEnvelope for NonReturningEnvelope<A, M> {
    type Actor = A;

    fn handle<'a>(
        self: Box<Self>,
        act: &'a mut Self::Actor,
        ctx: &'a mut Context<Self::Actor>,
    ) -> BoxFuture<'a, ()> {
        #[cfg(feature = "timeout")]
        let handler_timeout = ctx.handler_timeout();

        Box::pin(async move {
            #[cfg(feature = "metrics")]
            self.queue_timer.observe_duration();

            #[cfg(feature = "metrics")]
            let _processing_timer_will_automatically_observe_on_drop =
                PROCESSING_DURATION_HISTOGRAM
                    .with(&std::collections::HashMap::from([
                        (ACTOR_LABEL, std::any::type_name::<A>()),
                        (MESSAGE_LABEL, std::any::type_name::<M>()),
                    ]))
                    .start_timer();

            let handle_fut = act.handle(self.message, ctx).map(|_| ());

            #[cfg(not(feature = "timeout"))]
            handle_fut.await;

            #[cfg(feature = "timeout")]
            match handler_timeout {
                None => handle_fut.await,
                Some(timeout) => {
                    let handler_with_timeout = futures_util::future::select(
                        futures_timer::Delay::new(timeout),
                        handle_fut,
                    );

                    if let Either::Left(((), _handler)) = handler_with_timeout.await {
                        #[cfg(feature = "tracing")]
                        {
                            let msg_type = std::any::type_name::<M>();
                            let actor_type = std::any::type_name::<A>();
                            let timeout_seconds = timeout.as_secs();

                            tracing::warn!(%msg_type, %actor_type, %timeout_seconds, "Handler execution timeout");
                        }
                    };
                }
            }
        })
    }
}

/// Like MessageEnvelope, but can be cloned.
pub(crate) trait BroadcastMessageEnvelope: MessageEnvelope + Sync {
    fn clone(&self) -> Box<dyn BroadcastMessageEnvelope<Actor = Self::Actor>>;
}

impl<A: Handler<M>, M: Message + Clone + Sync> BroadcastMessageEnvelope
    for NonReturningEnvelope<A, M>
{
    fn clone(&self) -> Box<dyn BroadcastMessageEnvelope<Actor = Self::Actor>> {
        Box::new(NonReturningEnvelope {
            message: self.message.clone(),
            phantom: PhantomData,
            #[cfg(feature = "metrics")]
            queue_timer: QUEUEING_DURATION_HISTOGRAM
                .with(&std::collections::HashMap::from([
                    (ACTOR_LABEL, std::any::type_name::<A>()),
                    (MESSAGE_LABEL, std::any::type_name::<M>()),
                ]))
                .start_timer(),
        })
    }
}

impl<A> Clone for Box<dyn BroadcastMessageEnvelope<Actor = A>> {
    fn clone(&self) -> Self {
        BroadcastMessageEnvelope::clone(&**self)
    }
}

#[cfg(feature = "timeout")]
pub(crate) struct TimedOut<M> {
    phantom: PhantomData<M>,
}

#[cfg(feature = "metrics")]
const ACTOR_LABEL: &str = "actor";

#[cfg(feature = "metrics")]
const MESSAGE_LABEL: &str = "message";

#[cfg(feature = "metrics")]
lazy_static::lazy_static! {
    static ref QUEUEING_DURATION_HISTOGRAM: prometheus::HistogramVec = prometheus::register_histogram_vec!(
        "xtra_message_queueing_duration_seconds",
        "The time of an xtra message from creation to being processed in seconds.",
        &[ACTOR_LABEL, MESSAGE_LABEL],
        vec![0.0000001, 0.000001, 0.000002, 0.000005, 0.00001, 0.0001, 0.001, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]
    )
    .unwrap();
}

#[cfg(feature = "metrics")]
lazy_static::lazy_static! {
    static ref PROCESSING_DURATION_HISTOGRAM: prometheus::HistogramVec = prometheus::register_histogram_vec!(
        "xtra_message_processing_duration_seconds",
        "The processing time of an xtra message in seconds.",
        &[ACTOR_LABEL, MESSAGE_LABEL],
        vec![0.0000001, 0.000001, 0.000002, 0.000005, 0.00001, 0.0001, 0.001, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]
    )
    .unwrap();
}
