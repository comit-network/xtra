use std::time::Duration;
use tracing_subscriber::util::SubscriberInitExt;
use xtra::{prelude::*, Error};

#[tokio::test]
async fn handler_timeout_results_in_disconnected() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .set_default();

    let (address, context) = Context::new(None);
    let context = context.with_handler_timeout(Duration::from_secs(1));
    tokio::spawn(context.run(Sleeper {}));

    let result = address.send(Sleep(Duration::from_secs(2))).await;

    assert_eq!(
        result,
        Err(Error::TimedOut {
            message: "handler_timeout::Sleep".to_string()
        })
    );
}

#[tokio::test]
async fn no_handler_timeout_allows_handler_to_execute_normally() {
    let (address, context) = Context::new(None);
    tokio::spawn(context.run(Sleeper {}));

    let result = address.send(Sleep(Duration::from_secs(2))).await;

    assert!(result.is_ok());
}

struct Sleeper {}

#[async_trait::async_trait]
impl Actor for Sleeper {
    type Stop = ();

    async fn stopped(self) -> Self::Stop {}
}

struct Sleep(Duration);

impl Message for Sleep {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<Sleep> for Sleeper {
    async fn handle(&mut self, message: Sleep, _: &mut Context<Self>) {
        tokio::time::sleep(message.0).await;
    }
}
