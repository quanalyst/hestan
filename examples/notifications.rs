//! Named destinations work without an HTTP feature or external service.
use hestan::{Hestan, Job, NotificationDeliveryCtx, NotificationDestination, Op};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    let output = std::env::temp_dir().join("hestan-notification-example");
    std::fs::create_dir_all(&output)?;
    let destination = NotificationDestination::builder("local-receiver")
        .on_run_finished()
        .sender(move |ctx: NotificationDeliveryCtx| {
            let path = output.join(format!("{}.json", ctx.event.delivery_id));
            async move {
                // Await the receiver. The stable filename makes repeated deliveries replace
                // the same logical message; a real service should deduplicate the ID too.
                let body = serde_json::to_vec(ctx.event.as_ref()).expect("event is JSON");
                tokio::task::spawn_blocking(move || std::fs::write(path, body))
                    .await
                    .map_err(|_| {
                        hestan::NotificationDeliveryError::retryable("receiver task failed")
                    })?
                    .map_err(|_| {
                        hestan::NotificationDeliveryError::retryable("receiver write failed")
                    })
            }
        })
        .build()?;
    let job = Job::builder("example")
        .op(Op::new("work", |_| async {
            Ok(json!({"completed": true}))
        }))
        .build()?;
    Hestan::new()
        .job(job)
        .notification(destination)
        .db(std::env::temp_dir()
            .join("hestan-notification-example.db")
            .display()
            .to_string())
        .run_once("example", json!({}))
        .await?;
    Ok(())
}
