//! Application-owned mail service integration. The configured endpoint accepts
//! {to, subject, text, message_id} and acknowledges accepted submission with 2xx.
//! Set HESTAN_MAIL_ENDPOINT, HESTAN_MAIL_TOKEN and HESTAN_MAIL_TO before running.
//! This is an example mail-service contract, not a built-in SMTP adapter.
use hestan::{
    Hestan, Job, NotificationDeliveryCtx, NotificationDeliveryError as Failure,
    NotificationDestination, Op,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("HESTAN_MAIL_ENDPOINT")?;
    let token = std::env::var("HESTAN_MAIL_TOKEN")?;
    // One recipient per destination avoids partial-recipient acceptance ambiguity.
    let recipient = std::env::var("HESTAN_MAIL_TO")?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let destination = NotificationDestination::builder("email")
        .on_run_finished()
        .sender(move |ctx: NotificationDeliveryCtx| {
            let (client, endpoint, token, recipient) = (client.clone(), endpoint.clone(), token.clone(), recipient.clone());
            async move {
                let event = &ctx.event;
                let response = client.post(endpoint).bearer_auth(token)
                    .header("Idempotency-Key", &event.delivery_id)
                    .json(&json!({"to":recipient,"subject":format!("{}: {}",event.display_name,event.run.status.as_str()),"text":format!("Job {} completed with status {}. Run: {}",event.run.job,event.run.status.as_str(),event.run.run_id),"message_id":format!("<{}@hestan.invalid>",event.delivery_id)}))
                    .send().await.map_err(|_|Failure::retryable("mail service transport failed"))?;
                match response.status().as_u16() {
                    200..=299 => Ok(()),
                    408 | 429 | 500..=599 => Err(Failure::retryable("mail service temporarily unavailable")),
                    _ => Err(Failure::permanent("mail service rejected submission")),
                }
            }
        }).build()?;
    Hestan::new()
        .notification(destination)
        .job(
            Job::builder("example")
                .op(Op::new("work", |_| async { Ok(json!(true)) }))
                .build()?,
        )
        .run_once("example", json!({}))
        .await?;
    Ok(())
}
