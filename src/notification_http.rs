use crate::{
    Error, NotificationDeliveryCtx, NotificationDeliveryError as Failure, NotificationSender,
};
use futures::future::BoxFuture;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

fn excerpt(text: &str, bytes: usize) -> String {
    let mut result = crate::notification::bounded(text, bytes);
    if result.len() < text.len() {
        result.push('…');
    }
    result
}
fn markdown(text: &str) -> String {
    let mut result = String::new();
    for ch in text.chars() {
        if "\\`*_{}[]()#+-.!<>".contains(ch) {
            result.push('\\');
        }
        result.push(ch);
    }
    result
}

fn tls_identity_error(error: &reqwest::Error) -> bool {
    let mut cause = std::error::Error::source(error);
    while let Some(error) = cause {
        if error
            .to_string()
            .to_ascii_lowercase()
            .contains("certificate")
        {
            return true;
        }
        cause = error.source();
    }
    false
}

#[derive(Clone, Copy)]
enum Format {
    Json,
    Slack,
    Teams,
}
#[derive(Clone)]
struct Http {
    url: Arc<str>,
    headers: HeaderMap,
    invalid: bool,
    format: Format,
}
impl Http {
    fn new(url: impl Into<String>, format: Format) -> Self {
        Self {
            url: Arc::from(url.into()),
            headers: HeaderMap::new(),
            invalid: false,
            format,
        }
    }
    fn header(mut self, name: &str, value: &str) -> Self {
        match (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            (Ok(name), Ok(mut value))
                if ![
                    "host",
                    "content-length",
                    "idempotency-key",
                    "x-hestan-attempt",
                    "content-type",
                ]
                .contains(&name.as_str()) =>
            {
                value.set_sensitive(true);
                self.headers.insert(name, value);
            }
            _ => self.invalid = true,
        }
        self
    }
    fn validate(&self) -> Result<(), Error> {
        let valid = reqwest::Url::parse(&self.url).is_ok_and(|u| {
            matches!(u.scheme(), "http" | "https")
                && u.host_str().is_some()
                && u.username().is_empty()
                && u.password().is_none()
                && u.fragment().is_none()
        });
        if !valid || self.invalid {
            Err(Error::Graph(
                "invalid notification HTTP endpoint or headers".into(),
            ))
        } else {
            Ok(())
        }
    }
    async fn send(self, ctx: NotificationDeliveryCtx) -> Result<(), Failure> {
        let event = &ctx.event;
        let title = format!(
            "{}: {}",
            excerpt(&event.display_name, 500),
            event.run.status.as_str()
        );
        let identity = format!("job {} · run {}", event.run.job, event.run.run_id);
        let error = event.run.error.as_deref().unwrap_or_default();
        let body: Value = match self.format {
            Format::Json => serde_json::to_value(event.as_ref())
                .map_err(|_| Failure::permanent("notification serialization failed"))?,
            Format::Slack => {
                let escape = |s: &str| {
                    s.replace('&', "&amp;")
                        .replace('<', "&lt;")
                        .replace('>', "&gt;")
                };
                // Identity and links are never truncated. Only human-readable excerpts shrink.
                let mut text = format!("{}\n{}", escape(&title), escape(&identity));
                if let Some(url) = &event.run_url {
                    text.push_str(&format!("\n<{}|View run>", escape(url)));
                }
                if text.chars().count() > 3000 {
                    return Err(Failure::permanent(
                        "Slack identity and link exceed message limit",
                    ));
                }
                if !error.is_empty() {
                    let available = 3000 - text.chars().count();
                    let mut excerpt_len = error.len().min(1000);
                    loop {
                        let addition = format!("\n{}", escape(&excerpt(error, excerpt_len)));
                        if addition.chars().count() <= available {
                            text.push_str(&addition);
                            break;
                        }
                        if excerpt_len == 0 {
                            break;
                        }
                        excerpt_len /= 2;
                    }
                }
                json!({"text":text,"unfurl_links":false,"unfurl_media":false})
            }
            Format::Teams => {
                let mut card = json!({"type":"AdaptiveCard","$schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.2","body":[{"type":"TextBlock","text":markdown(&title),"weight":"Bolder","wrap":true},{"type":"TextBlock","text":markdown(&format!("{identity}\n{}", excerpt(error,1000))),"wrap":true}]});
                if let Some(url) = &event.run_url {
                    card["actions"] =
                        json!([{"type":"Action.OpenUrl","title":"View run","url":url}]);
                }
                json!({"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","contentUrl":null,"content":card}]})
            }
        };
        let body = serde_json::to_vec(&body)
            .map_err(|_| Failure::permanent("notification serialization failed"))?;
        let max = if matches!(self.format, Format::Teams) {
            24 * 1024
        } else {
            64 * 1024
        };
        if body.len() > max {
            return Err(Failure::permanent(
                "notification payload exceeds size limit",
            ));
        }
        let response = super::client()
            .post(self.url.as_ref())
            .timeout(ctx.timeout)
            .headers(self.headers)
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", &event.delivery_id)
            .header("X-Hestan-Attempt", ctx.attempt)
            .body(body)
            .send()
            .await;
        let mut response = match response {
            Ok(r) => r,
            Err(e) => {
                return Err(if e.is_builder() {
                    Failure::permanent("HTTP request configuration failed")
                } else if tls_identity_error(&e) {
                    Failure::permanent("HTTP TLS identity validation failed")
                } else if e.is_timeout() {
                    Failure::retryable("HTTP timeout; remote outcome unknown")
                } else {
                    Failure::retryable("HTTP transport failed")
                });
            }
        };
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .and_then(retry_after);
        if (200..300).contains(&status) {
            if matches!(self.format, Format::Slack) {
                let mut bytes = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| Failure::retryable("Slack acknowledgement could not be read"))?
                {
                    if bytes.len() + chunk.len() > 4096 {
                        return Err(Failure::permanent("Slack acknowledgement exceeds limit"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if bytes.as_slice().trim_ascii() != b"ok" {
                    return Err(Failure::permanent(
                        "Slack returned an unexpected acknowledgement",
                    ));
                }
            }
            return Ok(());
        }
        let mut failure = if status == 408 || status == 429 || status >= 500 {
            Failure::retryable(format!("HTTP {status}"))
        } else {
            Failure::permanent(format!("HTTP {status}"))
        };
        failure.status = Some(status);
        if failure.retry {
            failure.after = retry_after;
            failure.cooldown = status == 429 || (status == 503 && retry_after.is_some());
        }
        Err(failure)
    }
}
fn retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    chrono::DateTime::parse_from_rfc2822(value)
        .ok()
        .and_then(|date| {
            (date.with_timezone(&chrono::Utc) - chrono::Utc::now())
                .to_std()
                .ok()
        })
}

macro_rules! adapter {
    ($name:ident,$format:ident,$protocol:literal,$spacing:expr,$doc:literal) => {
        #[doc=$doc]
        #[derive(Clone)]
        pub struct $name(Http);
        impl $name {
            /// Configure an application-owned endpoint. No request is sent here.
            pub fn new(url: impl Into<String>) -> Self {
                Self(Http::new(url, Format::$format))
            }
            /// Add a header. Values are never included in diagnostics.
            pub fn header(mut self, name: &str, value: &str) -> Self {
                self.0 = self.0.header(name, value);
                self
            }
        }
        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(concat!(stringify!($name), " { endpoint: [redacted] }"))
            }
        }
        impl NotificationSender for $name {
            fn send(
                &self,
                ctx: NotificationDeliveryCtx,
            ) -> BoxFuture<'static, Result<(), Failure>> {
                Box::pin(self.0.clone().send(ctx))
            }
            fn validate(&self) -> Result<(), Error> {
                self.0.validate()
            }
            fn protocol(&self) -> &str {
                $protocol
            }
            fn minimum_interval(&self) -> Duration {
                Duration::from_secs($spacing)
            }
        }
    };
}
adapter!(
    Webhook,
    Json,
    "hestan.webhook",
    0,
    "A durable generic JSON webhook sender."
);
adapter!(
    Slack,
    Slack,
    "slack.incoming",
    1,
    "A durable Slack incoming-webhook sender."
);
adapter!(
    TeamsWorkflow,
    Teams,
    "teams.workflow.adaptive",
    1,
    "An Adaptive Card sender for a configured Teams Workflows webhook. Acceptance does not prove downstream workflow completion."
);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_headers_accept_seconds_dates_and_reject_malformed_values() {
        assert_eq!(retry_after("60"), Some(Duration::from_secs(60)));
        assert!(retry_after("not a date").is_none());
        assert!(retry_after("-20").is_none());
        assert!(retry_after("Wed, 21 Oct 2015 07:28:00 GMT").is_none());
        let date = (chrono::Utc::now() + chrono::Duration::seconds(120))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let seconds = retry_after(&date).unwrap().as_secs();
        assert!((118..=120).contains(&seconds));
    }
    #[test]
    fn endpoints_and_headers_are_validated_without_exposing_credentials() {
        let sender = Webhook::new("https://example.test/secret-token")
            .header("Authorization", "Bearer private");
        assert!(sender.validate().is_ok());
        let debug = format!("{sender:?}");
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("private"));
        assert!(Webhook::new("file:///tmp/secret").validate().is_err());
        assert!(
            Webhook::new("https://user:password@example.test/")
                .validate()
                .is_err()
        );
        assert!(
            Webhook::new("https://example.test")
                .header("Idempotency-Key", "override")
                .validate()
                .is_err()
        );
        assert!(
            Webhook::new("https://example.test")
                .header("Authorization", "bad\r\nvalue")
                .validate()
                .is_err()
        );
    }
}
