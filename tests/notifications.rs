use hestan::{
    Job, NotificationDeliveryCtx as Ctx, NotificationDeliveryError as Failure,
    NotificationDeliveryState as State, NotificationDestination as Destination,
    NotificationPolicy as Policy, NotificationQuery, Op, RunStatus, Runner, Store, Trigger,
};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn job() -> Job {
    Job::builder("task")
        .display_name("Readable task")
        .op(Op::new("work", |_| async { Ok(json!(1)) }))
        .build()
        .unwrap()
}
fn rows(store: &Store) -> Vec<hestan::NotificationDelivery> {
    store
        .notification_deliveries(&NotificationQuery::default())
        .unwrap()
}
fn quick() -> Policy {
    Policy::default().backoff(Duration::from_millis(1), Duration::from_millis(2))
}

#[tokio::test]
async fn independent_destinations_retry_actual_failures_and_preserve_payload() {
    let store = Store::open(":memory:").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let intermittent = Destination::builder("intermittent")
        .on_run_finished()
        .policy(quick())
        .sender({
            let calls = calls.clone();
            let events = events.clone();
            move |ctx: Ctx| {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                events
                    .lock()
                    .unwrap()
                    .push(serde_json::to_value(ctx.event.as_ref()).unwrap());
                async move {
                    if n == 0 {
                        Err(Failure::retryable("temporary"))
                    } else {
                        Ok(())
                    }
                }
            }
        })
        .build()
        .unwrap();
    let good = Destination::builder("good")
        .on_run_finished()
        .sender({
            let accepted = accepted.clone();
            move |_: Ctx| {
                accepted.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }
        })
        .build()
        .unwrap();
    let runner = Runner::new([job()], store.clone())
        .unwrap()
        .with_notifications([intermittent, good])
        .unwrap();
    runner
        .run("task", json!({}), Trigger::Manual)
        .await
        .unwrap();
    for _ in 0..4 {
        runner
            .flush_notifications(Duration::from_secs(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert!(rows(&store).iter().all(|r| r.state == State::Delivered));
    let snapshots = events.lock().unwrap();
    assert_eq!(snapshots[0], snapshots[1]);
    assert_eq!(snapshots[0]["display_name"], "Readable task");
}

#[tokio::test]
async fn acknowledgement_is_awaited_and_run_success_is_independent() {
    let store = Store::open(":memory:").unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let d = Destination::builder("slow")
        .on_run_finished()
        .sender({
            let entered = entered.clone();
            let release = release.clone();
            move |_: Ctx| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                }
            }
        })
        .build()
        .unwrap();
    let runner = Runner::new([job()], store.clone())
        .unwrap()
        .with_notifications([d])
        .unwrap();
    let run = runner
        .run("task", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let task = tokio::spawn({
        let runner = runner.clone();
        async move {
            runner
                .flush_notifications(Duration::from_secs(3))
                .await
                .unwrap()
        }
    });
    entered.notified().await;
    assert_eq!(rows(&store)[0].state, State::InFlight);
    assert!(rows(&store)[0].delivered_at.is_none());
    release.notify_one();
    task.await.unwrap();
    assert_eq!(rows(&store)[0].state, State::Delivered);
}

#[tokio::test]
async fn restart_and_missing_registration_do_not_lose_owed_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let path = path.to_str().unwrap();
    let destination = || {
        Destination::builder("sink")
            .on_run_finished()
            .sender(|_: Ctx| async { Ok(()) })
            .build()
            .unwrap()
    };
    {
        let runner = Runner::new([job()], Store::open(path).unwrap())
            .unwrap()
            .with_notifications([destination()])
            .unwrap();
        runner
            .run("task", json!({}), Trigger::Manual)
            .await
            .unwrap();
    }
    let store = Store::open(path).unwrap();
    let missing = Runner::new([], store.clone()).unwrap();
    missing
        .flush_notifications(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(rows(&store)[0].state, State::Blocked);
    let restored = missing.with_notifications([destination()]).unwrap();
    restored
        .flush_notifications(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(rows(&store)[0].state, State::Delivered);
}

#[tokio::test]
async fn permanent_failure_and_operator_retry_keep_history_and_identity() {
    let store = Store::open(":memory:").unwrap();
    let accepted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d = Destination::builder("sink")
        .on_run_finished()
        .sender({
            let accepted = accepted.clone();
            move |_: Ctx| {
                let ok = accepted.load(Ordering::SeqCst);
                async move {
                    if ok {
                        Ok(())
                    } else {
                        Err(Failure::permanent("configuration rejected"))
                    }
                }
            }
        })
        .build()
        .unwrap();
    let runner = Runner::new([job()], store.clone())
        .unwrap()
        .with_notifications([d])
        .unwrap();
    runner
        .run("task", json!({}), Trigger::Manual)
        .await
        .unwrap();
    runner
        .flush_notifications(Duration::from_secs(1))
        .await
        .unwrap();
    let failed = rows(&store).remove(0);
    assert_eq!(failed.state, State::Failed);
    accepted.store(true, Ordering::SeqCst);
    store
        .control_notification(&failed.id, failed.generation, true, Some("operator"))
        .unwrap();
    assert!(
        store
            .control_notification(&failed.id, failed.generation, true, Some("operator"))
            .is_err()
    );
    runner
        .flush_notifications(Duration::from_secs(1))
        .await
        .unwrap();
    let sent = rows(&store).remove(0);
    assert_eq!(sent.id, failed.id);
    assert_eq!(sent.cycle, 2);
    assert_eq!(sent.attempts, 2);
    assert_eq!(sent.state, State::Delivered);
    assert!(
        store
            .control_notification(&sent.id, sent.generation, true, None)
            .is_err()
    );
    assert_eq!(store.notification_attempts(&sent.id).unwrap().len(), 2);
}

#[tokio::test]
async fn panic_and_timeout_are_bounded_failed_attempts() {
    for panic in [true, false] {
        let store = Store::open(":memory:").unwrap();
        let d = Destination::builder("sink")
            .on_run_finished()
            .policy(
                Policy::default()
                    .attempts(1)
                    .timeout(Duration::from_millis(10)),
            )
            .sender(move |_: Ctx| async move {
                assert!(!panic, "synthetic sender panic");
                std::future::pending::<()>().await;
                Ok(())
            })
            .build()
            .unwrap();
        let runner = Runner::new([job()], store.clone())
            .unwrap()
            .with_notifications([d])
            .unwrap();
        runner
            .run("task", json!({}), Trigger::Manual)
            .await
            .unwrap();
        runner
            .flush_notifications(Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(rows(&store)[0].state, State::Failed);
        assert_eq!(rows(&store)[0].attempts, 1);
    }
}

#[test]
fn registration_validation_is_additive_and_explicit() {
    assert!(
        Destination::builder("sink")
            .sender(|_: Ctx| async { Ok(()) })
            .build()
            .is_err()
    );
    assert!(
        Destination::builder("")
            .on_failure()
            .sender(|_: Ctx| async { Ok(()) })
            .build()
            .is_err()
    );
    let d = Destination::builder("sink")
        .on_failure()
        .sender(|_: Ctx| async { Ok(()) })
        .build()
        .unwrap();
    assert!(
        Runner::new([job()], Store::open(":memory:").unwrap())
            .unwrap()
            .with_notifications([d.clone(), d])
            .is_err()
    );
}

#[cfg(feature = "http")]
mod http {
    use super::*;
    use axum::{
        Router,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (format!("http://{addr}/hook"), task)
    }
    #[tokio::test]
    async fn webhook_observes_http_failures_and_stable_idempotency_headers() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new().route(
            "/hook",
            post({
                let calls = calls.clone();
                move |headers: HeaderMap, axum::Json(body): axum::Json<serde_json::Value>| {
                    let calls = calls.clone();
                    async move {
                        let mut seen = calls.lock().unwrap();
                        seen.push((headers, body));
                        if seen.len() == 1 {
                            StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            StatusCode::NO_CONTENT
                        }
                    }
                }
            }),
        );
        let (url, task) = serve(router).await;
        let store = Store::open(":memory:").unwrap();
        let d = Destination::builder("http")
            .on_run_finished()
            .policy(quick())
            .sender(hestan::notify::Webhook::new(url).header("Authorization", "secret-auth"))
            .build()
            .unwrap();
        let runner = Runner::new([job()], store.clone())
            .unwrap()
            .with_notifications([d])
            .unwrap();
        runner
            .run("task", json!({}), Trigger::Manual)
            .await
            .unwrap();
        for _ in 0..4 {
            runner
                .flush_notifications(Duration::from_secs(1))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let seen = calls.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0["idempotency-key"], seen[1].0["idempotency-key"]);
        assert_ne!(seen[0].0["x-hestan-attempt"], seen[1].0["x-hestan-attempt"]);
        assert_eq!(seen[0].1, seen[1].1);
        assert_eq!(rows(&store)[0].state, State::Delivered);
        assert!(
            !serde_json::to_string(&rows(&store))
                .unwrap()
                .contains("secret-auth")
        );
        task.abort();
    }
    #[tokio::test]
    async fn provider_adapters_use_expected_payloads() {
        for slack in [true, false] {
            let bodies = Arc::new(Mutex::new(Vec::new()));
            let router = Router::new().route(
                "/hook",
                post({
                    let bodies = bodies.clone();
                    move |axum::Json(body): axum::Json<serde_json::Value>| {
                        let bodies = bodies.clone();
                        async move {
                            bodies.lock().unwrap().push(body);
                            "ok"
                        }
                    }
                }),
            );
            let (url, task) = serve(router).await;
            let builder = Destination::builder("provider").on_run_finished();
            let d = if slack {
                builder.sender(hestan::notify::Slack::new(url))
            } else {
                builder.sender(hestan::notify::TeamsWorkflow::new(url))
            }
            .build()
            .unwrap();
            let store = Store::open(":memory:").unwrap();
            let decorated = Job::builder("task")
                .display_name(format!(
                    "Readable task <untrusted> [link](url) {}",
                    "🔥&".repeat(1000)
                ))
                .op(Op::new("work", |_| async { Ok(json!(true)) }))
                .build()
                .unwrap();
            let runner = Runner::new([decorated], store.clone())
                .unwrap()
                .with_notifications([d])
                .unwrap();
            runner
                .run("task", json!({}), Trigger::Manual)
                .await
                .unwrap();
            runner
                .flush_notifications(Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(rows(&store)[0].state, State::Delivered);
            let bodies = bodies.lock().unwrap();
            if slack {
                let text = bodies[0]["text"].as_str().unwrap();
                assert!(text.chars().count() <= 3000);
                assert!(text.contains("&lt;untrusted&gt;"));
                assert!(text.contains(&rows(&store)[0].event.run.run_id));
                assert!(!text.contains("<untrusted>"));
                assert!(
                    bodies[0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("Readable task")
                );
            } else {
                let card = &bodies[0]["attachments"][0]["content"];
                assert!(
                    card["body"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("\\[link\\]")
                );
                assert!(serde_json::to_vec(&bodies[0]).unwrap().len() <= 24 * 1024);
                assert_eq!(
                    bodies[0]["attachments"][0]["content"]["type"],
                    "AdaptiveCard"
                );
            }
            task.abort();
        }
    }
}

#[tokio::test]
async fn headless_flush_is_bounded_and_does_not_change_run_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("headless.db");
    let destination = Destination::builder("slow")
        .on_run_finished()
        .sender(|_: Ctx| async {
            std::future::pending::<()>().await;
            Ok(())
        })
        .build()
        .unwrap();
    let started = std::time::Instant::now();
    let run = hestan::Hestan::new()
        .db(path.display().to_string())
        .job(job())
        .notification(destination)
        .notification_flush_within(Duration::from_millis(20))
        .run_once("task", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert!(started.elapsed() < Duration::from_secs(2));
    let store = Store::open(path.to_str().unwrap()).unwrap();
    assert_eq!(rows(&store).len(), 1);
    assert_eq!(rows(&store)[0].state, State::InFlight);
}

#[tokio::test]
async fn http_status_policy_redirect_and_shared_cooldown() {
    #[cfg(feature = "http")]
    {
        use axum::{Router, http::StatusCode, routing::post};
        for status in [400, 401, 403, 404, 302, 408, 429, 500, 503] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let seen = calls.clone();
            let app = Router::new().route(
                "/",
                post(move || {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::from_u16(status).unwrap(),
                            [
                                ("retry-after", "60"),
                                ("location", "http://127.0.0.1:1/secret"),
                            ],
                            "private response text",
                        )
                    }
                }),
            );
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let store = Store::open(":memory:").unwrap();
            let d = Destination::builder("receiver")
                .on_run_finished()
                .sender(hestan::notify::Webhook::new(format!("http://{addr}/")))
                .build()
                .unwrap();
            let runner = Runner::new([job()], store.clone())
                .unwrap()
                .with_notifications([d])
                .unwrap();
            runner
                .run("task", json!({}), Trigger::Manual)
                .await
                .unwrap();
            runner
                .flush_notifications(Duration::from_secs(1))
                .await
                .unwrap();
            let row = rows(&store).remove(0);
            let retry = status == 408 || status == 429 || status >= 500;
            assert_eq!(
                row.state,
                if retry { State::Pending } else { State::Failed },
                "HTTP {status}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(!row.last_error.unwrap().contains("private response"));
            if status == 429 || status == 503 {
                runner
                    .run("task", json!({}), Trigger::Manual)
                    .await
                    .unwrap();
                runner
                    .flush_notifications(Duration::from_secs(1))
                    .await
                    .unwrap();
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    1,
                    "destination cooldown must apply to later events"
                );
            }
            task.abort();
        }
    }
}
