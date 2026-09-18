use hestan::Store;
use hestan::prelude::*;

#[tokio::test]
async fn startup_redacts_declared_schedule_and_preset_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("review.db");
    let job = Job::builder("job")
        .op(Op::new("op", |_| async { Ok(json!(null)) }).secret_params(["token"]))
        .build()
        .unwrap();
    let secret = "review-only-synthetic-credential";
    Hestan::new()
        .job(job)
        .db(path.to_str().unwrap())
        .schedule_with("job", "0 0 * * *", json!({"token": secret}))
        .preset("job", "saved", json!({"token": secret}))
        .run_once("job", json!({"token": secret}))
        .await
        .unwrap();
    let store = Store::open(path.to_str().unwrap()).unwrap();
    let schedule = store.schedules().unwrap().remove(0);
    let preset = store.preset("job", "saved").unwrap().unwrap();
    assert_eq!(
        (
            schedule.params["token"].as_str(),
            preset.params["token"].as_str()
        ),
        (
            Some(hestan::secret::REDACTED),
            Some(hestan::secret::REDACTED)
        )
    );
}
