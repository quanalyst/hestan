use super::*;
use crate::notification::{
    self, ClaimedDelivery, NotificationAttempt, NotificationDelivery, NotificationDeliveryError,
    NotificationDeliveryState as State, NotificationEnvelope, NotificationQuery, Registry,
};

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE run_notification_routes (run_id TEXT NOT NULL, destination TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(run_id,destination));
CREATE TABLE notification_deliveries (
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL, job TEXT NOT NULL, destination TEXT NOT NULL,
 state TEXT NOT NULL, due BIGINT, claim TEXT, expires BIGINT, settled BIGINT, created BIGINT NOT NULL, data TEXT NOT NULL,
 UNIQUE(run_id,destination));
CREATE INDEX delivery_due ON notification_deliveries(due,id) WHERE state='pending';
CREATE INDEX delivery_destination ON notification_deliveries(destination,state,due);
CREATE INDEX delivery_run ON notification_deliveries(run_id,id);
CREATE INDEX delivery_state ON notification_deliveries(state,id);
CREATE INDEX delivery_created ON notification_deliveries(created,id);
CREATE TABLE notification_attempts (delivery_id TEXT NOT NULL, attempt BIGINT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(delivery_id,attempt));
CREATE TABLE notification_destination_state (id TEXT PRIMARY KEY, next_at BIGINT NOT NULL DEFAULT 0, claim TEXT, expires BIGINT NOT NULL DEFAULT 0);
"#;

#[derive(serde::Serialize, serde::Deserialize)]
struct Route {
    destination: String,
    statuses: Vec<RunStatus>,
    policy: notification::NotificationPolicy,
    protocol: String,
    event_id: String,
    display_name: String,
    owner: Option<crate::Owner>,
    run_url: Option<String>,
}
fn decode<T: serde::de::DeserializeOwned>(r: &AnyRow<'_>, col: usize) -> Result<T, Error> {
    serde_json::from_str(&r.text(col)?)
        .map_err(|e| Error::Graph(format!("invalid notification record: {e}")))
}
// Project destination throttling into read responses without changing stored events.
fn delivery_view(r: &AnyRow<'_>) -> Result<NotificationDelivery, Error> {
    let mut row: NotificationDelivery = decode(r, 0)?;
    if row.state == State::Pending
        && let Some(due) = row.next_attempt_at
    {
        row.next_attempt_at = Some(due.max(instant(r.int(1)?)));
    }
    Ok(row)
}
fn encode(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("notification record is JSON")
}
fn state(s: State) -> &'static str {
    match s {
        State::Pending => "pending",
        State::InFlight => "in_flight",
        State::Blocked => "blocked",
        State::Delivered => "delivered",
        State::Failed => "failed",
        State::Dismissed => "dismissed",
    }
}
fn now(tx: &mut impl Exec) -> Result<i64, Error> {
    let sql = match tx.dialect() {
        Dialect::Sqlite => "SELECT CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)",
        #[cfg(feature = "postgres")]
        Dialect::Postgres => "SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)",
    };
    tx.query_opt(sql, args![], |r| r.int(0))?
        .ok_or_else(|| Error::Graph("database clock unavailable".into()))
}
fn instant(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).expect("database time is representable")
}
fn delay(at: i64, d: std::time::Duration) -> Option<i64> {
    i64::try_from(d.as_millis())
        .ok()
        .and_then(|d| at.checked_add(d))
        .filter(|v| DateTime::from_timestamp_millis(*v).is_some())
}
fn save(tx: &mut impl Exec, row: &NotificationDelivery) -> Result<(), Error> {
    tx.execute(
        "UPDATE notification_deliveries SET state=?2,due=?3,data=?4 WHERE id=?1",
        args![
            &row.id,
            state(row.state),
            row.next_attempt_at.map(|v| v.timestamp_millis()),
            encode(row)
        ],
    )?;
    Ok(())
}
fn audit(
    tx: &mut impl Exec,
    row: &NotificationDelivery,
    action: &str,
    actor: Option<&str>,
) -> Result<(), Error> {
    write_event(
        tx,
        &NewEvent::about(
            SubjectKind::System,
            row.id.clone(),
            EventKind::Unknown(format!("notification_{action}")),
            format!("notification {} {action}", row.id),
        )
        .actor(actor)
        .data(json!({"delivery_id":row.id,"destination":row.destination,"state":state(row.state)})),
        Utc::now(),
    )
}

pub(super) fn snapshot(tx: &mut impl Exec, run: &Run, registry: &Registry) -> Result<(), Error> {
    let event_id = uuid::Uuid::now_v7().to_string();
    for d in &registry.destinations {
        if !d.jobs.is_empty() && !d.jobs.contains(&run.job) {
            continue;
        }
        let (display_name, owner) = registry
            .jobs
            .get(&run.job)
            .cloned()
            .unwrap_or((run.job.clone(), None));
        let route = Route {
            destination: d.id.clone(),
            statuses: d.statuses.clone(),
            policy: d.policy.clone(),
            protocol: d.sender.protocol().into(),
            event_id: event_id.clone(),
            display_name,
            owner,
            run_url: d
                .base_url
                .as_ref()
                .map(|u| format!("{}/runs/{}", u.trim_end_matches('/'), run.id)),
        };
        tx.execute(
            "INSERT INTO run_notification_routes (run_id,destination,data) VALUES (?1,?2,?3)",
            args![&run.id, &d.id, encode(&route)],
        )?;
    }
    Ok(())
}

// Recovery may settle several runs at once. Ordinary completion uses its known ID.
pub(super) fn finish_terminal(tx: &mut impl Exec) -> Result<(), Error> {
    let runs: Vec<String> = tx.query(
        "SELECT DISTINCT r.run_id FROM run_notification_routes r \
         JOIN runs ON runs.id=r.run_id \
         WHERE runs.status IN ('success','failed','canceled')",
        args![],
        |r| r.text(0),
    )?;
    for id in runs {
        finish(tx, &id)?;
    }
    Ok(())
}

// Called within each terminal transaction. Routes are consumed exactly once.
pub(super) fn finish(tx: &mut impl Exec, id: &str) -> Result<(), Error> {
    let routes: Vec<Route> = tx.query(
        "SELECT data FROM run_notification_routes WHERE run_id=?1",
        args![id],
        |r| decode(r, 0),
    )?;
    if routes.is_empty() {
        return Ok(());
    }
    let run = tx
        .query_opt(
            &format!("SELECT {RUN_COLS} FROM runs WHERE id=?1"),
            args![id],
            run_from_row,
        )?
        .ok_or_else(|| Error::UnknownRun(id.into()))?;
    if matches!(run.status, RunStatus::Queued | RunStatus::Running) {
        return Ok(());
    }
    let failed_op: Option<String> = tx.query_opt(
        "SELECT op FROM op_runs WHERE run_id=?1 AND status='failed' ORDER BY op LIMIT 1",
        args![id],
        |r| r.text(0),
    )?;
    for route in routes {
        if !route.statuses.contains(&run.status) {
            continue;
        }
        let delivery_id = uuid::Uuid::now_v7().to_string();
        let at = run.finished_at.unwrap_or_else(Utc::now);
        let due = instant(now(tx)?);
        let error_truncated = run.error.as_ref().is_some_and(|s| s.len() > 2048);
        let event = NotificationEnvelope {
            event_id: route.event_id,
            delivery_id: delivery_id.clone(),
            event_type: "run.finished".into(),
            occurred_at: at,
            display_name: route.display_name,
            run_url: route.run_url,
            error_truncated,
            run: crate::RunEvent {
                run_id: run.id.clone(),
                job: run.job.clone(),
                owner: route.owner,
                trigger: run.trigger,
                status: run.status,
                failed_op: failed_op.clone(),
                error: run.error.as_ref().map(|s| notification::bounded(s, 2048)),
                started_at: run.started_at,
                finished_at: at,
                duration: run.started_at.and_then(|start| (at - start).to_std().ok()),
            },
        };
        tx.execute("INSERT INTO notification_destination_state (id) VALUES (?1) ON CONFLICT (id) DO NOTHING",args![&route.destination])?;
        let row = NotificationDelivery {
            id: delivery_id,
            destination: route.destination,
            event,
            state: State::Pending,
            attempts: 0,
            cycle_attempts: 0,
            cycle: 1,
            generation: 0,
            created_at: at,
            next_attempt_at: Some(due),
            delivered_at: None,
            last_error: None,
            protocol: route.protocol,
            policy: route.policy,
        };
        tx.execute("INSERT INTO notification_deliveries (id,run_id,job,destination,state,due,data,created) VALUES (?1,?2,?3,?4,'pending',?5,?6,?7) ON CONFLICT (run_id,destination) DO NOTHING",args![&row.id,&run.id,&run.job,&row.destination,due.timestamp_millis(),encode(&row),at.timestamp_millis()])?;
    }
    tx.execute(
        "DELETE FROM run_notification_routes WHERE run_id=?1",
        args![id],
    )?;
    Ok(())
}

impl Store {
    /// Aggregate named delivery backlog, independent of run execution health.
    pub fn notification_health(&self) -> Result<Value, Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        let at = now(&mut tx)?;
        let counts: Vec<(String, i64)> = tx.query(
            "SELECT state,COUNT(*) FROM notification_deliveries GROUP BY state",
            args![],
            |r| Ok((r.text(0)?, r.int(1)?)),
        )?;
        let mut states = serde_json::Map::new();
        for name in [
            "pending",
            "in_flight",
            "blocked",
            "failed",
            "delivered",
            "dismissed",
        ] {
            states.insert(name.into(), json!(0));
        }
        for (name, count) in counts {
            states.insert(name, json!(count));
        }
        let oldest:Option<NotificationDelivery>=tx.query_opt("SELECT data FROM notification_deliveries WHERE state IN ('pending','blocked','in_flight') ORDER BY id LIMIT 1",args![],|r|decode(r,0))?;
        let expired=tx.query_opt("SELECT COUNT(*) FROM notification_deliveries WHERE state='in_flight' AND expires<=?1",args![at],|r|r.int(0))?.unwrap_or(0);
        Ok(
            json!({"states":states,"expired_claims":expired,"oldest_pending_age_seconds":oldest.map(|r|(instant(at)-r.created_at).num_seconds().max(0)).unwrap_or(0)}),
        )
    }
    pub(crate) fn prune_named_notifications(
        &self,
        older_than: DateTime<Utc>,
    ) -> Result<usize, Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        tx.execute("DELETE FROM notification_attempts WHERE delivery_id IN (SELECT id FROM notification_deliveries WHERE settled<?1 AND state IN ('delivered','dismissed'))",args![older_than.timestamp_millis()])?;
        let count=tx.execute("DELETE FROM notification_deliveries WHERE settled<?1 AND state IN ('delivered','dismissed')",args![older_than.timestamp_millis()])?;
        tx.execute("DELETE FROM notification_destination_state WHERE claim IS NULL AND NOT EXISTS (SELECT 1 FROM notification_deliveries WHERE destination=notification_destination_state.id) AND NOT EXISTS (SELECT 1 FROM run_notification_routes WHERE destination=notification_destination_state.id)",args![])?;
        tx.commit()?;
        Ok(count)
    }
    pub(crate) fn set_notification_registry(&self, registry: Registry) {
        *self.notifications.lock().unwrap() = registry;
    }
    pub(crate) fn notification_registry(&self) -> Registry {
        self.notifications.lock().unwrap().clone()
    }

    /// Named deliveries, newest first, with a stable keyset cursor.
    pub fn notification_deliveries(
        &self,
        q: &NotificationQuery,
    ) -> Result<Vec<NotificationDelivery>, Error> {
        if q.state.as_deref().is_some_and(|s| {
            ![
                "pending",
                "in_flight",
                "blocked",
                "delivered",
                "failed",
                "dismissed",
            ]
            .contains(&s)
        }) {
            return Err(Error::Graph("unknown notification delivery state".into()));
        }
        self.conn().query("SELECT data, COALESCE((SELECT next_at FROM notification_destination_state WHERE id=notification_deliveries.destination),0) FROM notification_deliveries WHERE (CAST(?1 AS TEXT) IS NULL OR run_id=?1) AND (CAST(?2 AS TEXT) IS NULL OR job=?2) AND (CAST(?3 AS TEXT) IS NULL OR destination=?3) AND (CAST(?4 AS TEXT) IS NULL OR state=?4) AND (CAST(?5 AS TEXT) IS NULL OR id<?5) AND (CAST(?7 AS BIGINT) IS NULL OR created>=?7) AND (CAST(?8 AS BIGINT) IS NULL OR created<?8) ORDER BY id DESC LIMIT ?6",args![q.run.as_deref(),q.job.as_deref(),q.destination.as_deref(),q.state.as_deref(),q.before.as_deref(),i64::from(q.limit.unwrap_or(50).clamp(1,500)),q.since.map(|t|t.timestamp_millis()),q.until.map(|t|t.timestamp_millis())],delivery_view)
    }
    /// One named delivery, including its immutable event.
    pub fn notification_delivery(&self, id: &str) -> Result<Option<NotificationDelivery>, Error> {
        self.conn().query_opt(
            "SELECT data, COALESCE((SELECT next_at FROM notification_destination_state WHERE id=notification_deliveries.destination),0) FROM notification_deliveries WHERE id=?1",
            args![id],
            delivery_view,
        )
    }
    /// Recorded attempts in chronological order.
    pub fn notification_attempts(&self, id: &str) -> Result<Vec<NotificationAttempt>, Error> {
        self.conn().query(
            "SELECT data FROM notification_attempts WHERE delivery_id=?1 ORDER BY attempt",
            args![id],
            |r| decode(r, 0),
        )
    }
    pub(crate) fn outstanding_deliveries(&self) -> Result<usize, Error> {
        Ok(self.conn().query_opt("SELECT COUNT(*) FROM notification_deliveries WHERE state IN ('pending','in_flight','blocked','failed')",args![],|r|r.int(0))?.unwrap_or(0) as usize)
    }

    pub(crate) fn reconcile_deliveries(&self, registry: &Registry) -> Result<(), Error> {
        let mut cursor = String::new();
        loop {
            let rows:Vec<NotificationDelivery>=self.conn().query("SELECT data FROM notification_deliveries WHERE state IN ('pending','blocked','in_flight') AND id>?1 ORDER BY id LIMIT 500",args![&cursor],|r|decode(r,0))?;
            let Some(last) = rows.last() else {
                break;
            };
            cursor = last.id.clone();
            for old in rows {
                let compatible = registry.destinations.iter().any(|d| {
                    d.id == old.destination && d.enabled && d.sender.protocol() == old.protocol
                });
                if (old.state == State::Pending && compatible)
                    || (old.state == State::Blocked && !compatible)
                {
                    continue;
                }
                let mut recovered = None;

                let mut conn = self.conn();
                let mut tx = conn.begin_immediate()?;
                let at = now(&mut tx)?;
                tx.execute("INSERT INTO notification_destination_state (id) VALUES (?1) ON CONFLICT (id) DO NOTHING",args![&old.destination])?;
                tx.execute(
                    "UPDATE notification_destination_state SET id=id WHERE id=?1",
                    args![&old.destination],
                )?;
                let found: Option<(NotificationDelivery, Option<String>, Option<i64>)> = tx
                    .query_opt(
                        &format!(
                            "SELECT data,claim,expires FROM notification_deliveries WHERE id=?1 {}",
                            tx.dialect().claim_lock()
                        ),
                        args![&old.id],
                        |r| Ok((decode(r, 0)?, r.opt_text(1)?, r.opt_int(2)?)),
                    )?;
                let Some((mut row, token, expires)) = found else {
                    continue;
                };
                let before = row.generation;
                if row.state == State::InFlight {
                    if expires.is_some_and(|e| e > at) {
                        continue;
                    }
                    let mut attempt:NotificationAttempt=tx.query_opt("SELECT data FROM notification_attempts WHERE delivery_id=?1 AND attempt=?2",args![&row.id,i64::from(row.attempts)],|r|decode(r,0))?.expect("claimed delivery has attempt");
                    recovered = Some(instant(at) - attempt.started_at);
                    attempt.finished_at = Some(instant(at));
                    attempt.outcome = "outcome_unknown".into();
                    attempt.error = Some("delivery lease expired; remote outcome unknown".into());
                    tx.execute(
                    "UPDATE notification_attempts SET data=?3 WHERE delivery_id=?1 AND attempt=?2",
                    args![&row.id, i64::from(row.attempts), encode(&attempt)],
                )?;
                    row.state = if row.cycle_attempts >= row.policy.attempts {
                        State::Failed
                    } else {
                        State::Pending
                    };
                    row.last_error = attempt.error;
                    row.next_attempt_at = (row.state == State::Pending).then(|| instant(at));
                    row.generation += 1;
                    tx.execute(
                        "UPDATE notification_deliveries SET claim=NULL,expires=NULL WHERE id=?1",
                        args![&row.id],
                    )?;
                    tx.execute("UPDATE notification_destination_state SET claim=NULL,expires=0 WHERE id=?1 AND claim=?2",args![&row.destination,token.as_deref()])?;
                }
                if matches!(row.state, State::Pending | State::Blocked) {
                    let compatible = registry.destinations.iter().any(|d| {
                        d.id == row.destination && d.enabled && d.sender.protocol() == row.protocol
                    });
                    if !compatible && row.state != State::Blocked {
                        row.state = State::Blocked;
                        row.last_error =
                            Some("destination missing, disabled, or incompatible".into());
                        row.generation += 1;
                    } else if compatible
                        && row.state == State::Blocked
                        && row.last_error.as_deref()
                            == Some("destination missing, disabled, or incompatible")
                    {
                        row.state = State::Pending;
                        row.next_attempt_at = Some(instant(at));
                        row.last_error = None;
                        row.generation += 1;
                    }
                }
                if row.generation != before {
                    save(&mut tx, &row)?;
                    audit(&mut tx, &row, state(row.state), None)?;
                }
                tx.commit()?;
                if let Some(span) = recovered {
                    self.meters.notification_attempt(3, span);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn claim_delivery(
        &self,
        registry: &Registry,
        only_run: Option<&str>,
    ) -> Result<Option<ClaimedDelivery>, Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let at = now(&mut tx)?;
        let destinations:Vec<String>=tx.query("SELECT destination FROM notification_deliveries WHERE state='pending' AND due<=?1 AND (CAST(?2 AS TEXT) IS NULL OR run_id=?2) GROUP BY destination ORDER BY MIN(due),MIN(id)",args![at,only_run],|r|r.text(0))?;
        for destination in destinations {
            let Some(d) = registry
                .destinations
                .iter()
                .find(|d| d.id == destination && d.enabled)
            else {
                continue;
            };
            let token = uuid::Uuid::now_v7().to_string();
            if tx.execute("UPDATE notification_destination_state SET claim=?2,expires=?3 WHERE id=?1 AND expires<=?4 AND next_at<=?4",args![&destination,&token,at+30000,at])?==0 {continue;}
            let selected:Option<NotificationDelivery>=tx.query_opt(&format!("SELECT data FROM notification_deliveries WHERE destination=?1 AND state='pending' AND due<=?2 AND (CAST(?3 AS TEXT) IS NULL OR run_id=?3) ORDER BY due,id LIMIT 1 {}",tx.dialect().claim_lock()),args![&destination,at,only_run],|r|decode(r,0))?;
            let Some(mut row) = selected else {
                tx.execute("UPDATE notification_destination_state SET claim=NULL,expires=0 WHERE id=?1 AND claim=?2",args![&destination,&token])?;
                continue;
            };
            if row.protocol != d.sender.protocol() {
                tx.execute("UPDATE notification_destination_state SET claim=NULL,expires=0 WHERE id=?1 AND claim=?2",args![&destination,&token])?;
                continue;
            }
            let expires = delay(
                at,
                row.policy.timeout.max(std::time::Duration::from_secs(10))
                    + std::time::Duration::from_secs(20),
            )
            .expect("validated timeout");
            row.state = State::InFlight;
            row.attempts += 1;
            row.cycle_attempts += 1;
            row.generation += 1;
            row.next_attempt_at = None;
            tx.execute("UPDATE notification_destination_state SET expires=?3,next_at=?4 WHERE id=?1 AND claim=?2",args![&destination,&token,expires,delay(at,row.policy.spacing).expect("validated spacing")])?;
            tx.execute(
                "UPDATE notification_deliveries SET claim=?2,expires=?3 WHERE id=?1",
                args![&row.id, &token, expires],
            )?;
            save(&mut tx, &row)?;
            let attempt = NotificationAttempt {
                delivery_id: row.id.clone(),
                attempt: row.attempts,
                cycle: row.cycle,
                started_at: instant(at),
                finished_at: None,
                outcome: "in_flight".into(),
                status: None,
                error: None,
            };
            tx.execute(
                "INSERT INTO notification_attempts (delivery_id,attempt,data) VALUES (?1,?2,?3)",
                args![&row.id, i64::from(row.attempts), encode(&attempt)],
            )?;
            tx.commit()?;
            return Ok(Some(ClaimedDelivery { row, token }));
        }
        // Roll back speculative destination reservations if nothing was claimed.
        Ok(None)
    }

    pub(crate) fn settle_delivery(
        &self,
        claim: &ClaimedDelivery,
        error: Option<&NotificationDeliveryError>,
    ) -> Result<(), Error> {
        #[cfg(test)]
        if let Some(error) = self.injected("notification acknowledgement") {
            return Err(error);
        }
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let at = now(&mut tx)?;
        tx.execute(
            "UPDATE notification_destination_state SET id=id WHERE id=?1",
            args![&claim.row.destination],
        )?;
        if tx.execute("UPDATE notification_deliveries SET id=id WHERE id=?1 AND claim=?2 AND expires>?3 AND state='in_flight'",args![&claim.row.id,&claim.token,at])?==0 {return Err(Error::ClaimLost(claim.row.id.clone()));}
        let mut row = claim.row.clone();
        row.generation += 1;
        let mut attempt: NotificationAttempt = tx
            .query_opt(
                "SELECT data FROM notification_attempts WHERE delivery_id=?1 AND attempt=?2",
                args![&row.id, i64::from(row.attempts)],
                |r| decode(r, 0),
            )?
            .expect("claimed attempt exists");
        attempt.finished_at = Some(instant(at));
        if let Some(e) = error {
            let message = notification::bounded(&self.secrets.scrub(&e.message), 2048);
            row.last_error = Some(message.clone());
            attempt.error = Some(message);
            attempt.status = e.status;
            let backoff = crate::backoff::jittered_exponential(
                row.policy.base,
                row.cycle_attempts - 1,
                row.policy.cap,
            );
            let wait = e.after.unwrap_or_default().max(backoff);
            let due = delay(at, wait);
            if e.cooldown
                && let Some(due) = due
            {
                tx.execute("UPDATE notification_destination_state SET next_at=CASE WHEN next_at>?2 THEN next_at ELSE ?2 END WHERE id=?1",args![&row.destination,due])?;
            }
            row.state = if e.retry && due.is_none() {
                State::Failed
            } else if e.retry && row.cycle_attempts < row.policy.attempts {
                State::Pending
            } else {
                State::Failed
            };
            if due.is_none() && e.retry {
                row.last_error =
                    Some("retry delay is outside the supported timestamp range".into());
            }
            row.next_attempt_at = if row.state == State::Pending {
                due.map(instant)
            } else {
                None
            };
            attempt.outcome = if e.retry {
                "retryable_failure"
            } else {
                "permanent_failure"
            }
            .into();
        } else {
            row.state = State::Delivered;
            row.delivered_at = Some(instant(at));
            row.last_error = None;
            attempt.outcome = "accepted".into();
        }
        save(&mut tx, &row)?;
        if row.state == State::Delivered {
            tx.execute(
                "UPDATE notification_deliveries SET settled=?2 WHERE id=?1",
                args![&row.id, at],
            )?;
        }
        tx.execute(
            "UPDATE notification_deliveries SET claim=NULL,expires=NULL WHERE id=?1",
            args![&row.id],
        )?;
        tx.execute("UPDATE notification_destination_state SET claim=NULL,expires=0 WHERE id=?1 AND claim=?2",args![&row.destination,&claim.token])?;
        tx.execute(
            "UPDATE notification_attempts SET data=?3 WHERE delivery_id=?1 AND attempt=?2",
            args![&row.id, i64::from(row.attempts), encode(&attempt)],
        )?;
        if matches!(row.state, State::Delivered | State::Failed | State::Blocked) {
            audit(&mut tx, &row, state(row.state), None)?;
        }
        tx.commit()?;
        self.meters.notification_attempt(
            match attempt.outcome.as_str() {
                "accepted" => 0,
                "retryable_failure" => 1,
                _ => 2,
            },
            instant(at) - attempt.started_at,
        );
        Ok(())
    }

    /// Retry an exhausted delivery or dismiss owed work, guarding against stale UI actions.
    pub fn control_notification(
        &self,
        id: &str,
        generation: i64,
        retry: bool,
        actor: Option<&str>,
    ) -> Result<NotificationDelivery, Error> {
        let registry = self.notification_registry();
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let at = now(&mut tx)?;
        let mut row: NotificationDelivery = tx
            .query_opt(
                &format!(
                    "SELECT data FROM notification_deliveries WHERE id=?1 {}",
                    tx.dialect().claim_lock()
                ),
                args![id],
                |r| decode(r, 0),
            )?
            .ok_or_else(|| Error::Graph("unknown notification delivery".into()))?;
        if row.generation != generation {
            return Err(Error::Conflict(
                "notification changed; refresh before acting".into(),
            ));
        }
        if retry {
            if row.state != State::Failed
                || !registry.destinations.iter().any(|d| {
                    d.id == row.destination && d.enabled && d.sender.protocol() == row.protocol
                })
            {
                return Err(Error::Conflict(
                    "retry requires a failed delivery and compatible destination".into(),
                ));
            }
            row.state = State::Pending;
            row.cycle += 1;
            row.cycle_attempts = 0;
            row.next_attempt_at = Some(instant(at));
        } else {
            if !matches!(row.state, State::Failed | State::Pending | State::Blocked) {
                return Err(Error::Conflict(
                    "only inactive, undelivered notifications can be dismissed".into(),
                ));
            }
            row.state = State::Dismissed;
            row.next_attempt_at = None;
        }
        row.generation += 1;
        save(&mut tx, &row)?;
        if row.state == State::Dismissed {
            tx.execute(
                "UPDATE notification_deliveries SET settled=?2 WHERE id=?1",
                args![&row.id, at],
            )?;
        }
        audit(
            &mut tx,
            &row,
            if retry {
                "retry_requested"
            } else {
                "dismissed"
            },
            actor,
        )?;
        tx.commit()?;
        Ok(row)
    }
}
