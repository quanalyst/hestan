# Authentication

the api launches runs, cancels them, pauses schedules, starts backfills and
moves queue positions. on loopback that is a process talking to itself. on any
other address it is a button on the internet that runs arbitrary jobs, and
until this phase the only thing standing between those two situations was a
sentence in the documentation.

documentation is not a control. so:

**`serve` refuses to start on an address that is not loopback while no
authenticator is configured.**

```
$ orders serve --addr 0.0.0.0:4000
error: refusing to serve 0.0.0.0:4000: that address is reachable from outside
this machine and nothing here checks who is asking — this api launches runs,
cancels them and changes limits. bind a loopback address, give it
Hestan::auth(Auth::bearer(…)) or Hestan::auth(Auth::custom(…)), or say
Hestan::auth(Auth::None) if something in front of hestan already checks
identity
```

a refusal rather than a warning, because a warning is a line in a log that
scrolled past three deploys ago and the thing it was warning about is a
stranger's run on your warehouse.

## What did not change

**loopback, with nothing configured, serves exactly as it always has.** no
token, no header, no login, no configuration at all — one process on one
machine, which is what most deployments are and what every test that binds
`127.0.0.1` still is.

loopback means every spelling of it, because they are the same socket and do
not look alike:

| address | reachable from | serves unguarded |
| --- | --- | --- |
| `127.0.0.1`, and the rest of `127.0.0.0/8` | this machine | yes |
| `::1` | this machine | yes |
| `::ffff:127.0.0.1` — v4 loopback wearing a v6 address | this machine | yes |
| `0.0.0.0`, `[::]` | every interface this machine has | **no** |
| `192.168.1.10`, `10.0.0.4`, a public address | whoever can route to it | **no** |

the check is made on the address the **listener is holding**, not the one it
was handed. today those are the same; the check goes on the real one so that
nothing put between the ask and the bind can ever make the guarded address and
the served one two different things.

## The two authenticators

### `Auth::bearer` — one token

```rust
Hestan::new()
    .job(orders)
    .auth(Auth::bearer(std::env::var("HESTAN_TOKEN")?))
    .serve(([0, 0, 0, 0], 4000))
    .await
```

one shared secret, presented as `Authorization: Bearer <token>`, and it is an
**admin** token. take it from the environment or a secret file rather than
writing a literal: a token in argv is a token in `ps`, and a token in source is
a token in git.

hestan hashes the token when you hand it over and drops the plaintext. from
that line on the process holds a sha-256 digest and not the secret, and the
comparison against what a request presents is **constant time** — a
byte-by-byte `==` stops at the first byte that differs, which makes how long it
took to say no into how much of the token was right, and enough requests turn
that into the token itself.

one token is one identity, named `bearer`. everybody holding it is that
identity, which is why the audit trail says "somebody with the token" rather
than a person's name.

### `Auth::custom` — your own check

```rust
Hestan::new().auth(Auth::custom(|req| {
    // whatever the thing in front of this promises it has checked
    let user = req.header("x-forwarded-user")?;
    let role = match req.header("x-forwarded-groups").unwrap_or_default() {
        groups if groups.contains("ops") => Access::Admin,
        _ => Access::Viewer,
    };
    Some(Identity::new(user, role))
}))
```

a closure over each request's method, path and headers, answering with an
[`Identity`] or `None` — `None` being a 401. this is how a deployment that
already authenticates composes hestan into what it has rather than standing a
second scheme up beside it: a header its proxy sets, a signature it can check,
a session table it owns.

it runs on the request path, so it must not block. a lookup that costs a
network round trip belongs in the thing in front of hestan, where its answer is
already being taken. if it compares a secret of its own, compare it with
`hestan::auth::secret_eq` and not with `==`, for the reason above.

### `Auth::None` — the deliberate opt-out

```rust
Hestan::new().auth(Auth::None).serve(([0, 0, 0, 0], 4000))
```

nothing in hestan checks identity, and you are asserting that something in
front of it does — a proxy that authenticates, a mesh doing mtls, a network
nobody else is on. it turns the refusal off for every address, and says so once
at startup:

```
WARN serving 0.0.0.0:4000 with Auth::None: nothing in hestan checks who is
asking, and whatever is in front of it is what stops a stranger launching runs
here
```

it is spelled out rather than implied because the difference between "I have
thought about this" and "I did not know" is the whole of what the refusal is
for.

## The roles

three roles, and they contain each other: an operator may everything a viewer
may, an admin everything an operator may. every decision the server makes is
one comparison — `identity.role >= what this endpoint needs`.

| role | may |
| --- | --- |
| **viewer** | read: every `GET` |
| **operator** | that, plus launch, cancel, retry, resume, build, backfill |
| **admin** | that, plus pause, unpause, priority, presets — anything that changes how the deployment behaves rather than what it is doing now |

endpoint by endpoint, and this table is the security surface — the code is
derived from it, and `src/server.rs`'s suite asserts every row of it against
the real router:

| endpoint | needs |
| --- | --- |
| `GET /api/whoami` | nobody — see below |
| every other `GET /api/…` | viewer |
| `POST /api/jobs/{name}/runs` | operator |
| `POST /api/jobs/{name}/validate_params` | operator |
| `POST /api/runs/{id}/retry` | operator |
| `POST /api/runs/{id}/resume` | operator |
| `POST /api/runs/{id}/cancel` | operator |
| `POST /api/assets/build` | operator |
| `POST /api/assets/{name}/build` | operator |
| `POST /api/assets/{name}/backfill` | operator |
| `POST /api/backfills/{id}/cancel` | operator |
| `POST /api/runs/{id}/priority` | **admin** |
| `POST /api/schedules/state` | **admin** |
| `POST /api/sensors/state` | **admin** |
| `PUT`/`DELETE /api/jobs/{name}/presets/{preset}` | **admin** |
| the ui's own files (`/`, `/assets/*.js`, …) | nobody |

**what is not in the table is a mutation and needs an operator.** the rule is
default-deny by method: a `GET` is a read, anything else changes something. an
endpoint added tomorrow lands on the rule rather than in a hole, and a test
scrapes every route out of the router and fails if one of them is not asserted
here.

**401 for a credential that is absent or not recognized. 403 for an identity
that may not.** a 401 says nothing about what was wrong with what it refused —
"that one was close" is a sentence an attacker can use and a person cannot. a
403 says what it would have taken, which is the only useful half of a refusal:

```json
{ "error": "this needs operator, and vic is a viewer" }
```

**the ui's own files need no credentials**, or the page that asks for one could
not load. neither does `GET /api/whoami`, which answers "does this deployment
check who is asking, and who does it make you":

```json
{ "auth": true, "identity": { "name": "ada", "role": "admin" } }
```

with no credentials that is `{"auth": true, "identity": null}` — a 200, not a
401, because it is the endpoint asked *before* there is anything to present.
an open deployment answers `{"auth": false, "identity": null}`.

## The ui

the ui asks `/api/whoami` before anything else. an authenticated deployment
with nothing to present gets a prompt for a token rather than a page of failed
requests; everything else gets the ui it always had.

**a control a role may not use is not rendered.** a viewer's job page says
`launching needs an operator` where the launch controls are; the cancel,
re-run, resume, build, backfill, pause, preset and queue-order controls are
absent the same way. a button that is there and answers 403 teaches people that
the ui lies about what they can do, and the ones who learn that stop reading
the rest of it.

### Where the token lives, and what that does not protect against

**`sessionStorage`, scoped to the tab.** the browser drops it when the tab
closes, another origin cannot read it, and another tab of the same origin does
not share it.

that is the least-bad option available without building things hestan
deliberately does not have. it is not a safe one, and here is what it does not
protect against, plainly:

- **any script running on this page can read it.** a cross-site scripting hole
  anywhere in this ui, or in anything a browser extension injects into it,
  hands over a credential that can launch runs. an `HttpOnly` cookie is the
  thing javascript cannot read, and it needs a login endpoint, a session table
  and an expiry — a user store, which hestan does not have and is not going to
  grow.
- **it does not expire.** there is no session and no revocation: the only way
  to take a token back is to change it and restart the deployment.
- **it is visible in devtools**, in this tab's own request headers, and to
  anyone at the keyboard while the tab is open.
- **a bearer token is an admin token.** the ui cannot make it read-only, so
  "give the dashboard to the team" and "give the team the ability to launch
  anything" are the same act under `Auth::bearer`.

so: reasonable for an internal deployment on a network you trust, where the
alternative is no authentication at all. not a substitute for an identity
provider in front of hestan — `Auth::custom` is how you compose one in, and
then the browser holds that scheme's credential (a proxy's cookie, usually)
rather than this one.

the event stream is the other thing worth knowing: an `EventSource` cannot
carry a header, and the only other way to authenticate one is to put the token
in the url, where it lands in the browser's history and in every access log
between here and the deployment. so **a tab holding a token polls the event log
instead of streaming it.** a second of lag on the activity page costs less than
a credential in a log.

## The command line

```
$ hestan --server https://hestan.internal --token "$(cat /run/secrets/hestan)" runs
$ HESTAN_TOKEN=… hestan --server https://hestan.internal runs
```

`--token` for a terminal, `HESTAN_TOKEN` for a cron line — an argument is
visible in `ps` to every account on the machine for as long as the process
runs, and a variable is not. the flag wins where both are set. the environment
is read by hestan rather than by the argument parser, which would print the
**value** in `--help`.

a refusal is **exit 8**, a code of its own so a script can tell work that
failed from a credential that was not accepted:

```
$ hestan --server https://hestan.internal runs
error: authentication required: present your credentials — https://hestan.internal
is authenticated: pass --token, or set HESTAN_TOKEN, which keeps it out of ps
$ echo $?
8
```

`doctor` reports whether the deployment it is pointed at is authenticated at
all — in the deployment's own binary, from what it is configured with, and over
`--server` from `/api/whoami`, which needs no credentials:

```
$ hestan --server https://hestan.internal doctor
ok    auth       it checks who is asking, and does not know you
                 pass --token, or set HESTAN_TOKEN
-     not checked the store, the schedules, the sensors, the leases, the queue,
      the retention policy and the disk, which an http api exposes none of
```

that is one finding and a long list of what it could not see. a doctor that
answered "everything looks fine" having read one endpoint would be worse than
one that refused to run.

## Who did what

phase 24 built an event log for the whole system. it now says who.

the identity that caused a run, a cancel, a pause or a backfill goes on the
event, and on the run row:

```
$ hestan runs
RUN       JOB         STATUS   TRIGGER  STARTED   TOOK
019ff1b7  orders_etl  success  manual   2m ago    1.5s
```

```json
{
  "id": "019ff1b7-…",
  "trigger": "manual",
  "actor": "ada"
}
```

`Trigger::Manual` becoming "manual, by whom" is the useful half of an audit
trail, and the event log carries the same name on every event a person caused:
`run_queued`, `run_canceled`, `schedule_paused`, `sensor_paused`,
`backfill_started`, `backfill_canceled`.

three rules about it:

- **the credential is never recorded.** only the identity's name. the token
  reaches the `Authorization` header and the constant-time comparison, and
  nothing else — not a log line, not an event, not an error message, not a
  response body. `tests/auth.rs` drives an authenticated deployment through a
  launch, a retry, a cancel and a pause, then greps both of the server's
  streams, every response it sent, every event and run row, and every byte of
  the database file for the token.
- **an unauthenticated deployment records no actor**, rather than a fabricated
  one. an empty name is not "system": `manual` with no actor means a person
  asked and nothing was checking who.
- **a cancel of a run that is already executing is two events.** the run's own
  terminal event is written by whichever process is executing it, which may not
  be the one that took the request and does not know who asked — so the asking
  is a line of its own, and it is the line with the name on it.

## What this is not

deliberately, and none of these are coming later by accident:

- **there is no user store.** hestan has no users table, no registration, no
  password anywhere. `Auth::bearer` is one secret; `Auth::custom` asks
  something that already knows who people are.
- **there are no sessions.** no login endpoint, no cookie hestan sets, no
  expiry, no logout beyond the browser forgetting the token. nothing to
  invalidate means nothing to revoke: changing the token and restarting is the
  whole of it.
- **there is no oauth, oidc, saml or ldap.** a deployment that needs any of
  them already runs something that speaks them, and `Auth::custom` is how that
  thing's answer becomes hestan's.
- **there are no per-job or per-asset permissions.** the roles are about kinds
  of action, not about which pipeline. "ada may launch `orders_etl` but not
  `payments_reconcile`" is not expressible, and pretending otherwise with a
  half-built rule engine would be worse than saying so.
- **there is no rate limiting and no lockout.** a token that leaks can be
  guessed at as fast as your network allows; the comparison is constant-time,
  which closes the timing oracle and nothing else. put something in front of
  hestan if that matters.
- **there is no tls.** hestan serves plain http, exactly as it always has, so a
  bearer token on a network you do not control is a bearer token anyone on that
  network can read. terminate tls in front of it.
- **the roles are not audited against your idea of them.** `Auth::custom` can
  hand out `Access::Admin` to anybody; hestan checks the role it was given, not
  where it came from.

## Where each piece lives

| | |
| --- | --- |
| the refusal, the loopback rule, the constant-time comparison | `src/auth.rs` |
| the guard, the roles table, `whoami` | `src/server.rs` |
| who did what, in the store | `src/store.rs` (schema v18: `runs.actor`, `events.actor`) |
| the token, the prompt, and what it does not protect against | `ui/src/identity.ts` |
| the token leaving no trace | `tests/auth.rs` |
