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
this machine and nothing here checks who is asking. this api launches runs,
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
token, no header, no login, no configuration at all: one process on one
machine, which is what most deployments are and what every test that binds
`127.0.0.1` still is.

loopback means every spelling of it, because they are the same socket and do
not look alike:

| address | reachable from | serves unguarded |
| --- | --- | --- |
| `127.0.0.1`, and the rest of `127.0.0.0/8` | this machine | yes |
| `::1` | this machine | yes |
| `::ffff:127.0.0.1`, v4 loopback wearing a v6 address | this machine | yes |
| `0.0.0.0`, `[::]` | every interface this machine has | **no** |
| `192.168.1.10`, `10.0.0.4`, a public address | whoever can route to it | **no** |

the check is made on the address the **listener is holding**, not the one it
was handed. today those are the same; the check goes on the real one so that
nothing put between the ask and the bind can ever make the guarded address and
the served one two different things.

## The two authenticators

### `Auth::bearer`: one token

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
comparison against what a request presents is **constant time**: a
byte-by-byte `==` stops at the first byte that differs, which makes how long it
took to say no into how much of the token was right, and enough requests turn
that into the token itself.

one token is one identity, named `bearer`. everybody holding it is that
identity, which is why the audit trail says "somebody with the token" rather
than a person's name.

### `Auth::custom`: your own check

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
[`Identity`] or `None`. `None` is a 401. this is how a deployment that
already authenticates composes hestan into what it has rather than standing a
second scheme up beside it: a header its proxy sets, a signature it can check,
a session table it owns.

it runs on the request path, so it must not block. a lookup that costs a
network round trip belongs in the thing in front of hestan, where its answer is
already being taken. if it compares a secret of its own, compare it with
`hestan::auth::secret_eq` and not with `==`, for the reason above.

### `Auth::None`: the deliberate opt-out

```rust
Hestan::new().auth(Auth::None).serve(([0, 0, 0, 0], 4000))
```

nothing in hestan checks identity, and you are asserting that something in
front of it does: a proxy that authenticates, a mesh doing mtls, a network
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
may, an admin everything an operator may. how much of the api a request needs
is one comparison: `identity.role >= what this endpoint needs`. *what* it may
be done to is [the other question](#the-scopes), and the roles say nothing
about it.

| role | may |
| --- | --- |
| **viewer** | read: every `GET` |
| **operator** | that, plus launch, cancel, retry, resume, replay, build, backfill |
| **admin** | that, plus pause, unpause, priority, presets (anything that changes how the deployment behaves rather than what it is doing now) |

endpoint by endpoint, and this table is the security surface. the code is
derived from it, and `src/server.rs`'s suite asserts every row of it against
the real router:

| endpoint | needs |
| --- | --- |
| `GET /api/whoami` | nobody; see below |
| every other `GET /api/…` | viewer |
| `POST /api/jobs/{name}/runs` | operator |
| `POST /api/jobs/{name}/validate_params` | operator |
| `POST /api/runs/{id}/retry` | operator |
| `POST /api/runs/{id}/resume` | operator |
| `POST /api/runs/{id}/replay` | operator |
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
that may not.** a 401 says nothing about what was wrong with what it refused:
"that one was close" is a sentence an attacker can use and a person cannot. a
403 says what it would have taken, which is the only useful half of a refusal:

```json
{ "error": "this needs operator, and vic is a viewer" }
```

**the ui's own files need no credentials**, or the page that asks for one could
not load. neither does `GET /api/whoami`, which answers "does this deployment
check who is asking, and who does it make you":

```json
{
  "auth": true,
  "identity": {
    "name": "ada",
    "role": "admin",
    "scope": { "everything": true, "jobs": [], "assets": [] }
  }
}
```

with no credentials that is `{"auth": true, "identity": null}`, a 200, not a
401, because it is the endpoint asked *before* there is anything to present.
an open deployment answers `{"auth": false, "identity": null}`.

## The scopes

the ladder is the right shape and it is too coarse for one case: **a token that
may launch one job and nothing else.** a ci pipeline triggering a deploy needs
an operator's `POST /api/jobs/deploy/runs`, and an operator may also cancel
production runs, retry anything and start a backfill. those are the same role.

a **scope** narrows what a role may touch without changing what the role is:

```rust
Hestan::new()
    .job(deploy)
    .job(billing)
    .auth(Auth::custom(|req| match req.bearer()? {
        t if secret_eq(t, &ci_token) => {
            Some(Identity::operator("ci").scoped_to(Scope::jobs(["deploy"])))
        }
        t if secret_eq(t, &ops_token) => Some(Identity::admin("ops")),
        _ => None,
    }))
    .serve(([0, 0, 0, 0], 4000))
    .await
```

`ci` is an operator on `deploy` and a stranger everywhere else:

```
$ curl -XPOST -H "authorization: Bearer $CI" .../api/jobs/deploy/runs
{"run_id":"019..."}

$ curl -XPOST -H "authorization: Bearer $CI" .../api/jobs/billing/runs
{"error":"ci is scoped to job deploy, and this changes job billing"}

$ curl -XPOST -H "authorization: Bearer $CI" .../api/runs/019.../cancel
{"error":"ci is scoped to job deploy, and this changes run 019..., which is a
run of job billing"}
```

a scope is a list of jobs, a list of assets, a list of
[namespaces](namespaces.md), or any of them together, and **the list is the
whole of what may be touched**: `Scope::jobs(["deploy"])` may touch no asset,
`Scope::assets(["orders"])` may launch no job, and
`Scope::jobs(["etl"]).and_assets(["orders"])` may touch exactly those two. a
scope naming something nothing matches may change nothing at all, which is the
right way round for a thing whose job is to say no.

### A namespace is the coarse half

naming jobs one at a time is right for a ci token that launches one deploy and
wrong for a token that stands for a team: eleven jobs today, twelve next week,
and the token has to be edited for the twelfth. a
[namespace](namespaces.md) is the coarser thing to name:

```rust
Identity::operator("finance-ci").scoped_to(Scope::namespaces(["finance"]))
```

that admits **every job and every asset declared in `finance`**, of either
kind, including the one somebody adds next week. the refusal names the
namespace the same way it names a job:

```
$ curl -XPOST -H "authorization: Bearer $FIN" .../api/jobs/payslips/runs
{"error":"fin is scoped to namespace finance, and this changes job payslips"}
```

**a thing in no namespace is in nobody's.** an unnamespaced job is refused to a
namespace-scoped token exactly as another team's job is: `None` is the absence
of a namespace and not a namespace that everything falls into. and the
namespace is read off the registry inside the same check, never off anything in
the request, so it is no more widenable than the rest of a scope.

the check itself did not move: it is the same function, called from the same
guard, reading the same subject off the same matched route. what it gained is
the namespace the subject is declared in, so the rule below covers a namespace
without a second path to remember.

### An unscoped token is unaffected

`Scope::everything()` is what every identity is unless something said
otherwise, and it is what `Identity::new`, `viewer`, `operator` and `admin`
build. an unscoped identity skips the scope check entirely: its requests are
decided by the ladder alone, byte for byte as they were before scopes existed,
and a case asserts that against every row of the table above rather than
assuming it. `Auth::bearer` is unscoped, so the one-token deployment and the ui
are untouched.

### What a scope does to a read

**nothing.** a scope limits what a token may change. a token that can read
reads the whole deployment: every run of every job, every param, every log
line, the event log, the queue.

that is a decision, not an omission, and here is the reasoning. a write names
in its path what it is about, so one check in one place can rule on every write
there will ever be. a read is mostly a *list*: `/api/runs`, `/api/events`,
`/api/queue`, `/api/assets`, the sse stream, `/metrics`. narrowing those means
a filter inside each handler that builds one, which is a check that has to be
remembered at every list, and phase 33's lesson is that a check which must be
remembered at each call site is one that will be forgotten at one. a
confidentiality promise that holds in nine places out of ten is worse than no
promise, because people plan around it and the tenth is where the leak is.

so hestan makes the small exact promise instead. **a scope is not a
confidentiality boundary.** if a ci token must not *see* production run params,
a scope is the wrong tool: put a proxy in front that refuses the reads, or use
`Auth::custom` and return `None` for the paths that token has no business
reading, which is a decision the deployment can make exactly and hestan cannot.

### Deny by default, and why a route added later is covered

the check is in the guard, before any handler, and it reads what a request is
about off the route it matched rather than off a list of endpoints:

| the matched route | what it is about |
| --- | --- |
| `/api/jobs/{name}/…` | that job |
| `/api/assets/{name}/…` | that asset |
| `/api/runs/{id}/…` | whichever job that run is a run of, read off the row |
| `/api/backfills/{id}/…` | whichever asset that backfill is filling in |
| anything with no `{placeholder}` at all | **the deployment** |
| a `{placeholder}` under a noun this does not know | **the deployment** |

**the deployment is what a scoped token may not touch.** so
`POST /api/assets/build`, `POST /api/schedules/state` and
`POST /api/sensors/state` are refused for a scoped token by the rule rather
than by a list, and so is a mutation added next month that does not name a job
or an asset in its path. a run id or a backfill id this deployment cannot
resolve is refused for the same reason: an id nobody can resolve to a subject
is not a subject in the scope.

a case scrapes every route out of the router and asserts that a token scoped to
something else is refused at all of them. a route added tomorrow is in that
scrape the moment it is written, and there is no list to update.

### A scope cannot be widened by the holder

an [`Identity`] is built by an authenticator, in the deployment's own process,
and nothing in a header, a query or a body reaches the scope on it. there is no
input for a holder of a token to widen it through, and a case sends the obvious
attempts (`x-hestan-scope`, `x-scope`, a second `authorization`, a `scope` in
the body) and gets the same refusal.

what a scope *is* worth is bounded by what `Auth::custom` does, exactly as a
role is: hestan checks the scope it was given, not where it came from.

### Where a scope is not

a scope is an **api** limit. it is not a capability check inside the library:
code holding a `Runner` launches whatever it likes, exactly as it always could,
because that code is the deployment rather than a caller of it. and the ui
shows a scope without acting on one; see below.

[`Identity`]: https://docs.rs/hestan/latest/hestan/struct.Identity.html

## The ui

the ui asks `/api/whoami` before anything else. an authenticated deployment
with nothing to present gets a prompt for a token rather than a page of failed
requests; everything else gets the ui it always had.

**a control a role may not use is not rendered.** a viewer's job page says
`launching needs an operator` where the launch controls are; the cancel,
re-run, resume, replay, build, backfill, pause, preset and queue-order
controls are absent the same way. a button that is there and answers 403 teaches people that
the ui lies about what they can do, and the ones who learn that stop reading
the rest of it.

**a scope is shown and not acted on, and that is the one exception to the line
above.** the header reads `ci · operator · jobs deploy`, so somebody whose
launch is about to be refused can see why before they press it. the controls
themselves are not hidden: a token scoped to one job still sees another job's
launch button, and the api answers 403 naming the scope. hiding them would mean
a scope check at every list, tile, palette entry and detail page, which is
exactly the per-call-site check the api rule was built to avoid, and getting it
wrong at one of them would hide a control somebody may in fact use. so the
disagreement is deliberate, it is here rather than left to be discovered, and
the api is where the rule is.

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
  and an expiry: a user store, which hestan does not have and is not going to
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
provider in front of hestan. `Auth::custom` is how you compose one in, and
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

`--token` for a terminal, `HESTAN_TOKEN` for a cron line: an argument is
visible in `ps` to every account on the machine for as long as the process
runs, and a variable is not. the flag wins where both are set. the environment
is read by hestan rather than by the argument parser, which would print the
**value** in `--help`.

a refusal is **exit 8**, a code of its own so a script can tell work that
failed from a credential that was not accepted:

```
$ hestan --server https://hestan.internal runs
error: authentication required: present your credentials; https://hestan.internal
is authenticated: pass --token, or set HESTAN_TOKEN, which keeps it out of ps
$ echo $?
8
```

`doctor` reports whether the deployment it is pointed at is authenticated at
all, in the deployment's own binary, from what it is configured with, and over
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
  nothing else: not a log line, not an event, not an error message, not a
  response body. `tests/auth.rs` drives an authenticated deployment through a
  launch, a retry, a cancel and a pause, then greps both of the server's
  streams, every response it sent, every event and run row, and every byte of
  the database file for the token.
- **an unauthenticated deployment records no actor**, rather than a fabricated
  one. an empty name is not "system": `manual` with no actor means a person
  asked and nothing was checking who.
- **a cancel of a run that is already executing is two events.** the run's own
  terminal event is written by whichever process is executing it, which may not
  be the one that took the request and does not know who asked, so the asking
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
- **a scope is not a rule engine, and reads are not in it.** "ci may launch
  `deploy`" and "finance may drive its own namespace" are expressible; "ada may
  launch `orders_etl` between nine and five if the last run failed" is not, and
  neither is "ci may not see `payments_reconcile`". what a scope narrows is
  which job, asset or namespace a *change* may name. the read half is [not a
  promise hestan makes](#what-a-scope-does-to-a-read), and
  `?namespace=` is a filter on a list rather than a second answer to it.
- **a namespace is not a tenant.** it divides one deployment's declarations so
  a token and a page can name a team's half of them. it is not a separate
  database, a separate process, a separate queue or a confidentiality boundary,
  and [namespaces and owners](namespaces.md) says so in the same words.
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
| the refusal, the loopback rule, the constant-time comparison, `Scope` | `src/auth.rs` |
| what a namespace is and what declares one | `src/whose.rs`, and [namespaces and owners](namespaces.md) |
| the guard, the roles table, what a request is about, `whoami` | `src/server.rs` |
| who did what, in the store | `src/store.rs` (schema v18: `runs.actor`, `events.actor`) |
| the token, the prompt, and what it does not protect against | `ui/src/identity.ts` |
| the token leaving no trace | `tests/auth.rs` |
