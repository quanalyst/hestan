# Security

## The threat model

hestan runs your code and serves a control plane. the api is not read-only: it
launches runs with caller-supplied params, retries and cancels runs, triggers
asset builds, starts backfills, moves queue positions, and pauses or resumes
schedules and sensors.

**with no authenticator configured, `serve` binds loopback and refuses
anything else.** that is a refusal rather than a warning, and it is the whole
of what stands between "one process on one machine" and "a button on the
internet that runs arbitrary jobs".

an address anyone can reach needs an [authenticator](docs/auth.md):
`Auth::bearer(token)` for one shared admin token, compared in constant time and
never logged, or `Auth::custom(|req| …)` to hand the decision to something that
already knows who your people are. three roles — viewer, operator, admin — with
the endpoint-by-endpoint mapping in the docs. `Auth::None` turns the refusal
off for a deployment fronted by something that authenticates for it, and says
so at startup.

**there is still no csrf protection**, no user store, no sessions and no
per-job permissions, and hestan serves plain http — terminate tls in front of
it, or a bearer token on a network you do not control is a bearer token anyone
on that network can read. `docs/auth.md` has a section on what this
deliberately is not.

two smaller notes:

- run params, op outputs, and event data are stored verbatim in the sqlite
  file and served back over the api. don't put secrets in them. read
  credentials from the environment instead — `HttpSource::bearer_env` does.
- the sqlite file is protected by nothing but filesystem permissions, and it
  holds the full history of every run.

## Reporting

report vulnerabilities privately through github's
[security advisories](https://github.com/quanalyst/hestan/security/advisories/new)
for this repository. please don't open a public issue for something
exploitable.

this is a pre-1.0 project maintained in spare time: expect an
acknowledgement within a week or so, and fixes on a best-effort basis. only
the latest release is supported.
