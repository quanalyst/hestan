# Security

## The threat model

hestan runs your code and serves an unauthenticated control plane.

there is no authentication, no authorization, and no csrf protection on the
ui or the json api under `/api`. the api is not read-only: it launches runs
with caller-supplied params, retries and cancels runs, triggers asset builds,
and pauses or resumes schedules and sensors. anyone who can reach the port
can do all of that.

so: **bind to loopback.** `serve(([127, 0, 0, 1], 4000))` is the documented
form for a reason. binding `0.0.0.0` — or publishing the container port, or
putting it behind a reverse proxy that doesn't authenticate — hands remote
callers the ability to execute the job code you registered, with arguments
they choose. if it needs to be reachable, terminate authentication in front
of it and never expose the port directly.

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
