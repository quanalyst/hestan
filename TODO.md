# Open work

These are candidate improvements, not a committed roadmap.

## Output data in notification hooks

`RunEvent` identifies the run and its outcome but does not include saved samples
or output metadata. Reporting hooks must query the store themselves.

Decide whether to expose a lookup handle or include selected data in the payload.
Account for payload size, read failures, per-run versus asset-history queries,
and the transactional guarantees of [durable delivery](docs/notifications.md#durable-delivery).

## Notification integrations

The built-in HTTP helpers support Slack and generic webhooks. Consider a Teams
payload helper and an email example using an application-provided sender.
The existing helpers spawn HTTP requests and log failures after returning.
Durable delivery retries hook panics, so it cannot observe those HTTP failures.
Any new helper must make its delivery guarantees explicit.

## Run query options

Replace the positional filters on `Store::runs` with a query type, following
`EventQuery`. Keep a compatible entry point for existing callers.

## Public row constructors

Add constructors for public row types before considering restrictions on struct
literals. Preserve support for test fixtures and fake stores; see
[stability](docs/stability.md#the-structs).
