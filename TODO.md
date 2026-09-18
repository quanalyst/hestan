# Open work

These are candidate improvements, not a committed roadmap.

## Output data in notification hooks

`RunEvent` identifies the run and its outcome but does not include saved samples
or output metadata. Reporting hooks must query the store themselves.

Decide whether to expose a lookup handle or include selected data in the payload.
Account for payload size, read failures, per-run versus asset-history queries,
and the transactional guarantees of [durable delivery](docs/notifications.md#durable-delivery).

## Live Teams integration testing

- [ ] Test `notify::TeamsWorkflow` against a dedicated Teams test channel using
  a configured Workflows webhook and its chosen authentication mode.
- [ ] Run successful and failed jobs; verify the rendered cards, readable names,
  persistent identifiers, error excerpts, and optional run links.
- [ ] Compare Hestan delivery attempts with Workflow run history and actual
  channel messages. Webhook acceptance does not prove downstream posting.

Local adapter tests cover the payload and HTTP behavior. Live provider testing
remains outstanding; keep webhook URLs and credentials outside the repository.
See [named notifications](docs/notifications.md#named-destinations).

## Run query options

Replace the positional filters on `Store::runs` with a query type, following
`EventQuery`. Keep a compatible entry point for existing callers.

## Public row constructors

Add constructors for public row types before considering restrictions on struct
literals. Preserve support for test fixtures and fake stores; see
[stability](docs/stability.md#the-structs).
