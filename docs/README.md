# Hestan documentation

## Start here

- [Getting started](getting-started.md): create and schedule a job.
- [Concepts](concepts.md): jobs, ops, runs and dependencies.
- [Choosing components](choosing.md): jobs or assets, schedules or sensors, and storage options.
- [Web UI](web-ui.md): navigation, status and run inspection.
- [Command line](cli.md): commands, configuration and exit codes.

## Define work

- [Assets](assets.md): lineage, fingerprints and materialization.
- [Display names and grouping](presentation.md): names, subgroups, labels and grouping views.
- [Scheduling](scheduling.md): cron, timezones and missed runs.
- [Sensors](sensors.md): event-driven runs and source probes.
- [Freshness](freshness.md): policies and late-work detection.
- [Launching](launching.md): parameters, presets, tags and subsets.
- [Replay](replay.md): rerun work using recorded inputs.
- [Notifications](notifications.md): hooks and delivery.

## Work with data

- [Connecting to data](connecting.md): clients, retries and credentials.
- [Resources](resources.md): shared and per-run dependencies.
- [Typed I/O](typed-io.md): typed inputs, outputs and parameters.
- [I/O managers](io-managers.md): output storage.
- [Op state](state.md): persisted watermarks.
- [Metadata](metadata.md): structured facts attached to outputs.
- [HTTP sources](http-sources.md): REST pulls.
- [dbt](dbt.md): register models as assets.
- [Secrets](secrets.md): parameter redaction and its limits.

## Run Hestan

- [Embedding](embedding.md): integrate Hestan into an application.
- [Authentication](auth.md): identities, roles and access controls.
- [Namespaces and owners](namespaces.md): scopes and ownership metadata.
- [Scaling](scaling.md): workers, queues, concurrency and leases.
- [Isolation](isolation.md): subprocess execution and resource limits.
- [Containers](containers.md): Docker, Compose and Kubernetes examples.
- [Deployment identity](deployment.md): installation and application-build metadata.
- [Storage](storage.md): backends, schema and migrations.
- [Backup and recovery](backup.md): backup and restore procedures.

## Reference

- [HTTP API](http-api.md): endpoints, fields and errors.
- [Events](events.md): event types and payloads.
- [Logs](logs.md): capture and retrieval.
- [Metrics](metrics.md): Prometheus metrics.
- [Stability](stability.md): compatibility policy.
- [Development](development.md): repository layout, tests and build instructions.
