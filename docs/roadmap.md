# Learning roadmap

Work through the milestones in order, but use the experience labels only as a guide. Each track should end with a working artifact, tests, and a short design note. Updated: October 2026; review tool-specific links regularly.

## Foundations (newcomer)

- Learn Python file handling, exceptions, typing, and tests; SQL filtering, joins, grouping, window functions, and transactions.
- Run [Lab 01](../labs/01-orders/README.md); explain bronze/silver/gold tables and reproduce a failure.
- Build a tiny schema diagram, add an edge-case test, and write a one-page explanation of idempotency.
- Interview focus: SQL correctness, data grain, batch vs. streaming, keys, nulls, duplicates.

## Reliable pipelines (~2 years)

- Extend Lab 01 to handle source schema changes, late-arriving updates, backfills, and partitioned runs.
- Add metrics for input count, rejected rows, lag/freshness, duration, and reconciliation. Define alert thresholds and runbook steps.
- Add data tests at source and model boundaries. Compare orchestration choices and implement a DAG in a *separate* optional lab.
- Interview focus: debugging failures, incremental loads, retries, warehouse modeling, tests, CI.

## Platform engineering (~4 years)

- Design storage, compute, and orchestration for a larger workload. Document cost, security, recovery, and migration trade-offs.
- Compare CDC with periodic extraction; batch with streaming; star schema with normalized models; lakehouse with warehouse.
- Add lineage, data ownership, contracts, access control, and SLOs to a capstone architecture.
- Interview focus: partitioning, skew, query plans, observability, disaster recovery, stakeholder trade-offs.

## Senior/lead (~6+ years)

- Write an architecture decision record for a multi-team platform and an incident retrospective for a broken data product.
- Plan migration with rollback, governance, privacy classification, and spend controls.
- Demonstrate mentoring: review a junior contributor's pipeline for correctness and operational risk.
- Interview focus: system design, roadmap prioritization, operational excellence, leadership, and communication.

## Planned modules (contributors welcome)

1. SQL modeling and dbt data tests, using the [official data-test guide](https://docs.getdbt.com/docs/build/data-tests).
2. Orchestration and retry-safe tasks, using [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html).
3. CDC and streaming with a reproducible local fixture and documented failure semantics.
4. Columnar files, partitioning, and query-plan benchmarks with measured costs.
5. Cloud deployment examples, each with teardown and spending guidance.

Do not add a tool simply because it is fashionable. A module needs learning objectives, setup, a runnable exercise, automated checks, failure cases, source links, and a dated review note.
