# Data Engineering Launchpad

An open-source, hands-on path from your first SQL query to designing reliable data platforms. Start with the runnable order-processing pipeline, then work through the [SQL lab](labs/02-sql/README.md), [roadmap](docs/roadmap.md), [interview bank](docs/interviews.md), and [contribution guide](CONTRIBUTING.md).

> This is a growing curriculum, not a claim that one repository can teach every tool or guarantee an interview outcome. The first release focuses on transferable fundamentals. Cloud, streaming, and distributed-system modules are planned and need contributors.

## Who is this for?

| Experience | Start here | Evidence to build |
| --- | --- | --- |
| New to data engineering | Python, SQL, files, and the first pipeline | Run it, explain each table, write a new test |
| ~2 years | Incremental loads, data quality, orchestration, CI | Handle late updates and failure recovery |
| ~4 years | Platform design, performance, contracts, cost | Design an observable multi-team pipeline |
| ~6+ years | Architecture and leadership | Defend trade-offs, migration, governance, incident response |

These are study tracks, not strict job-title or age gates. See [roadmap](docs/roadmap.md) for milestones.

## Run the first lab

Requires Python 3.11+; no third-party packages or cloud account.

```bash
python3 -m launchpad.pipeline run --input labs/01-orders/orders.csv --db /tmp/launchpad-orders.db
python3 -m launchpad.pipeline report --db /tmp/launchpad-orders.db
python3 -m unittest discover -s tests -v
```

Run `run` twice and compare the report: the result should not double-count orders. Edit a row's `updated_at` and status to see how the latest state and revenue change. The lab uses synthetic data only.

The flow is `CSV -> validated bronze_orders -> latest silver_orders -> daily_revenue`. It validates the entire file before writing, rejects conflicting records with the same event key, commits atomically, and recomputes derived tables from a deterministic latest-record rule. See the [lab guide](labs/01-orders/README.md) for exercises and limitations.

## What is in this release

- A tested, local batch pipeline, sample dataset, and SQL practice lab.
- Four experience-based learning tracks with explicit outcomes.
- Interview questions with answer rubrics, not memorized scripts.
- [Engineering standards](docs/engineering-standards.md) for future labs.
- Community contribution, conduct, security, and issue templates.
- CI that runs unit tests and an end-to-end smoke check.

## What is not yet implemented

Airflow/dbt orchestration, streaming, a cloud deployment, a distributed warehouse, dashboards, and real production data are roadmap items—not features we pretend to have. Start with an [issue](.github/ISSUE_TEMPLATE/learning-module.md) if you want to help build one.

## Why these practices?

This curriculum treats retries and deterministic re-runs as first-class concerns, consistent with [Apache Airflow's best-practices guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html). The testing and transformation track follows [dbt's data-testing guidance](https://docs.getdbt.com/docs/build/data-tests) and [dbt's project best practices](https://docs.getdbt.com/best-practices). Tool-specific guidance can change; contributors should link primary documentation and record when a module was reviewed.

## License and community

MIT licensed. See [LICENSE](LICENSE), [CONTRIBUTING.md](CONTRIBUTING.md), and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Please do not commit credentials, private datasets, or real customer records.
