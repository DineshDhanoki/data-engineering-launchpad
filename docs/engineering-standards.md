# Engineering standards for new labs

These are review criteria, not claims that the starter pipeline implements every production concern. Updated: October 2026.

## Correctness and recovery

- Define source schema, grain, keys, update ordering, and allowed nulls before coding.
- Make reruns deterministic; use idempotent writes or atomic replacement and test retries after partial failure.
- Specify late-data and backfill behavior. Record source offsets or partitions where appropriate.
- Reconcile input, rejected, transformed, and published record counts. Test business invariants.

## Operations

- Record freshness, job duration, row counts, failures, and data-quality results. Alerts need owners and runbooks.
- Test a failure and a recovery path, not only the happy path.
- Set realistic service levels from consumer needs; distinguish delayed data from incorrect data.
- Measure cost and performance with representative synthetic data. Never present toy benchmarks as production evidence.

## Security and community

- Use synthetic or consented data; classify sensitive fields and minimize retention.
- Keep secrets out of Git; use scoped credentials and avoid logging personal records.
- Review dependency and workflow changes before running contributor code.
- Document licenses and attribution for datasets and educational material.

## Learning-module checklist

Every module should include: objectives, prerequisites, setup and teardown, a runnable exercise, expected result, at least one automated check, a failure scenario, production limitations, primary-source links, and a dated review note.

Sources: [Apache Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) for retry-safe tasks; [dbt data tests](https://docs.getdbt.com/docs/build/data-tests) for model and source assertions. These are tool-specific examples, not universal prescriptions.
