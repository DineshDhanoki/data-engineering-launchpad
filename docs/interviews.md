# Interview practice bank

These are representative practice prompts, **not** claims about a company's actual or most-frequently-asked interview questions. For every answer: state assumptions, show a small example, discuss edge cases, and explain how you would verify it.

## Newcomer

1. What is the grain of a table? How can an incorrect grain double-count revenue? *Strong answer:* identify the unique business key and demonstrate a one-to-many join trap.
2. Write SQL to find customers with no orders. *Strong answer:* `LEFT JOIN ... IS NULL` or `NOT EXISTS`, with null behavior explained.
3. Explain `WHERE` vs `HAVING`. *Strong answer:* row filtering vs aggregate-group filtering and a concrete query.
4. What is an idempotent pipeline? *Strong answer:* same input and run key produce the same result after retries; demonstrate with Lab 01.
5. Why use integer cents for money in this lab? *Strong answer:* avoid binary floating-point rounding; mention decimals and currency rules in production.
6. When would you choose a batch load instead of a stream? *Strong answer:* latency needs, complexity, cost, and failure recovery.

## Around 2 years

1. An incremental load missed updates arriving three days late. How would you repair it? *Strong answer:* bounded lookback or CDC, idempotent merge, backfill, reconciliation.
2. A scheduled job reruns after failing halfway through. How do you prevent duplicate data? *Strong answer:* transaction or atomic publish, stable keys, upsert, and retry tests.
3. How would you test a transformation pipeline? *Strong answer:* schema, uniqueness, null, relationships, business invariants, fixtures, CI, and production monitoring.
4. What causes a slowly changing dimension Type 2 table to be wrong? *Strong answer:* overlapping validity intervals, tie-breaking, late events, and current-row uniqueness.
5. Your dashboard differs from a finance report. How do you investigate? *Strong answer:* agree grain and definition, compare filters/time zones, trace lineage, reconcile counts.
6. Which operational metrics would you collect for Lab 01? *Strong answer:* freshness, input/output/reject counts, lag, runtime, and alert thresholds.

## Around 4 years

1. Design a CDC pipeline from an OLTP database to analytics. *Strong answer:* snapshot + log handoff, ordering, deduplication, schema evolution, replay, and recovery.
2. A warehouse query became 10x slower. Diagnose it. *Strong answer:* query plan, scan size, join cardinality, skew, partitions/clustering, and measured changes.
3. Compare a star schema with a wide denormalized table. *Strong answer:* workload, governance, semantic consistency, performance, and maintenance trade-offs.
4. How do you set a data product SLO? *Strong answer:* consumer need, freshness/availability/correctness, error budget, ownership, and alert actionability.
5. A team wants to stream every source. What questions do you ask? *Strong answer:* required latency, correctness semantics, cost, operational capacity, and failure modes.

## Around 6+ years

1. Migrate a critical pipeline without breaking consumers. *Strong answer:* inventory, contracts, dual-run comparison, staged cutover, rollback, ownership.
2. A privacy incident exposed sensitive fields in analytics. Lead the response. *Strong answer:* containment, access audit, deletion/retention, communication, root cause, preventive controls.
3. Choose between managed warehouse, lakehouse, and custom streaming stack. *Strong answer:* workloads, team skills, governance, portability, total cost, and reversible decisions.
4. How do you prioritize reliability work against feature requests? *Strong answer:* quantify incident impact, SLOs, risk, effort, and stakeholder alignment.
5. Review a junior engineer's pipeline that passes tests but fails on replays. *Strong answer:* reproduce, teach deterministic keys/atomicity, add regression tests, and improve review checklist.

Contribute more questions with a level, example data, evaluation rubric, and links for any tool-specific claims. Avoid publishing confidential interview material.
