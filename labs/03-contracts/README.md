# Lab 03: executable data-contract compatibility

This lab makes a deliberately small producer/consumer contract executable. It
classifies changes before rollout and runs representative JSON records through
a selected consumer version. Everything is synthetic and uses the Python
standard library; setup and teardown require no service or network access.

## Contract and compatibility policy

Each contract declares `format`, `version`, and named `fields`. Every field has
a `type` (`string`, `integer`, or `boolean`) and explicit `required` flag.
Optional fields must have a typed, stable `default`; `allowed` constrains values
when present.

This lab uses one conservative policy: **a new producer contract is compatible
when it preserves every guarantee consumed from the old contract**. The rules
are explicit in `launchpad.contracts.compare`:

- adding an optional field with a stable default is compatible;
- adding a required field is breaking for existing records/producers;
- removing or renaming a consumed field is breaking;
- changing a type, default, or allowed-value set is breaking;
- making an existing optional field required is breaking for records that
  relied on its default;
- making a formerly required field optional is breaking because the producer
  may stop supplying it;
- reordering an unchanged allowed-value set is not a change.

This is one teaching policy, not a universal definition of backward, forward,
or full compatibility. Name the direction and consumer behavior whenever a
real system chooses a policy.

## Run the lab

From the repository root:

```bash
python3 -m launchpad.contracts compare \
  --old labs/03-contracts/contracts/orders-v1.json \
  --new labs/03-contracts/contracts/orders-v2.json

python3 -m launchpad.contracts validate \
  --contract labs/03-contracts/contracts/orders-v2.json \
  --records labs/03-contracts/records/orders-v1.json \
  --unknown-fields strict
```

Both commands exit `0`. The comparison reports `"compatible": true`; the
validation injects `"sales_channel": "unknown"` into the normalized v1 record.
Results are deterministic JSON.

Now exercise a future field against the v1 consumer:

```bash
python3 -m launchpad.contracts validate \
  --contract labs/03-contracts/contracts/orders-v1.json \
  --records labs/03-contracts/records/orders-v2.json \
  --unknown-fields strict

python3 -m launchpad.contracts validate \
  --contract labs/03-contracts/contracts/orders-v1.json \
  --records labs/03-contracts/records/orders-v2.json \
  --unknown-fields permissive
```

Strict mode prints an `unknown_field` diagnostic for `sales_channel` and exits
`1`. Permissive mode ignores the unknown field, returns the six consumed fields,
and exits `0`. Malformed contracts or CLI input exit `2`. Every record or
compatibility error identifies contract version, field, rule, message, and a
corrective action.

Run every compatible and breaking fixture encoded in the tests:

```bash
python3 -m unittest tests.test_contracts -v
```

The suite covers optional/required additions, removal/rename, type and enum
changes, strict/permissive future fields, default fallback, diagnostics, and
the unchanged six/seven-column Lab 01 CSV behavior.

The `contracts/` directory includes one directly executable file for each
change: compatible optional `sales_channel`, required `currency`, renamed
`order_id`, string `amount_cents`, and a narrowed status enum. Compare any of
them with `orders-v1.json`; breaking fixtures print their specific findings and
exit `1`.

## Migration rehearsal

### Consumer-first (preferred here)

1. Baseline: v1 producers and consumers; save validation/error counts.
2. Deploy the v2 consumer first. Dual-read representative v1 records through
   both consumers. The v2 consumer must inject the `unknown` fallback and
   produce the same business result.
3. Verify zero validation errors, unchanged order/revenue counts, and explicit
   metrics for fallback use. Do not cut over if any required field/type/enum
   diagnostic appears.
4. Deploy the v2 producer with `sales_channel`. During the overlap, old
   consumers must use the explicitly tested permissive policy.
5. Dual-read v2 records, reconcile consumed six-field results and the new field,
   then remove the old path only after the agreed observation window.

### Producer-first (conditional)

Producer-first is safe only after proving every active v1 consumer is
permissive for unknown fields. The strict command above intentionally fails and
is the stop signal. If consumer inventory or policy is unknown, do not deploy
the producer first.

### Rollback

If the new producer causes errors, restore the v1 producer and keep v2
consumers deployed: their stable default reads v1 records. Drain or replay v2
records through the verified fallback before removing v2 consumers. Never roll
the consumer back first while v2 records are still in flight. Record the
contract versions, error rules, reconciliation result, owner, and decision.

## What this format is not

This repository-owned JSON shape is intentionally tiny. It does not implement
JSON Schema vocabularies or meta-schema validation. It has no Avro writer/reader
schema resolution, Protobuf field numbers or binary wire format, generated
code, subject/version storage, access control, or managed-registry compatibility
modes. Use the published formats and registry products when those capabilities
are required; do not extend this exercise into an unofficial clone.

Primary references reviewed 2026-11-24 (UTC):

- [Apache Avro 1.12.0 specification and schema resolution](https://avro.apache.org/docs/1.12.0/specification/)
- [Protocol Buffers proto3 guide: updating message types](https://protobuf.dev/programming-guides/proto3/#updating)
- [JSON Schema Draft 2020-12 specification](https://json-schema.org/draft/2020-12)
- [Confluent Schema Registry compatibility concepts](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)

## Teardown and limits

The commands create no databases, environments, or services, so teardown is
normally unnecessary. Delete only copies you created for experiments. This lab
does not parse dates, express nested/array schemas, discover consumers, store
contracts, negotiate versions, or prove safe production rollout. It validates
representative JSON objects; Lab 01 remains the independent CSV implementation.
