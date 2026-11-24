# Deterministic synthetic order generator

Use `launchpad.generator` when a lab needs more rows than the committed sample.
It uses only the Python standard library and emits synthetic identifiers; it
does not model real people, businesses, or statistically representative
commerce behavior.

## Generate and verify a valid fixture

Run from the repository root with Python 3.11–3.14:

```bash
python3 -m launchpad.generator \
  --seed 42 \
  --orders 100 \
  --max-versions 4 \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --late-arrival-window 3 \
  --output generated/orders.csv

python3 -m launchpad.pipeline run \
  --input generated/orders.csv \
  --db generated/orders.db
python3 -m launchpad.pipeline report --db generated/orders.db
```

The default manifest path is `<output>.manifest.json`; use `--manifest` to
choose another path. Compare the pipeline's `bronze_versions` and
`current_orders` with the manifest and compare `report` output with
`expected.daily_revenue`. `version_status_counts` describes every emitted
version while `current_status_counts` describes only each order's final state.
`csv_sha256` detects accidental fixture changes.

The required parameters are explicit: seed, order count, maximum versions per
order, inclusive start/end dates, and output path. The generator rejects more
than 1,000,000 orders, 100 versions per order, or 2,000,000 potential versions
(`orders * max-versions`) to limit accidental resource use. It replaces each
named output atomically, but the CSV/manifest pair is not a transactional
filesystem unit and it does not delete old files with other names.

## Data rules and defaults

Every order has a stable synthetic order/customer ID, order date, amount, and
sales channel. Versions have strictly increasing UTC `updated_at` values.
Histories progress through supported states: paid and cancelled orders progress
from pending when multiple versions exist; refunded orders progress through
paid when enough versions exist. A single-version order may already be in its
final state.

Defaults are integer weights, not promises of exact percentages:

| Dimension | Default weights |
| --- | --- |
| Final status | `paid=60,pending=15,cancelled=15,refunded=10` |
| Sales channel | `web=50,mobile=35,store=15` |

Override them with comma-separated integer weights. Omitted values have zero
probability, and the weights need not total 100:

```bash
python3 -m launchpad.generator \
  --seed 9 --orders 20 --max-versions 3 \
  --start-date 2026-02-01 --end-date 2026-02-02 \
  --status-weights paid=1,refunded=1 \
  --channel-weights web=1,mobile=2 \
  --output generated/custom.csv
```

`--late-arrival-window N` rotates rows only inside blocks of at most `N + 1`,
so no row moves more than `N` positions. It changes arrival order without
changing source timestamps or expected current state. Zero preserves canonical
source order.

The CSV uses UTF-8 and LF line endings. Selection is derived from SHA-256 of a
versioned seed/field key instead of Python's `random` implementation. JSON keys,
aggregate dates, and line endings are canonical. Therefore identical parameters
produce byte-identical CSV and manifest files across supported operating
systems. Changing the generator version may intentionally change that contract.

## One-failure fixtures

Pass at most one `--invalid` mode. Each starts from otherwise valid data:

| Mode | Injected failure | Expected pipeline diagnostic |
| --- | --- | --- |
| `duplicate-header` | A second `status` heading | CSV column contract failure |
| `conflicting-event` | Same event key with a changed amount | Conflicting version |
| `missing-timezone` | First timestamp loses its UTC offset | Timezone requirement |
| `oversized-amount` | First amount exceeds SQLite's supported integer range | Amount range failure |

Example:

```bash
python3 -m launchpad.generator \
  --seed 42 --orders 5 --max-versions 2 \
  --start-date 2026-01-01 --end-date 2026-01-02 \
  --invalid missing-timezone \
  --output generated/missing-timezone.csv
python3 -m launchpad.pipeline run \
  --input generated/missing-timezone.csv \
  --db generated/invalid.db
```

The manifest records `invalid_fixture.mode` and `expected_error`. Invalid data
must never be mixed with a performance or correctness measurement.

## Cleanup and limits

Generated data is ignored by Git when kept under `generated/` and must not be
committed in bulk. Remove only the directory created for this exercise:

```bash
rm -r generated
```

This tool is a deterministic fixture builder, not a load-test harness, privacy
anonymizer, production data simulator, statistical model, or performance claim.
The manifest describes expected logical results, but the pipeline remains the
independent verifier. Large generations consume memory proportional to emitted
versions because canonical CSV bytes and manifest aggregates are prepared
before publication.
