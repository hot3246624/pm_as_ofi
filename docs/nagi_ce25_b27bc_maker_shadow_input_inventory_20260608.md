# NAGI + CE25 + B27BC Maker Shadow Input Inventory - 2026-06-08

## Purpose

The maker-shadow research pipeline needs local CSV input. The automation should
not fabricate data and should not silently run arbitrary CSV files. The input
inventory tool makes that gate explicit:

```text
scripts/inventory_nagi_ce25_b27bc_maker_shadow_inputs.py
```

It scans bounded local CSV samples and classifies whether each file can feed the
no-order maker-shadow pipeline.

## Scope

Allowed:

- Local CSV header/sample inspection.
- Local SHA256 provenance.
- Contract compatibility checks for BTC 5m maker-shadow rows.
- CE25 coverage-gate sampling.
- Public SELL touch and visible-depth proxy checks.
- Limit-order minimum guard: at least 5 shares.

Not allowed:

- Network access.
- WebSocket or OOS runner startup.
- Private key/API credential import.
- Order, cancel, redeem, canary, live, deploy, or funding actions.
- Maker-fill truth or private queue-priority claims.

## Classification

- `READY_MAKER_SHADOW_INPUT`: sampled rows include CE25-gated BTC 5m rows with
  public SELL touch and enough visible/proxy depth to satisfy the 5-share
  limit-order floor.
- `UNKNOWN_COMPATIBLE_LOW_SIGNAL_MAKER_SHADOW_INPUT`: the CSV has enough
  contract fields to parse, but sampled rows do not yet contain a runnable
  >=5-share maker-shadow opportunity.
- `BLOCKED_NOT_MAKER_SHADOW_INPUT`: sampled rows do not satisfy the basic input
  contract.
- `BLOCKED_CSV_TOO_LARGE_FOR_AUTOMATION_SAMPLE`: file exceeds the configured
  bounded-scan size.

## Output

Each run writes:

- `decision_register.json`
- `input_inventory.csv`

The decision register includes `preferred_input_csv` only when a ready input is
found. Automations may run the maker-shadow pipeline on that preferred file,
but must keep all status at local research/no-order proxy level.

## Current Local Finding

The current default materializer and inventory path is precise but blocked by
source availability:

- Default materializer run excluding `smoke`/`fixture` saw 0 source files and
  materialized 0 rows.
- A diagnostic `--include-smoke` run saw local synthetic event/activity files,
  which confirms the converter path works, but those files remain excluded from
  evidence.
- Inventory over the default materialized CSV, `data/inputs`, `evidence`, and
  `xuan_research_artifacts` found `ready_input_count = 0`.

The next executable research step is not parameter tuning. It is to place a
non-smoke/non-fixture BTC 5m public event or public-activity row export into a
bounded local input path. Public-activity rows must include a market slug and
must be `source_side = SELL`; BUY rows are not post-only maker bid touch
evidence.
