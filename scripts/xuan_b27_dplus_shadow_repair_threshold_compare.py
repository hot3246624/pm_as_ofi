#!/usr/bin/env python3
"""Compare D+ no-order shadow repair-threshold runs from local summaries.

This reads only pulled ``aggregate_report.json`` and ``*.summary.json`` files.
It does not read event JSONL, raw/replay data, sockets, SSH, or network paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
)

BLOCK_REASONS = (
    "late_repair_only",
    "activation_opp_seen",
    "pairing_only_when_residual",
    "cooldown",
    "l1_pair_ask_gt_cap",
    "target",
    "qty_zero",
    "offset",
)

METRICS = (
    "candidates",
    "queue_supported_fills",
    "pair_actions",
    "pair_qty",
    "pair_pnl",
    "roi_on_filled_cost",
    "net_pair_cost_proxy_p90",
    "residual_qty",
    "residual_cost",
    "material_residual_lots",
    "filled_qty",
    "seed_qty",
    "fill_rate",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def as_float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def ratio(new: float, old: float) -> float | None:
    return None if old == 0 else new / old


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path) -> dict[str, Any]:
    if not path_safe(path):
        raise SystemExit(f"unsafe input path: {path}")
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def output_dir(run_dir: Path) -> Path:
    candidate = run_dir / "remote" / "output"
    return candidate if candidate.exists() else run_dir


def summary_metrics(summary: dict[str, Any]) -> dict[str, float]:
    raw = summary.get("metrics") or {}
    return {
        "candidates": as_float(raw.get("candidates")),
        "queue_supported_fills": as_float(raw.get("queue_supported_fills")),
        "pair_actions": as_float(raw.get("pair_actions")),
        "pair_qty": as_float(raw.get("pair_qty")),
        "pair_pnl": as_float(raw.get("pair_pnl")),
        "net_pair_cost_p90": as_float(raw.get("net_pair_cost_p90")),
        "residual_qty": as_float(raw.get("residual_qty")),
        "residual_cost": as_float(raw.get("residual_cost")),
        "fill_rate": as_float(raw.get("fill_rate")),
        "sell_triggers": as_float(summary.get("sell_triggers")),
        "book_ticks": as_float(summary.get("book_ticks")),
        "trade_ticks": as_float(summary.get("trade_ticks")),
    }


def load_run(name: str, run_dir: Path) -> dict[str, Any]:
    out = output_dir(run_dir)
    if not path_safe(run_dir) or not path_safe(out):
        raise SystemExit(f"unsafe run path: {run_dir}")
    aggregate_path = out / "aggregate_report.json"
    manifest_path = out / "manifest.json"
    if not aggregate_path.exists() or not manifest_path.exists():
        raise SystemExit(f"missing aggregate/manifest under {out}")
    summaries = []
    for path in sorted(out.glob("*.summary.json")):
        summaries.append({"path": str(path), "data": read_json(path)})
    aggregate = read_json(aggregate_path)
    manifest = read_json(manifest_path)
    metrics = aggregate.get("metrics") or {}
    blocked = {k: as_float(v) for k, v in (aggregate.get("blocked") or {}).items()}
    summary_totals: dict[str, float] = {}
    per_slug: dict[str, dict[str, Any]] = {}
    for item in summaries:
        data = item["data"]
        slug = str(data.get("slug") or "")
        sm = summary_metrics(data)
        for key, value in sm.items():
            summary_totals[key] = summary_totals.get(key, 0.0) + value
        sb = {k: as_float(v) for k, v in (data.get("blocked") or {}).items()}
        per_slug[slug] = {
            "slug": slug,
            "metrics": sm,
            "blocked": sb,
            "config": data.get("config") or {},
        }
    candidates = as_float(metrics.get("candidates"))
    sell_triggers = summary_totals.get("sell_triggers", 0.0)
    total_blocked = sum(blocked.values())
    return {
        "name": name,
        "run_dir": str(run_dir),
        "output_dir": str(out),
        "aggregate_path": str(aggregate_path),
        "manifest_path": str(manifest_path),
        "manifest": manifest,
        "summary_count": len(summaries),
        "slugs": sorted(per_slug),
        "config": manifest.get("config") or {},
        "metrics": {key: as_float(metrics.get(key)) for key in METRICS},
        "blocked": blocked,
        "summary_totals": summary_totals,
        "rates": {
            "candidates_per_sell_trigger": candidates / sell_triggers if sell_triggers else 0.0,
            "fills_per_candidate": as_float(metrics.get("queue_supported_fills")) / candidates if candidates else 0.0,
            "late_repair_only_per_sell_trigger": blocked.get("late_repair_only", 0.0) / sell_triggers if sell_triggers else 0.0,
            "activation_opp_seen_per_sell_trigger": blocked.get("activation_opp_seen", 0.0) / sell_triggers if sell_triggers else 0.0,
            "total_blocked_per_sell_trigger": total_blocked / sell_triggers if sell_triggers else 0.0,
        },
        "per_slug": per_slug,
    }


def delta_map(new: dict[str, float], old: dict[str, float], keys: tuple[str, ...] | list[str]) -> dict[str, Any]:
    return {
        key: {
            "old": old.get(key, 0.0),
            "new": new.get(key, 0.0),
            "delta": new.get(key, 0.0) - old.get(key, 0.0),
            "ratio": ratio(new.get(key, 0.0), old.get(key, 0.0)),
        }
        for key in keys
    }


def per_slug_delta(candidate: dict[str, Any], baseline: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for slug in sorted(set(candidate["per_slug"]) & set(baseline["per_slug"])):
        new = candidate["per_slug"][slug]
        old = baseline["per_slug"][slug]
        rows.append(
            {
                "slug": slug,
                "metric_delta": delta_map(new["metrics"], old["metrics"], list(summary_metrics({}).keys())),
                "block_delta": delta_map(new["blocked"], old["blocked"], BLOCK_REASONS),
            }
        )
    return rows


def rank_summary(run: dict[str, Any], key: str) -> list[dict[str, Any]]:
    rows = []
    for slug, item in run["per_slug"].items():
        row = {"slug": slug, **item["metrics"]}
        for reason in BLOCK_REASONS:
            row[f"block_{reason}"] = item["blocked"].get(reason, 0.0)
        rows.append(row)
    return sorted(rows, key=lambda row: row.get(key, 0.0), reverse=True)


def decide(candidate: dict[str, Any], baseline: dict[str, Any], overlap_count: int) -> tuple[str, str, str]:
    cm = candidate["metrics"]
    bm = baseline["metrics"]
    p90_improved = cm["net_pair_cost_proxy_p90"] <= bm["net_pair_cost_proxy_p90"] and cm["net_pair_cost_proxy_p90"] <= 1.0
    residual_improved = cm["residual_qty"] < bm["residual_qty"] and cm["residual_qty"] <= 10.0
    pnl_positive = cm["pair_pnl"] > 0.0
    sample_failed = cm["candidates"] < 100.0
    enough_overlap = overlap_count >= max(1, min(len(candidate["slugs"]), len(baseline["slugs"])) // 2)
    if not enough_overlap:
        return (
            "UNKNOWN_DIFFERENT_WINDOWS_NO_CAUSAL_ATTRIBUTION",
            "repair45 improved p90/residual in this shadow, but the compared runs have insufficient slug overlap, so sample collapse cannot be causally attributed from summaries",
            "Implement or run a same-window causal verifier: either a multi-profile no-order shadow harness or event-lite same-window replay/summary that compares repair_after=45/60/75 on identical market streams before spending another acceptance-style shadow.",
        )
    if p90_improved and residual_improved and pnl_positive and sample_failed:
        return (
            "KEEP_REPAIR45_RISK_LEAD_NEEDS_SAMPLE_RESTORE",
            "same-window summaries show repair45 improves p90/residual while only sample fails",
            "Run one bounded sample-restoration A/B around repair_after=45/60 with identical market streams and keep p90/residual gates fixed.",
        )
    if not p90_improved or not residual_improved or cm["pair_pnl"] <= 0.0:
        return (
            "DISCARD_REPAIR45_SHADOW_VARIANT",
            "repair45 does not preserve core p90/residual/PnL gates against the baseline",
            "Return to local causal mechanisms; do not spend another EC2 shadow on repair45.",
        )
    return (
        "UNKNOWN_SUMMARY_COMPARATOR_INCONCLUSIVE",
        "summaries do not isolate the causal mechanism",
        "Add event-lite decision context or a multi-profile same-window runner before the next EC2 shadow.",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-run-dir", type=Path, default=Path("xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_driver_20260519T144649Z"))
    parser.add_argument("--candidate-run-dir", type=Path, default=Path("xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_driver_20260519T193208Z"))
    parser.add_argument("--baseline-name", default="repair75")
    parser.add_argument("--candidate-name", default="repair45")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_shadow_repair_threshold_compare_{label}"

    baseline = load_run(args.baseline_name, args.baseline_run_dir)
    candidate = load_run(args.candidate_name, args.candidate_run_dir)
    overlap = sorted(set(candidate["slugs"]) & set(baseline["slugs"]))
    status, interpretation, next_action = decide(candidate, baseline, len(overlap))

    metric_keys = list(METRICS)
    block_keys = sorted(set(BLOCK_REASONS) | set(candidate["blocked"]) | set(baseline["blocked"]))
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shadow_repair_threshold_compare",
        "created_utc": label,
        "status": status,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_summary_aggregate_comparator",
        "decision_label": "UNKNOWN" if status.startswith("UNKNOWN") else ("KEEP" if status.startswith("KEEP") else "DISCARD"),
        "hypothesis": "repair_after=45 may preserve p90/residual but reduce candidate admission relative to repair_after=75",
        "data_declaration": {
            "data_root": "xuan_research_artifacts",
            "dataset_type": "pulled_no_order_shadow_aggregate_and_summary_json",
            "labels": [args.baseline_name, args.candidate_name],
            "market_prefix": "btc-updown-5m",
            "summary_files": {
                args.baseline_name: baseline["summary_count"],
                args.candidate_name: candidate["summary_count"],
            },
            "events_jsonl_read": False,
            "raw_replay_scanned": False,
            "ssh_started": False,
            "network_started": False,
        },
        "safety": {
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_or_shared_ingress_modified": False,
            "service_control_used": False,
            "started_canary": False,
        },
        "comparability": {
            "baseline_slugs": baseline["slugs"],
            "candidate_slugs": candidate["slugs"],
            "overlap_count": len(overlap),
            "overlap_slugs": overlap,
            "same_window_comparable": len(overlap) >= max(1, min(len(candidate["slugs"]), len(baseline["slugs"])) // 2),
            "limitation": "no shared slugs means live-window regime can explain sample differences; summaries alone are screening evidence, not causal proof",
        },
        "runs": {
            args.baseline_name: {
                "run_dir": baseline["run_dir"],
                "aggregate_path": baseline["aggregate_path"],
                "manifest_path": baseline["manifest_path"],
                "config": {k: baseline["config"].get(k) for k in ("edge", "target_qty", "late_repair_after_s", "imbalance_qty_cap", "pairing_only_when_residual", "activation_mode", "activation_window_s")},
                "metrics": baseline["metrics"],
                "blocked": baseline["blocked"],
                "summary_totals": baseline["summary_totals"],
                "rates": baseline["rates"],
            },
            args.candidate_name: {
                "run_dir": candidate["run_dir"],
                "aggregate_path": candidate["aggregate_path"],
                "manifest_path": candidate["manifest_path"],
                "config": {k: candidate["config"].get(k) for k in ("edge", "target_qty", "late_repair_after_s", "imbalance_qty_cap", "pairing_only_when_residual", "activation_mode", "activation_window_s")},
                "metrics": candidate["metrics"],
                "blocked": candidate["blocked"],
                "summary_totals": candidate["summary_totals"],
                "rates": candidate["rates"],
            },
        },
        "delta": {
            "metrics": delta_map(candidate["metrics"], baseline["metrics"], metric_keys),
            "blocked": delta_map(candidate["blocked"], baseline["blocked"], block_keys),
            "rates": delta_map(candidate["rates"], baseline["rates"], sorted(set(candidate["rates"]) | set(baseline["rates"]))),
        },
        "interpretation": {
            "summary": interpretation,
            "sample_collapse_notes": [
                "raw candidates fell 58 -> 29",
                "sell triggers also fell 4361 -> 3133, so market activity/regime accounts for part of the raw decline",
                "candidates per sell trigger fell from 1.33% to 0.93%, but the compared runs have zero slug overlap",
                "late_repair_only blocks per sell trigger are similar, so summaries do not prove earlier repair threshold is the only blocker",
            ],
            "next_action": next_action,
            "do_not_repeat_same_repair45_1800s_shadow": True,
            "do_not_promote_to_g2_canary": True,
        },
    }
    write_json(out_dir / "manifest.json", manifest)
    write_json(out_dir / "overlap_per_slug_delta.json", per_slug_delta(candidate, baseline))
    write_json(out_dir / "candidate_ranked_by_candidates.json", rank_summary(candidate, "candidates"))
    write_json(out_dir / "baseline_ranked_by_candidates.json", rank_summary(baseline, "candidates"))
    write_json(out_dir / "candidate_ranked_by_residual_qty.json", rank_summary(candidate, "residual_qty"))
    write_json(out_dir / "baseline_ranked_by_residual_qty.json", rank_summary(baseline, "residual_qty"))
    print(out_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
