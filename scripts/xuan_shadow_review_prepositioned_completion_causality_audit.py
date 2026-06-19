#!/usr/bin/env python3
"""Causality audit for prepositioned maker-completion replay proxy.

The prepositioned simulator can show whether there were enough later-or-earlier
trade-through opportunities in the window. This audit checks whether those
fills are implementable by an after-seed or after-fill runner, or whether they
depend on orders resting before the seed itself was accepted.

This is local/research-only and does not authorize remote runs.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_SIM = Path(".tmp_xuan/scorecards/xuan_shadow_review_prepositioned_maker_completion_simulator_20260526T2148Z.json")
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_prepositioned_completion_causality_audit_20260526T2148Z.json"
)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def load_seed_times(root: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(root.glob("*.would_action_decisions.csv")):
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                qid = row.get("quote_intent_id")
                if not qid or row.get("kind") != "candidate":
                    continue
                out.setdefault(qid, {}).update(
                    {
                        "accepted_ts_ms": fnum(row.get("ts_ms")),
                        "side": row.get("side"),
                        "price": fnum(row.get("price")),
                        "qty": fnum(row.get("qty")),
                    }
                )
    for path in sorted(root.glob("*.would_fill_events.csv")):
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                qid = row.get("quote_intent_id")
                if not qid:
                    continue
                out.setdefault(qid, {}).update({"fill_ts_ms": fnum(row.get("ts_ms"))})
    return out


def classify_fill(fill_ts: float, accepted_ts: float | None, fill_seed_ts: float | None) -> str:
    if accepted_ts is None or accepted_ts <= 0:
        return "seed_accept_missing"
    if fill_ts < accepted_ts - 1e-12:
        return "before_seed_accept"
    if fill_seed_ts is None or fill_seed_ts <= 0:
        return "after_seed_accept_fill_missing"
    if fill_ts < fill_seed_ts - 1e-12:
        return "after_seed_accept_before_seed_fill"
    return "after_seed_fill"


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    c = card["causality_summary"]
    lines = [
        "# Xuan Prepositioned Completion Causality Audit",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- causality_audit_ready: `{d['causality_audit_ready']}`",
        f"- after_seed_runner_profile_ready: `{d['after_seed_runner_profile_ready']}`",
        f"- symmetric_prepositioning_feature_required: `{d['symmetric_prepositioning_feature_required']}`",
        f"- future_remote_allowed_by_this_audit: `{d['future_remote_allowed_by_this_audit']}`",
        "",
        "## Summary",
        "",
        f"- filled target orders: `{c['filled_target_orders']}`",
        f"- before seed accept: `{c['before_seed_accept_orders']}`",
        f"- after seed accept before seed fill: `{c['after_seed_accept_before_seed_fill_orders']}`",
        f"- after seed fill: `{c['after_seed_fill_orders']}`",
        f"- non-causal closed residual cost: `{c['before_seed_accept_closed_residual_cost']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Does not authorize remote runs, cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    sim = load_json(args.simulator_scorecard)
    seed_times = load_seed_times(root)
    strict = sim.get("simulation", {}).get("strict_surplus_cap_102", {})
    audited = []
    buckets: dict[str, int] = {}
    qty_by_bucket: dict[str, float] = {}
    cost_by_bucket: dict[str, float] = {}
    for order in strict.get("orders", []):
        qid = str(order.get("quote_intent_id"))
        meta = seed_times.get(qid, {})
        accepted_ts = meta.get("accepted_ts_ms")
        seed_fill_ts = meta.get("fill_ts_ms")
        fill_buckets: dict[str, float] = {}
        for fill in order.get("fills", []):
            bucket = classify_fill(fnum(fill.get("ts_ms")), accepted_ts, seed_fill_ts)
            fill_buckets[bucket] = fill_buckets.get(bucket, 0.0) + fnum(fill.get("filled_qty"))
        dominant_bucket = "unfilled"
        if fill_buckets:
            dominant_bucket = max(fill_buckets.items(), key=lambda item: item[1])[0]
        closed_qty = fnum(order.get("closed_qty"))
        held_px = fnum(order.get("held_px"))
        buckets[dominant_bucket] = buckets.get(dominant_bucket, 0) + (1 if closed_qty > 1e-12 else 0)
        qty_by_bucket[dominant_bucket] = qty_by_bucket.get(dominant_bucket, 0.0) + closed_qty
        cost_by_bucket[dominant_bucket] = cost_by_bucket.get(dominant_bucket, 0.0) + closed_qty * held_px
        audited.append(
            {
                "quote_intent_id": qid,
                "slug": order.get("slug"),
                "held_side": order.get("held_side"),
                "close_side": order.get("close_side"),
                "accepted_ts_ms": accepted_ts,
                "seed_fill_ts_ms": seed_fill_ts,
                "closed_qty": closed_qty,
                "closed_residual_cost": closed_qty * held_px,
                "classification": dominant_bucket,
                "fill_qty_by_classification": fill_buckets,
                "fills": order.get("fills", []),
            }
        )

    filled_orders = [row for row in audited if fnum(row.get("closed_qty")) > 1e-12]
    before_accept_cost = cost_by_bucket.get("before_seed_accept", 0.0)
    after_seed_candidate_ready = before_accept_cost <= 1e-12
    symmetric_required = before_accept_cost > 1e-12
    hard_blockers = []
    if not after_seed_candidate_ready:
        hard_blockers.append("prepositioned_proxy_uses_before_seed_accept_fills")
    if symmetric_required:
        hard_blockers.append("requires_symmetric_or_prior_resting_completion_feature")
    if sim.get("decision", {}).get("hybrid_prepositioned_plus_uncloseable_block_proxy_pass") is not True:
        hard_blockers.append("hybrid_proxy_not_passed")
    card = {
        "artifact": "xuan_shadow_review_prepositioned_completion_causality_audit",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_prepositioned_completion_causality_audit.py",
        "status": "KEEP_SHADOW_REVIEW_PREPOSITIONED_COMPLETION_CAUSALITY_AUDIT_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "simulator_scorecard": str(Path(args.simulator_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "prepositioned_simulator": sim.get("status"),
        },
        "causality_summary": {
            "filled_target_orders": len(filled_orders),
            "before_seed_accept_orders": buckets.get("before_seed_accept", 0),
            "after_seed_accept_before_seed_fill_orders": buckets.get("after_seed_accept_before_seed_fill", 0),
            "after_seed_fill_orders": buckets.get("after_seed_fill", 0),
            "unfilled_orders": len([row for row in audited if fnum(row.get("closed_qty")) <= 1e-12]),
            "before_seed_accept_closed_qty": qty_by_bucket.get("before_seed_accept", 0.0),
            "before_seed_accept_closed_residual_cost": before_accept_cost,
            "after_seed_accept_before_seed_fill_closed_qty": qty_by_bucket.get(
                "after_seed_accept_before_seed_fill", 0.0
            ),
            "after_seed_accept_before_seed_fill_closed_residual_cost": cost_by_bucket.get(
                "after_seed_accept_before_seed_fill", 0.0
            ),
            "after_seed_fill_closed_qty": qty_by_bucket.get("after_seed_fill", 0.0),
            "after_seed_fill_closed_residual_cost": cost_by_bucket.get("after_seed_fill", 0.0),
        },
        "audited_orders": audited,
        "interpretation": {
            "causality_boundary": (
                "the hybrid replay proxy is not implementable by an after-fill completion controller; some required "
                "fills occur before the corresponding seed is accepted"
            ),
            "next_local_target": (
                "design a symmetric/prior-resting completion feature or a causality-safe local simulator before any "
                "new bounded PM_DRY_RUN"
            ),
        },
        "decision": {
            "causality_audit_ready": True,
            "after_seed_runner_profile_ready": after_seed_candidate_ready,
            "symmetric_prepositioning_feature_required": symmetric_required,
            "candidate_profile_ready": False,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_audit": False,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "next_action": "local_symmetric_or_prior_resting_completion_design_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--simulator-scorecard", type=Path, default=DEFAULT_SIM)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=None)
    args = parser.parse_args()
    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
