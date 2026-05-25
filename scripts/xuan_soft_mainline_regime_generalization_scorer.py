#!/usr/bin/env python3
"""Score soft-mainline regime selectivity against overfit risk.

This local-only scorer keeps both good and weak no-cancel windows in the
evidence ledger. The goal is not to prove the strategy trades every regime; it
is to prove the controller can trade when rescue density is present and abstain
when the same lane has traffic but poor rescue density.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    num = fnum(value)
    return int(num) if num is not None else default


def safe_rate(num: float, den: float) -> float | None:
    return num / den if den > 0 else None


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def status_is_keep(card: dict[str, Any]) -> bool:
    return str(card.get("status") or "").startswith("KEEP")


def metric_dict(runtime: dict[str, Any]) -> dict[str, Any]:
    metrics = runtime.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def event_counts(event: dict[str, Any]) -> dict[str, Any]:
    counts = event.get("event_counts")
    return counts if isinstance(counts, dict) else {}


def prefix_decision(prefix: dict[str, Any]) -> dict[str, Any]:
    decision = prefix.get("decision")
    return decision if isinstance(decision, dict) else {}


def parse_window(raw: list[str]) -> dict[str, str]:
    if len(raw) != 4:
        raise SystemExit("--window requires LABEL RUNTIME_SUMMARY EVENT_DIAGNOSTICS PREFIX_SCORECARD")
    label, runtime, event, prefix = raw
    return {
        "label": label,
        "runtime_summary": runtime,
        "event_diagnostics": event,
        "prefix_scorecard": prefix,
    }


def classify(obs: dict[str, Any], args: argparse.Namespace) -> tuple[str, list[str], bool]:
    tags: list[str] = []
    candidates = inum(obs.get("candidate_rows"))
    touches = inum(obs.get("touch_rows"))
    fills = inum(obs.get("queue_supported_fills"))
    rescues = inum(obs.get("strict_rescue_closes"))
    fill_rate = fnum(obs.get("fill_rate"), 0.0) or 0.0
    rescue_per_candidate = fnum(obs.get("rescue_per_candidate"), 0.0) or 0.0
    rescue_per_fill = fnum(obs.get("rescue_per_fill"), 0.0) or 0.0
    pair_pnl = fnum(obs.get("pair_pnl"), 0.0) or 0.0
    residual_qty_share = fnum(obs.get("residual_qty_share"), 0.0) or 0.0
    residual_cost_share = fnum(obs.get("residual_cost_share"), 0.0) or 0.0
    source_blocks = inum(obs.get("source_blocks"))

    active = candidates >= args.min_candidates and touches >= args.min_touches
    if source_blocks > args.max_source_blocks:
        tags.append("source_not_clean")
    if not active:
        tags.append("traffic_starved")
    if fills < args.min_fills or fill_rate < args.min_fill_rate:
        tags.append("fill_density_weak")
    if (
        rescues < args.min_rescue_closes
        or rescue_per_candidate < args.min_rescue_per_candidate
        or rescue_per_fill < args.min_rescue_per_fill
    ):
        tags.append("rescue_density_weak")
    if pair_pnl < args.min_pair_pnl:
        tags.append("pnl_negative")
    if residual_qty_share > args.max_residual_qty_share or residual_cost_share > args.max_residual_cost_share:
        tags.append("residual_risk_high")

    qualified = active and not tags
    if qualified:
        return "density_qualified_tradeable", ["active_two_sided_rescue_density_present"], True
    if active and "rescue_density_weak" in tags:
        return "active_but_rescue_density_weak_abstain", tags, False
    if "traffic_starved" in tags:
        return "traffic_starved_abstain", tags, False
    return "blocked_abstain", tags, False


def summarize_window(raw: list[str], args: argparse.Namespace) -> dict[str, Any]:
    spec = parse_window(raw)
    runtime = load_json(spec["runtime_summary"])
    event = load_json(spec["event_diagnostics"])
    prefix = load_json(spec["prefix_scorecard"])
    metrics = metric_dict(runtime)
    counts = event_counts(event)
    candidates = inum(counts.get("candidate"), inum(metrics.get("accepted_actions")))
    fills = inum(counts.get("queue_supported_fill"), inum(metrics.get("queue_supported_fills")))
    rescues = inum(counts.get("fak_salvage"), inum(metrics.get("strict_rescue_closes")))
    accepted = inum(metrics.get("accepted_actions"), candidates)
    obs = {
        "candidate_rows": candidates,
        "touch_rows": inum(counts.get("touch")),
        "accepted_actions": accepted,
        "queue_supported_fills": fills,
        "strict_rescue_closes": rescues,
        "fill_rate": safe_rate(fills, candidates),
        "rescue_per_candidate": safe_rate(rescues, candidates),
        "rescue_per_fill": safe_rate(rescues, fills),
        "pair_pnl": fnum(metrics.get("pair_pnl"), 0.0) or 0.0,
        "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
        "residual_qty_share": fnum(metrics.get("residual_qty_share"), 0.0) or 0.0,
        "residual_cost_share": fnum(metrics.get("residual_cost_share"), 0.0) or 0.0,
        "rescue_net_pair_cost_max": fnum(metrics.get("rescue_net_pair_cost_max")),
        "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max")),
        "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
        "source_blocks": inum(metrics.get("strict_rescue_source_blocks")),
    }
    regime, tags, tradeable = classify(obs, args)
    decision = prefix_decision(prefix)
    return {
        "label": spec["label"],
        "inputs": spec,
        "runtime_status": runtime.get("status"),
        "prefix_status": prefix.get("status"),
        "prefix_pass": status_is_keep(prefix),
        "earliest_prefix_pass_minutes": decision.get("earliest_pass_minutes"),
        "observed": obs,
        "classification": {
            "regime": regime,
            "tags": tags,
            "tradeable_under_policy": tradeable,
        },
    }


def build_markdown(card: dict[str, Any]) -> str:
    lines = [
        "# Soft-Mainline Regime Generalization",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Principle",
        "",
        "The controller should be universal in risk discipline, not universal in trade frequency.",
        "A robust system must trade high-quality rescue-density windows and abstain from active but low-rescue-density windows.",
        "",
        "## Windows",
        "",
    ]
    for window in card["windows"]:
        obs = window["observed"]
        cls = window["classification"]
        lines.extend(
            [
                f"- `{window['label']}`: `{cls['regime']}`",
                f"  - candidates `{obs['candidate_rows']}`, fills `{obs['queue_supported_fills']}`, rescues `{obs['strict_rescue_closes']}`",
                f"  - rescue/candidate `{obs['rescue_per_candidate']}`, rescue/fill `{obs['rescue_per_fill']}`, residual cost share `{obs['residual_cost_share']}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Holdout Rule",
            "",
            "- A single good window cannot promote the strategy.",
            "- Weak windows remain in the ledger as abstention tests.",
            "- Shadow review needs repeated qualified windows plus clean abstention behavior in weak regimes.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    windows = [summarize_window(raw, args) for raw in args.window]
    if not windows:
        raise SystemExit("provide at least one --window")
    qualified = [w for w in windows if w["classification"]["tradeable_under_policy"]]
    active_abstain = [
        w
        for w in windows
        if w["classification"]["regime"] == "active_but_rescue_density_weak_abstain"
    ]
    traffic_abstain = [w for w in windows if w["classification"]["regime"] == "traffic_starved_abstain"]

    hard_blockers: list[str] = []
    if len(qualified) < args.min_qualified_windows_for_shadow:
        hard_blockers.append("insufficient_qualified_windows_for_shadow")
    if len(active_abstain) < args.min_active_abstain_windows_for_policy:
        hard_blockers.append("missing_active_weak_abstention_window")

    if qualified and active_abstain:
        status = "KEEP_SOFT_MAINLINE_REGIME_GENERALIZATION_ABSTENTION_POLICY_LOCAL_ONLY"
        next_action = "keep_regime_ledger_and_wait_for_fresh_density_signal"
    elif qualified:
        status = "UNKNOWN_SOFT_MAINLINE_REGIME_GENERALIZATION_NEEDS_ABSTENTION_HOLDOUT_LOCAL_ONLY"
        next_action = "collect_or_score_weak_regime_abstention_evidence_before_shadow"
    else:
        status = "BLOCKED_SOFT_MAINLINE_REGIME_GENERALIZATION_NO_TRADEABLE_REGIME_LOCAL_ONLY"
        next_action = "stay_local_until_tradeable_density_regime_reappears"

    return {
        "artifact": "xuan_soft_mainline_regime_generalization_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_regime_generalization_scorer.py",
        "status": status,
        "windows": windows,
        "summary": {
            "window_count": len(windows),
            "qualified_tradeable_count": len(qualified),
            "active_abstain_count": len(active_abstain),
            "traffic_abstain_count": len(traffic_abstain),
            "regimes_seen": sorted({w["classification"]["regime"] for w in windows}),
            "overfit_risk_control": "single_good_window_cannot_promote; weak_windows_are_kept_as_abstention_holdouts",
        },
        "thresholds": {
            "min_candidates": args.min_candidates,
            "min_touches": args.min_touches,
            "min_fills": args.min_fills,
            "min_rescue_closes": args.min_rescue_closes,
            "min_fill_rate": args.min_fill_rate,
            "min_rescue_per_candidate": args.min_rescue_per_candidate,
            "min_rescue_per_fill": args.min_rescue_per_fill,
            "min_pair_pnl": args.min_pair_pnl,
            "max_residual_qty_share": args.max_residual_qty_share,
            "max_residual_cost_share": args.max_residual_cost_share,
            "max_source_blocks": args.max_source_blocks,
        },
        "decision": {
            "next_action": next_action,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers_for_shadow": hard_blockers,
        },
        "guardrails": [
            "This scorer is local planning evidence only.",
            "It does not launch remote jobs, approve shadow, or approve deployment.",
            "Do not discard weak windows; they are abstention holdouts.",
            "Do not optimize the controller to trade every regime.",
            "Do not run longer than 1800s to compensate for weak rescue density.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--window",
        nargs=4,
        action="append",
        metavar=("LABEL", "RUNTIME_SUMMARY", "EVENT_DIAGNOSTICS", "PREFIX_SCORECARD"),
        required=True,
    )
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-output")
    parser.add_argument("--min-candidates", type=int, default=20)
    parser.add_argument("--min-touches", type=int, default=20)
    parser.add_argument("--min-fills", type=int, default=18)
    parser.add_argument("--min-rescue-closes", type=int, default=7)
    parser.add_argument("--min-fill-rate", type=float, default=0.70)
    parser.add_argument("--min-rescue-per-candidate", type=float, default=0.25)
    parser.add_argument("--min-rescue-per-fill", type=float, default=0.30)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-source-blocks", type=int, default=0)
    parser.add_argument("--min-qualified-windows-for-shadow", type=int, default=2)
    parser.add_argument("--min-active-abstain-windows-for-policy", type=int, default=1)
    args = parser.parse_args()

    card = rounded(build(args))
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card))
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
