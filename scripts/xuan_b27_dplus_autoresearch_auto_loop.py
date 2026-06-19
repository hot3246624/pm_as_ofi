#!/usr/bin/env python3
"""Local autoresearch loop controller for Xuan/B27 pair-arb backtests.

The loop stores next-run intent into a persisted JSON state file so each
automation wake-up can continue directly from the previous state without manual
prompt re-derivation.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import math
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path("/Users/hot/web3Scientist/poly_backtest_data/verification_store/replay_store_v2/20260502_20260518/store.duckdb")
DEFAULT_REPO_ROOT = REPO_ROOT
DEFAULT_POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
DEFAULT_STATE_PATH = REPO_ROOT / "configs" / "automation" / "xuan_b27_dplus_autoresearch_loop_state.json"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "xuan_research_artifacts" / "xuan_b27_dplus_autoresearch_auto_loop"
AUTOMATION_HINT_PATH = Path.home() / ".codex" / "automations" / "xuan-research-dplus-autoresearch-loop" / "next_action.json"


MODES = ("nagi", "ce25", "balanced")
SEEDS = (20260612, 20260613, 20260614, 20260615)

PROFILES = {
    "warm": {
        "max_runs": 2,
        "generations": 2,
        "keep_top": 2,
        "population": 2,
        "exploration": 1,
        "workers": 1,
        "limit": 120,
        "seed_size": 4,
        "max_wall_clock_seconds": 600,
        "saturation_patience_generations": 1,
        "saturation_score_delta": 1.0,
        "saturation_pnl_delta": 0.005,
    },
    "deep": {
        "max_runs": 2,
        "generations": 3,
        "keep_top": 3,
        "population": 6,
        "exploration": 2,
        "workers": 1,
        "limit": 300,
        "seed_size": 6,
        "max_wall_clock_seconds": 1200,
        "saturation_patience_generations": 1,
        "saturation_score_delta": 0.5,
        "saturation_pnl_delta": 0.002,
    },
    "residual_control": {
        "max_runs": 3,
        "generations": 3,
        "keep_top": 4,
        "population": 6,
        "exploration": 2,
        "workers": 1,
        "limit": 300,
        "seed_size": 6,
        "max_wall_clock_seconds": 1200,
        "saturation_patience_generations": 1,
        "saturation_score_delta": 0.5,
        "saturation_pnl_delta": 0.002,
    },
}

ITERATION_WEIGHTS = {
    "net_pnl": 1000.0,
    "pair_cost": 260.0,
    "participation": 300.0,
    "fill_count": 14.0,
    "residual_rate": 900.0,
    "residual_cost": 1200.0,
    "residual_qty": 80.0,
    "fee": 100.0,
}
MODE_WEIGHT_BOOST = {
    "nagi": {
        "residual_rate": 1.15,
        "residual_cost": 1.25,
        "pair_cost": 0.95,
    },
    "ce25": {
        "pair_cost": 1.20,
        "participation": 1.25,
        "fill_count": 1.30,
    },
    "balanced": {
        "net_pnl": 1.05,
        "pair_cost": 1.10,
    },
}

MIN_POSITIVE_PNL_REQUIRED = 0.0
DEFAULT_DAILY_CANONICAL_WINDOWS = 288.0
RESIDUAL_LOSS_RATE_ALERT = 0.92
LOW_PARTICIPATION_RATE = 0.22
POSITIVE_OBJECTIVE_TARGET = 80.0


DEFAULT_STATE = {
    "version": 1,
    "created_at_utc": None,
    "updated_at_utc": None,
    "step": 0,
    "mode_idx": 0,
    "seed_idx": 0,
    "profile": "warm",
    "history": [],
    "next_action": {
        "mode": MODES[0],
        "seed": SEEDS[0],
        "profile": "warm",
        "reason": "bootstrap",
    },
    "best_by_metric": {},
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_next_action_hint(next_action: dict[str, Any]) -> None:
    hint = {
        "updated_at_utc": utc_now_iso(),
        "next_action": next_action,
        "state_file": str(DEFAULT_STATE_PATH),
        "loop_id": "xuan_b27_dplus_autoresearch_loop",
        "run_contract": "review-only/no-order",
    }
    try:
        write_json(AUTOMATION_HINT_PATH, hint)
    except Exception:
        # keep loop resilient if automation path is unavailable
        return


def sanitize_candidate_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    best = manifest.get("best_candidate") or {}
    summary = best.get("summary", {}) if isinstance(best, dict) else {}
    run_dir = best.get("run_dir") if isinstance(best, dict) else None
    residual_pnl = _to_float(summary.get("residual_pnl"))
    residual_cost = _to_float(summary.get("residual_cost"))
    completion_fee = _to_float(summary.get("completion_fee"))
    paired_pnl = _to_float(summary.get("paired_pnl"))
    pair_cost = _to_float(summary.get("weighted_avg_pair_cost"))
    if pair_cost == 0.0:
        pair_cost = 1.0
    fill_window_rate = _to_float(summary.get("fill_window_rate"))
    fill_count = _to_float(summary.get("fills"))
    residual_loss_rate = _to_float(summary.get("residual_loss_rate"))
    participation = fill_window_rate
    if participation <= 0.0 and fill_count > 0.0:
        participation = min(1.0, fill_count / DEFAULT_DAILY_CANONICAL_WINDOWS)
    total_pnl = _to_float(summary.get("total_pnl"))
    if paired_pnl <= 0.0 and completion_fee > 0.0:
        paired_pnl = max(0.0, total_pnl + max(0.0, residual_pnl) + completion_fee)
    residual_loss = max(0.0, -residual_pnl)
    if residual_loss == 0.0 and residual_cost > 0.0:
        residual_loss = residual_cost
    net_pnl = max(0.0, paired_pnl) - residual_loss - max(0.0, completion_fee)
    score = {
        "mode": manifest.get("research_mode"),
        "total_pnl": total_pnl,
        "objective_net_pnl": net_pnl,
        "paired_pnl": paired_pnl,
        "pair_cost": pair_cost,
        "residual_loss": residual_loss,
        "residual_loss_rate": residual_loss_rate,
        "residual_cost": residual_cost,
        "completion_fee": completion_fee,
        "fill_count": fill_count,
        "fill_window_rate": fill_window_rate,
        "participation_rate": participation,
        "residual_qty": _to_float(summary.get("residual_qty")),
    }
    objective, objective_components = compute_iteration_objective(score)

    return {
        "status": manifest.get("status"),
        "run_count": manifest.get("run_count"),
        "mode": manifest.get("research_mode"),
        "candidate_id": best.get("candidate_id"),
        "run_dir": run_dir,
        "score": best.get("score"),
        "total_pnl": total_pnl,
        "paired_pnl": paired_pnl,
        "fill_count": fill_count,
        "paired_window_rate": _to_float(summary.get("paired_window_rate")),
        "fill_window_rate": fill_window_rate,
        "paired_cost": pair_cost,
        "residual_pnl": residual_pnl,
        "residual_cost": residual_cost,
        "completion_fee": completion_fee,
        "residual_loss_rate": _to_float(summary.get("residual_loss_rate")),
        "residual_qty": _to_float(summary.get("residual_qty")),
        "objective": objective,
        "objective_components": objective_components,
        "objective_net_pnl": net_pnl,
        "participation_rate": participation,
        "manifest_path": manifest.get("artifact"),
    }


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def compute_iteration_objective(metric: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    mode = str(metric.get("mode") or "balanced")
    mode_weights = dict(ITERATION_WEIGHTS)
    multiplier = MODE_WEIGHT_BOOST.get(mode, {})
    for key, mul in multiplier.items():
        if key in mode_weights:
            mode_weights[key] = mode_weights[key] * float(mul)

    net_pnl = _to_float(metric.get("objective_net_pnl"))
    if net_pnl == 0.0 and metric.get("total_pnl") is not None:
        net_pnl = _to_float(metric.get("total_pnl")) - max(0.0, _to_float(metric.get("residual_loss")))

    pair_cost = _to_float(metric.get("pair_cost"))
    if pair_cost == 0.0:
        pair_cost = 1.0
    participation_rate = clamp(_to_float(metric.get("participation_rate")), 0.0, 1.0)
    residual_loss_rate = clamp(_to_float(metric.get("residual_loss_rate")), 0.0, 1.0)
    residual_loss = max(0.0, _to_float(metric.get("residual_loss")))
    residual_qty = _to_float(metric.get("residual_qty"))
    fill_count = max(0.0, _to_float(metric.get("fill_count")))
    fee = max(0.0, _to_float(metric.get("completion_fee")))

    net_pnl_term = net_pnl * mode_weights["net_pnl"]
    pair_cost_term = (1.0 - clamp(pair_cost, 0.0, 2.0)) * mode_weights["pair_cost"]
    participation_term = participation_rate * mode_weights["participation"]
    fill_count_term = math.sqrt(fill_count) * mode_weights["fill_count"]
    residual_rate_term = (1.0 - residual_loss_rate) * mode_weights["residual_rate"]
    residual_cost_term = -residual_loss * mode_weights["residual_cost"]
    residual_qty_term = -math.log1p(max(0.0, residual_qty)) * mode_weights["residual_qty"]
    fee_term = -fee * mode_weights["fee"]

    objective = (
        net_pnl_term
        + pair_cost_term
        + participation_term
        + fill_count_term
        + residual_rate_term
        + residual_cost_term
        + residual_qty_term
        + fee_term
    )
    return objective, {
        "net_pnl_term": net_pnl_term,
        "pair_cost_term": pair_cost_term,
        "participation_term": participation_term,
        "fill_count_term": fill_count_term,
        "residual_rate_term": residual_rate_term,
        "residual_cost_term": residual_cost_term,
        "residual_qty_term": residual_qty_term,
        "fee_term": fee_term,
        "total_objective": objective,
    }


def _to_float(v: Any) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def choose_profile(last_record: dict[str, Any] | None, profile_override: str | None = None) -> str:
    if profile_override:
        return profile_override
    if not last_record:
        return "warm"
    residual_loss_rate = _to_float(last_record.get("residual_loss_rate"))
    net_pnl = _to_float(last_record.get("objective_net_pnl"))
    objective = _to_float(last_record.get("objective"))
    participation_rate = _to_float(last_record.get("participation_rate"))

    if net_pnl < MIN_POSITIVE_PNL_REQUIRED:
        return "residual_control"
    if residual_loss_rate > RESIDUAL_LOSS_RATE_ALERT:
        return "residual_control"
    if objective > POSITIVE_OBJECTIVE_TARGET and participation_rate >= LOW_PARTICIPATION_RATE:
        return "deep"
    if objective > POSITIVE_OBJECTIVE_TARGET * 0.5 and residual_loss_rate <= 0.75:
        return "deep"
    if participation_rate < LOW_PARTICIPATION_RATE:
        return "residual_control"
    return "warm"


def choose_next_mode_and_seed(state: dict[str, Any], last_record: dict[str, Any] | None) -> tuple[str, int, str]:
    modes = MODES
    seeds = SEEDS

    mode_idx = int(state.get("mode_idx", 0))
    seed_idx = int(state.get("seed_idx", 0))
    last_mode = state.get("next_action", {}).get("mode")
    last_seed = state.get("next_action", {}).get("seed")
    next_mode = modes[mode_idx % len(modes)]
    next_seed = seeds[seed_idx % len(seeds)]

    if last_record:
        residual_loss_rate = _to_float(last_record.get("residual_loss_rate"))
        net_pnl = _to_float(last_record.get("objective_net_pnl"))
        objective = _to_float(last_record.get("objective"))
        participation = _to_float(last_record.get("participation_rate"))
        # if residual tail risk is bad, prioritize nagi lane for residual-control focused search
        if net_pnl < MIN_POSITIVE_PNL_REQUIRED and last_mode != "ce25":
            next_mode = "ce25"
            next_profile = "residual_control"
        elif residual_loss_rate > RESIDUAL_LOSS_RATE_ALERT and last_mode != "nagi":
            next_mode = "nagi"
            next_profile = "residual_control"
        elif last_record.get("mode") == "nagi" and residual_loss_rate > RESIDUAL_LOSS_RATE_ALERT:
            next_mode = "nagi"
            next_profile = "residual_control"
        elif objective > POSITIVE_OBJECTIVE_TARGET and participation >= LOW_PARTICIPATION_RATE:
            next_mode = "balanced"
            next_profile = "deep"
        elif participation < LOW_PARTICIPATION_RATE:
            next_mode = "ce25"
            next_profile = "residual_control"
        else:
            next_mode = modes[(mode_idx + 1) % len(modes)]
            if next_mode == "balanced":
                next_profile = "residual_control"
            elif next_mode == "ce25":
                next_profile = "deep"

        # rotate seeds every run, but avoid immediately repeating the same run seed/mode pair
        next_seed = seeds[(seed_idx + 1) % len(seeds)]
        if next_seed == last_seed and next_mode == str(last_mode):
            next_seed = seeds[(seed_idx + 2) % len(seeds)]

    return next_mode, next_seed, choose_profile(last_record, None)


def run_one(
    db: Path,
    output_dir: Path,
    mode: str,
    seed: int,
    profile: str,
    poly_bt_root: Path,
    limit: int,
) -> tuple[int, Path, dict[str, Any]]:
    p = PROFILES[profile]
    run_args = [
        "uv",
        "run",
        "--with",
        "duckdb",
        "python",
        str(REPO_ROOT / "scripts" / "xuan_b27_dplus_pair_arb_autoresearch.py"),
        "--db",
        str(db),
        "--output-dir",
        str(output_dir),
        "--research-mode",
        mode,
        "--max-runs",
        str(p["max_runs"]),
        "--max-wall-clock-seconds",
        str(p["max_wall_clock_seconds"]),
        "--saturation-patience-generations",
        str(p["saturation_patience_generations"]),
        "--saturation-score-delta",
        str(p["saturation_score_delta"]),
        "--saturation-pnl-delta",
        str(p["saturation_pnl_delta"]),
        "--generations",
        str(p["generations"]),
        "--seed-size",
        str(p["seed_size"]),
        "--population",
        str(p["population"]),
        "--keep-top",
        str(p["keep_top"]),
        "--exploration",
        str(p["exploration"]),
        "--workers",
        str(p["workers"]),
        "--limit",
        str(limit),
        "--skip",
        "0",
        "--random-seed",
        str(seed),
        "--initial-balance",
        "10000",
        "--infer-settlement-outcome",
        "--force",
    ]

    env = os.environ.copy()
    env["POLY_BT_ROOT"] = str(poly_bt_root)
    env["PYTHONUNBUFFERED"] = "1"
    log_path = output_dir / "runner.log"

    proc = subprocess.run(
        run_args,
        cwd=REPO_ROOT,
        env=env,
        stdout=open(log_path, "w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        check=False,
        text=True,
    )

    manifest = {}
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        manifest = load_json(manifest_path)

    return proc.returncode, manifest_path, manifest


def summarize_run(output_dir: Path, mode: str, seed: int, seed_size: int, profile: str, rc: int, manifest: dict[str, Any]) -> dict[str, Any]:
    summary = sanitize_candidate_summary(manifest)
    summary.update(
        {
            "step_dir": str(output_dir),
            "command_exit_code": rc,
            "mode": mode,
            "seed": seed,
            "profile": profile,
            "seed_size": seed_size,
            "runner": "xuan_b27_dplus_pair_arb_autoresearch.py",
            "log": str(output_dir / "runner.log"),
        }
    )
    if manifest:
        summary["manifest_path"] = str(output_dir / "manifest.json")
    return summary


def update_best_by_metric(state: dict[str, Any], record: dict[str, Any]) -> None:
    if not record:
        return
    if record.get("status") != "COMPLETED":
        return

    by_metric = state.setdefault("best_by_metric", {})
    mode = str(record.get("mode", "unknown"))
    pnl = _to_float(record.get("objective_net_pnl"))
    residual = _to_float(record.get("residual_loss_rate"))
    score = _to_float(record.get("objective"))
    objective_components = record.get("objective_components") or {}
    participation = _to_float(record.get("participation_rate"))
    pair_cost = _to_float(record.get("paired_cost"))
    if pair_cost == 0.0:
        pair_cost = 1.0

    current = by_metric.get(mode, {})
    if not current or score > _to_float(current.get("objective")):
        by_metric[mode] = {
            "total_pnl": pnl,
            "residual_loss_rate": residual,
            "score": score,
            "objective": score,
            "participation_rate": participation,
            "pair_cost": pair_cost,
            "candidate_id": record.get("candidate_id"),
            "step_dir": record.get("step_dir"),
            "seed": record.get("seed"),
            "profile": record.get("profile"),
            "updated_at_utc": utc_now_iso(),
            "objective_components": objective_components,
        }
    state["best_by_metric"] = by_metric


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Replay DB path (sqlite/duckdb)")
    parser.add_argument("--poly-bt-root", default=str(DEFAULT_POLY_BT_ROOT))
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--manual-mode", default=None, choices=list(MODES))
    parser.add_argument("--manual-seed", type=int, default=None)
    parser.add_argument("--manual-profile", default=None, choices=list(PROFILES.keys()))
    parser.add_argument("--manual-limit", type=int, default=None)
    parser.add_argument("--cycles", type=int, default=1, help="how many runs to execute this wake-up")
    args = parser.parse_args()

    db = Path(args.db)
    if not db.exists():
        raise SystemExit(f"db not found: {db}")

    state_path = Path(args.state_path)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    poly_bt_root = Path(args.poly_bt_root)

    state = DEFAULT_STATE.copy()
    existing = load_json(state_path)
    if existing:
        state.update(existing)

    now_iso = utc_now_iso()
    if not state.get("created_at_utc"):
        state["created_at_utc"] = now_iso

    history = deque(state.get("history") or [], maxlen=80)
    last_record = history[-1] if history else None

    next_mode, next_seed, next_profile = choose_next_mode_and_seed(state, last_record)

    if args.manual_mode:
        next_mode = args.manual_mode
    if args.manual_seed:
        next_seed = args.manual_seed
    if args.manual_profile:
        next_profile = args.manual_profile

    profile = PROFILES[next_profile]
    limit = args.manual_limit or profile["limit"]
    next_reason = "resume from state"
    if args.manual_mode or args.manual_seed or args.manual_profile:
        next_reason = "manual override"
    print(
        f"[AUTO_LOOP] bootstrap plan: step={state.get('step', 0)} mode={next_mode} "
        f"seed={next_seed} profile={next_profile} reason={next_reason}"
    )

    cycle_count = int(args.cycles)
    run_records = []

    for cycle in range(cycle_count):
        step_now = int(state.get("step", 0))
        stamp = utc_now_iso()
        step_dir = output_root / f"{stamp}_step_{step_now:04d}_{next_mode}_s{next_seed}_{next_profile}"
        step_dir.mkdir(parents=True, exist_ok=True)

        code, manifest_path, manifest = run_one(
            db=db,
            output_dir=step_dir,
            mode=next_mode,
            seed=next_seed,
            profile=next_profile,
            poly_bt_root=poly_bt_root,
            limit=limit,
        )
        record = summarize_run(step_dir, next_mode, next_seed, profile["seed_size"], next_profile, code, manifest)
        # Clean up compat_db to preserve disk space
        compat_db_dir = step_dir / "compat_db"
        if compat_db_dir.exists():
            try:
                shutil.rmtree(compat_db_dir)
            except Exception:
                pass
        run_records.append(record)
        update_best_by_metric(state, record)
        history.append(record)
        state["step"] = step_now + 1
        state["last_exit_code"] = code
        state["last_manifest"] = str(manifest_path) if manifest_path else None
        state["last_profile"] = next_profile
        state["last_mode"] = next_mode
        state["last_seed"] = next_seed

        # deterministic one-step scheduler for next wake-up
        state["mode_idx"] = (state.get("mode_idx", 0) + 1) % len(MODES)
        state["seed_idx"] = (state.get("seed_idx", 0) + 1) % len(SEEDS)
        next_mode, next_seed, next_profile = choose_next_mode_and_seed(
            state,
            record,
        )
        state["next_action"] = {
            "mode": next_mode,
            "seed": next_seed,
            "profile": next_profile,
            "reason": (
                "residual-control lane activated"
                if _to_float(record.get("residual_loss_rate")) > 0.95
                else f"lane rotate profile={next_profile}"
            ),
            "when_utc": utc_now_iso(),
        }
        print(
            "[AUTO_LOOP] completed "
            f"mode={record.get('mode')} seed={record.get('seed')} profile={record.get('profile')} "
            f"status={record.get('status')} total_pnl={_to_float(record.get('total_pnl'))} "
            f"residual_loss_rate={_to_float(record.get('residual_loss_rate'))} "
            f"net_pnl={_to_float(record.get('objective_net_pnl'))} "
            f"objective={_to_float(record.get('objective'))} "
            f"fill_count={_to_float(record.get('fill_count'))}"
        )
        print(f"[AUTO_LOOP] next_action={json.dumps(state['next_action'], sort_keys=True)}")
        write_next_action_hint(state["next_action"])
        if cycle != cycle_count - 1:
            # keep the loop moving without re-reading itself
            next_mode = state["next_action"]["mode"]
            next_seed = state["next_action"]["seed"]
            next_profile = state["next_action"]["profile"]

    state["updated_at_utc"] = utc_now_iso()
    state["history"] = list(history)

    state_manifest = {
        "updated_at_utc": state["updated_at_utc"],
        "run_records": run_records,
        "state": {
            "next_action": state["next_action"],
            "best_by_mode": state.get("best_by_metric", {}),
            "mode_idx": state["mode_idx"],
            "seed_idx": state["seed_idx"],
            "step": state["step"],
        },
        "output_root": str(output_root),
        "state_path": str(state_path),
        "db": str(db),
        "poly_bt_root": str(poly_bt_root),
    }
    write_json(output_root / f"auto_loop_summary_{utc_now_iso()}.json", state_manifest)
    write_json(state_path, state)


if __name__ == "__main__":
    main()
