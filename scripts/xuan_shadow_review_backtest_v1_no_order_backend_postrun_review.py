#!/usr/bin/env python3
"""Review the local BTC no-order backend run result.

The local backend currently validates the runtime binding and writes status/event
logs. It is not a continuous public WS runner. This review records that safety
passed and emits the next colleague request for the real read-only WS/no-order
runner.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any


STAMP = "20260529T0522Z"
ROOT = Path(".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_backtest_v1_same_window_runtime_binding_package_20260529T0520Z")
DEFAULT_STATUS = ROOT / "runtime_logs/shadow_status.json"
DEFAULT_EVENTS = ROOT / "runtime_logs/shadow_events.jsonl"
DEFAULT_START_GATE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_same_window_start_binding_review_20260529T0525Z_runtime_package.json"
)
DEFAULT_RUNTIME_PACKAGE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_same_window_runtime_binding_package_20260529T0520Z.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/"
    f"xuan_shadow_review_backtest_v1_no_order_backend_postrun_review_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_no_order_backend_postrun_review_{STAMP}"
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if isinstance(raw, dict):
            rows.append(raw)
    return rows


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def safety_pass(status: dict[str, Any]) -> bool:
    return (
        status.get("status") == "KEEP_XUAN_SAME_WINDOW_NO_ORDER_SHADOW_BACKEND_VALIDATED_LOCAL_ONLY"
        and status.get("dry_run_only") is True
        and status.get("no_order") is True
        and status.get("orders_sent") is False
        and status.get("cancels_sent") is False
        and status.get("redeems_sent") is False
        and status.get("candidate_import_allowed") is False
        and status.get("live_orders_allowed") is False
        and status.get("private_key_loaded") is False
        and status.get("validation_errors") == []
        and (status.get("policy") or {}).get("continuous_shadow_started") is False
    )


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    status = load_json(args.status)
    events = load_jsonl(args.events)
    start_gate = load_json(args.start_gate)
    runtime_package = load_json(args.runtime_package)
    safety_ok = safety_pass(status)
    runtime_ready = (start_gate.get("decision") or {}).get("runtime_binding_ready") is True
    package_ready = (runtime_package.get("decision") or {}).get("runtime_binding_package_ready") is True
    blockers: list[str] = []
    if not safety_ok:
        blockers.append("local_no_order_backend_safety_failed")
    if not runtime_ready:
        blockers.append("runtime_binding_gate_not_ready")
    if not package_ready:
        blockers.append("runtime_binding_package_not_ready")
    return {
        "artifact": "xuan_shadow_review_backtest_v1_no_order_backend_postrun_review",
        "status": (
            "KEEP_SHADOW_REVIEW_BACKTEST_V1_NO_ORDER_BACKEND_VALIDATED_WS_RUNNER_NEEDED_LOCAL_ONLY"
            if not blockers
            else "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_NO_ORDER_BACKEND_POSTRUN_LOCAL_ONLY"
        ),
        "created_utc": STAMP,
        "reviewed_utc": utc_now(),
        "script": "scripts/xuan_shadow_review_backtest_v1_no_order_backend_postrun_review.py",
        "inputs": {
            "status": str(args.status),
            "events": str(args.events),
            "start_gate": str(args.start_gate),
            "runtime_package": str(args.runtime_package),
        },
        "outputs": {
            "artifact_dir": str(args.artifact_dir),
            "markdown": str(args.artifact_dir / "NO_ORDER_BACKEND_POSTRUN_REVIEW.md"),
            "colleague_request": str(args.artifact_dir / "WS_NO_ORDER_RUNNER_COLLEAGUE_REQUEST.md"),
        },
        "decision": {
            "local_backend_validated": safety_ok,
            "runtime_binding_ready": runtime_ready,
            "runtime_binding_package_ready": package_ready,
            "continuous_ws_runner_executed": False,
            "orders_sent": False,
            "candidate_import_allowed": False,
            "live_orders_allowed": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "private_truth_ready": False,
            "next_lane": "shared_ws_no_order_runner_colleague_build_true_readonly_ws_runner",
        },
        "runtime_status": status,
        "event_count": len(events),
        "events_tail": events[-5:],
        "remaining_blockers": blockers,
        "warnings": [
            "The local backend validates binding and writes status only; it is not a continuous WS shadow runner.",
            "Owner private truth data remains unavailable until future owner execution/reconciliation.",
            "No start/import/live/remote/promotion action is authorized by this post-run review.",
        ],
    }


def render_markdown(card: dict[str, Any]) -> str:
    decision = card.get("decision", {})
    blockers = card.get("remaining_blockers", [])
    lines = [
        "# No-Order Backend Post-Run Review",
        "",
        f"- status: `{card.get('status')}`",
        f"- local_backend_validated: `{decision.get('local_backend_validated')}`",
        f"- runtime_binding_ready: `{decision.get('runtime_binding_ready')}`",
        f"- continuous_ws_runner_executed: `{decision.get('continuous_ws_runner_executed')}`",
        f"- orders_sent: `{decision.get('orders_sent')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## Remaining Blockers",
        "",
    ]
    if blockers:
        lines.extend(f"- `{blocker}`" for blocker in blockers)
    else:
        lines.append("- none for the local backend validation step")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The executable backend validates the BTC <=3pct runtime binding and no-order safety. The next strategy step is a real read-only WS/no-order runner that observes current BTC markets over time and emits the same strict three-file report.",
            "",
        ]
    )
    return "\n".join(lines)


def render_colleague_request(card: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# WS No-Order Runner Colleague Request",
            "",
            "P0 request for `shared_ws_no_order_runner_colleague`: build the real read-only BTC same-window no-order runner on top of the validated runtime binding.",
            "",
            "Required behavior:",
            "",
            "1. Use the runtime config emitted by `xuan_shadow_review_backtest_v1_same_window_runtime_binding_package.py`.",
            "2. Discover current/future BTC 5m markets with `prefix=btc-updown-5m`, `round_offsets=[1,2,3]`, and persist slug/token resolution audit.",
            "3. Subscribe/read public book data only; no private key, no candidate import, no order/cancel/redeem calls.",
            "4. Apply `btc_same_window_residual_share_le_3pct_v1` as the historical research filter contract plus live market selector policy, not as historical market replay.",
            "5. Emit the same three-file report contract: strict 33-column `no_order_shadow_report.csv`, `no_order_shadow_audit_manifest.json`, and `no_order_shadow_gate_summary.json`.",
            "6. Preserve safety counters: order/cancel/redeem/import call counts all 0, `orders_sent=false`, `live_orders_allowed=false`, `candidate_import_allowed=false`, `private_key_loaded=false`.",
            "7. Bind future owner-truth output paths, but keep `owner_private_truth_data_ready=false` until future owner execution/reconciliation.",
            "",
            "P0 request for `backtest_evaluator_colleague`: ensure evaluator can consume that live WS no-order three-file report without changing the 33-column main schema.",
            "",
            "Non-goals:",
            "",
            "- no live orders",
            "- no candidate import",
            "- no deploy",
            "- no remote runner",
            "- no private truth or promotion claim",
            "",
            f"Current xuan review status: `{card.get('status')}`",
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--start-gate", type=Path, default=DEFAULT_START_GATE)
    parser.add_argument("--runtime-package", type=Path, default=DEFAULT_RUNTIME_PACKAGE)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    write_json(args.scorecard, card)
    (args.artifact_dir / "NO_ORDER_BACKEND_POSTRUN_REVIEW.md").write_text(
        render_markdown(card) + "\n", encoding="utf-8"
    )
    (args.artifact_dir / "WS_NO_ORDER_RUNNER_COLLEAGUE_REQUEST.md").write_text(
        render_colleague_request(card) + "\n", encoding="utf-8"
    )
    print(json.dumps({"scorecard": str(args.scorecard), "status": card.get("status")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
