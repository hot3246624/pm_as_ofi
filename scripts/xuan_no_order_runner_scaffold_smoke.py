#!/usr/bin/env python3
"""Local no-network smoke for xuan no-order runner scaffolding.

This does not connect to shared ingress and does not start a runner process. It
imports the runner class directly, feeds synthetic ticks, and verifies that the
new default-off budget/rescue controls are inert unless explicitly enabled.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.xuan_dplus_passive_passive_shadow_runner import (
    DPlusRunner,
    FairPriceAdmissionGate,
    Lot,
    RunnerConfig,
)


def read_events(out_dir: Path, slug: str) -> list[dict[str, Any]]:
    path = out_dir / f"{slug}.events.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def feed_pairable_sequence(runner: DPlusRunner) -> None:
    runner.on_book(
        {
            "ts_ms": 101_000,
            "yes_ask": 0.50,
            "no_ask": 0.50,
            "source_sequence_id": "book-1",
            "event_time_ms": 101_000,
        }
    )
    runner.on_trade(
        {
            "ts_ms": 101_000,
            "market_side": "YES",
            "taker_side": "SELL",
            "price": 0.50,
            "size": 20.0,
            "source_sequence_id": "trade-yes-seed",
            "event_time_ms": 101_000,
        }
    )
    runner.on_trade(
        {
            "ts_ms": 102_000,
            "market_side": "YES",
            "taker_side": "SELL",
            "price": 0.45,
            "size": 10.0,
            "source_sequence_id": "trade-yes-fill",
            "event_time_ms": 102_000,
        }
    )
    runner.on_trade(
        {
            "ts_ms": 107_000,
            "market_side": "NO",
            "taker_side": "SELL",
            "price": 0.50,
            "size": 20.0,
            "source_sequence_id": "trade-no-seed",
            "event_time_ms": 107_000,
        }
    )
    runner.on_trade(
        {
            "ts_ms": 108_000,
            "market_side": "NO",
            "taker_side": "SELL",
            "price": 0.45,
            "size": 10.0,
            "source_sequence_id": "trade-no-fill",
            "event_time_ms": 108_000,
        }
    )


def comparable_state(runner: DPlusRunner) -> dict[str, Any]:
    metrics = asdict(runner.metrics)
    for key in ("pair_costs", "net_pair_costs", "fill_wait_ms", "pair_wait_ms", "salvage_wait_ms"):
        metrics[key] = list(metrics[key])
    return {
        "blocked": dict(sorted(runner.blocked.items())),
        "metrics": metrics,
        "surplus_bank": round(runner.surplus_bank, 12),
    }


def run_pairable(cfg: RunnerConfig) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        runner = DPlusRunner(slug, Path(td), cfg)
        feed_pairable_sequence(runner)
        return {
            "state": comparable_state(runner),
            "events": read_events(Path(td), slug),
        }


def smoke_default_off_parity() -> dict[str, Any]:
    default = run_pairable(RunnerConfig())
    explicit_disabled = run_pairable(RunnerConfig(surplus_budget_mode="none", strict_rescue_mode="none"))
    passed = default["state"] == explicit_disabled["state"]
    return {
        "name": "default_off_parity",
        "status": "PASS" if passed else "FAIL",
        "default_state": default["state"],
        "explicit_disabled_state": explicit_disabled["state"],
    }


def smoke_surplus_budget_paths() -> dict[str, Any]:
    block = run_pairable(
        RunnerConfig(surplus_budget_mode="block", surplus_budget_max_abs_unpaired_cost=0.10)
    )
    block_state = block["state"]
    block_passed = (
        block_state["metrics"]["candidates"] == 0
        and block_state["metrics"]["surplus_budget_blocks"] >= 1
        and block_state["blocked"].get("surplus_budget", 0) >= 1
    )

    cap = run_pairable(
        RunnerConfig(surplus_budget_mode="cap", surplus_budget_max_abs_unpaired_cost=0.50)
    )
    cap_state = cap["state"]
    candidate_events = [event for event in cap["events"] if event.get("kind") == "candidate"]
    cap_event_has_audit = bool(candidate_events) and all(
        event.get("surplus_budget_mode") == "cap"
        and event.get("surplus_budget_decision") in {"cap", "allow"}
        and "surplus_budget_projected_unpaired_cost" in event
        for event in candidate_events
    )
    cap_passed = (
        cap_state["metrics"]["candidates"] >= 1
        and cap_state["metrics"]["surplus_budget_capped_qty"] > 0
        and cap_event_has_audit
    )
    return {
        "name": "surplus_budget_block_and_cap",
        "status": "PASS" if block_passed and cap_passed else "FAIL",
        "block_state": block_state,
        "cap_state": cap_state,
        "cap_candidate_events": candidate_events,
    }


def smoke_strict_rescue_event_fields() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        runner = DPlusRunner(
            slug,
            Path(td),
            RunnerConfig(
                salvage_net_cap=1.10,
                salvage_age_ms=30_000,
                strict_rescue_mode="source_audit",
                strict_rescue_l1_age_max_ms=50,
                strict_rescue_close_size_haircut=0.50,
                strict_rescue_close_ask_slip=0.01,
                strict_rescue_require_book_source=True,
            ),
        )
        runner.lots["YES"].append(
            Lot(
                id=1,
                quote_intent_id="quote-source-1",
                side="YES",
                qty=4.0,
                px=0.40,
                fill_ms=1_000,
                source_order_id=1,
                source_sequence_id="fill-seq-1",
                source_event_time_ms=1_000,
            )
        )
        runner.on_book(
            {
                "ts_ms": 31_000,
                "yes_ask": 0.40,
                "no_ask": 0.50,
                "source_sequence_id": "book-seq-1",
                "event_time_ms": 30_990,
            }
        )
        events = read_events(Path(td), slug)
        salvage_events = [event for event in events if event.get("kind") == "fak_salvage"]
        event = salvage_events[-1] if salvage_events else {}
        passed = (
            runner.metrics.strict_rescue_actions == 1
            and abs(runner.metrics.strict_rescue_qty - 2.0) < 1e-9
            and event.get("strict_rescue_mode") == "source_audit"
            and event.get("source_sequence_id") == "book-seq-1"
            and event.get("source_lot_sequence_id") == "fill-seq-1"
            and event.get("strict_rescue_close_size_haircut") == 0.5
            and event.get("strict_rescue_close_ask_slip") == 0.01
        )
        return {
            "name": "strict_rescue_source_audit_event_fields",
            "status": "PASS" if passed else "FAIL",
            "state": comparable_state(runner),
            "salvage_event": event,
        }


def smoke_strict_rescue_skip_low_cost_lots() -> dict[str, Any]:
    def run_case(skip_low_cost: bool) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as td:
            slug = "btc-updown-5m-101"
            runner = DPlusRunner(
                slug,
                Path(td),
                RunnerConfig(
                    salvage_net_cap=1.10,
                    salvage_age_ms=30_000,
                    salvage_min_lot_cost=0.25,
                    strict_rescue_skip_low_cost_lots=skip_low_cost,
                    strict_rescue_mode="source_audit",
                    strict_rescue_l1_age_max_ms=50,
                    strict_rescue_require_book_source=True,
                    write_rescue_block_diagnostics=True,
                ),
            )
            runner.lots["YES"].extend(
                [
                    Lot(
                        id=1,
                        quote_intent_id="quote-dust-head",
                        side="YES",
                        qty=1.0,
                        px=0.05,
                        fill_ms=1_000,
                        source_order_id=1,
                        source_sequence_id="fill-dust-head",
                        source_event_time_ms=1_000,
                    ),
                    Lot(
                        id=2,
                        quote_intent_id="quote-rescuable-tail",
                        side="YES",
                        qty=4.0,
                        px=0.40,
                        fill_ms=1_000,
                        source_order_id=2,
                        source_sequence_id="fill-rescuable-tail",
                        source_event_time_ms=1_000,
                    ),
                ]
            )
            runner.on_book(
                {
                    "ts_ms": 40_000,
                    "yes_ask": 0.40,
                    "no_ask": 0.50,
                    "source_sequence_id": "book-rescue",
                    "event_time_ms": 39_990,
                }
            )
            events = read_events(Path(td), slug)
            salvage_events = [event for event in events if event.get("kind") == "fak_salvage"]
            block_events = [event for event in events if event.get("kind") == "strict_rescue_block"]
            return {
                "state": comparable_state(runner),
                "salvage_events": salvage_events,
                "block_events": block_events,
                "remaining_lots": [asdict(lot) for lot in runner.lots["YES"]],
            }

    disabled = run_case(False)
    enabled = run_case(True)
    disabled_passed = (
        disabled["state"]["metrics"]["strict_rescue_actions"] == 0
        and disabled["block_events"]
        and disabled["block_events"][-1].get("block_component") == "lot_min_cost"
    )
    enabled_event = enabled["salvage_events"][-1] if enabled["salvage_events"] else {}
    enabled_passed = (
        enabled["state"]["metrics"]["strict_rescue_actions"] == 1
        and enabled_event.get("source_lot_id") == 2
        and enabled_event.get("strict_rescue_skipped_low_cost_lots") == 1
        and len(enabled["remaining_lots"]) == 1
        and enabled["remaining_lots"][0]["id"] == 1
    )
    passed = disabled_passed and enabled_passed
    return {
        "name": "strict_rescue_skip_low_cost_lots",
        "status": "PASS" if passed else "FAIL",
        "disabled": disabled,
        "enabled": enabled,
    }


def smoke_source_quality_and_l2_paths() -> dict[str, Any]:
    cfg = RunnerConfig(
        source_quality_require_trade_source=True,
        source_quality_require_l1_source=True,
        source_quality_l1_age_max_ms=1000,
        source_quality_require_l2_source=True,
    )
    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        runner = DPlusRunner(slug, Path(td), cfg)
        runner.on_book(
            {
                "ts_ms": 101_000,
                "yes_ask": 0.50,
                "no_ask": 0.50,
                "source_sequence_id": "book-l1-seq",
                "event_time_ms": 101_000,
                "l2_source_sequence_id": "book-l2-seq",
                "l2_event_time_ms": 100_999,
            }
        )
        runner.on_trade(
            {
                "ts_ms": 101_000,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.50,
                "size": 20.0,
                "source_sequence_id": "trade-seq",
                "event_time_ms": 101_000,
            }
        )
        events = read_events(Path(td), slug)
        candidate = next((event for event in events if event.get("kind") == "candidate"), {})
        source_quality_allow_passed = (
            candidate.get("source_quality_decision") == "allow"
            and candidate.get("source_quality_trade_source_sequence_id") == "trade-seq"
            and candidate.get("source_quality_l1_source_sequence_id") == "book-l1-seq"
            and candidate.get("source_quality_l2_source_sequence_id") == "book-l2-seq"
            and candidate.get("source_quality_l1_age_ms") == 0
        )

    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        runner = DPlusRunner(slug, Path(td), cfg)
        runner.on_book(
            {
                "ts_ms": 101_000,
                "yes_ask": 0.50,
                "no_ask": 0.50,
                "source_sequence_id": "book-l1-seq",
                "event_time_ms": 101_000,
            }
        )
        runner.on_trade(
            {
                "ts_ms": 101_000,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.50,
                "size": 20.0,
                "source_sequence_id": "trade-seq",
                "event_time_ms": 101_000,
            }
        )
        events = read_events(Path(td), slug)
        blocks = [event for event in events if event.get("kind") == "source_quality_block"]
        missing_l2_passed = (
            runner.metrics.candidates == 0
            and runner.blocked.get("source_quality_missing_l2_source") == 1
            and blocks
            and blocks[-1].get("source_quality_block_reason") == "source_quality_missing_l2_source"
        )

    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        runner = DPlusRunner(
            slug,
            Path(td),
            RunnerConfig(
                salvage_net_cap=1.10,
                salvage_age_ms=30_000,
                strict_rescue_mode="source_audit",
                strict_rescue_l1_age_max_ms=50,
                strict_rescue_close_size_haircut=0.50,
                strict_rescue_require_book_source=True,
                strict_rescue_require_l2_source=True,
            ),
        )
        runner.lots["YES"].append(
            Lot(
                id=1,
                quote_intent_id="quote-source-1",
                side="YES",
                qty=4.0,
                px=0.40,
                fill_ms=1_000,
                source_order_id=1,
                source_sequence_id="fill-seq-1",
                source_event_time_ms=1_000,
            )
        )
        runner.on_book(
            {
                "ts_ms": 31_000,
                "yes_ask": 0.40,
                "no_ask": 0.50,
                "source_sequence_id": "book-l1-rescue",
                "event_time_ms": 30_990,
                "l2_source_sequence_id": "book-l2-rescue",
                "l2_event_time_ms": 30_990,
            }
        )
        events = read_events(Path(td), slug)
        salvage_events = [event for event in events if event.get("kind") == "fak_salvage"]
        l2_rescue_passed = (
            runner.metrics.strict_rescue_actions == 1
            and salvage_events
            and salvage_events[-1].get("strict_rescue_l2_source_sequence_id") == "book-l2-rescue"
        )

    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        runner = DPlusRunner(
            slug,
            Path(td),
            RunnerConfig(
                salvage_net_cap=1.10,
                salvage_age_ms=30_000,
                strict_rescue_mode="source_audit",
                strict_rescue_require_book_source=True,
                strict_rescue_require_l2_source=True,
            ),
        )
        runner.lots["YES"].append(
            Lot(
                id=1,
                quote_intent_id="quote-source-1",
                side="YES",
                qty=4.0,
                px=0.40,
                fill_ms=1_000,
                source_order_id=1,
            )
        )
        runner.on_book(
            {
                "ts_ms": 31_000,
                "yes_ask": 0.40,
                "no_ask": 0.50,
                "source_sequence_id": "book-l1-rescue",
                "event_time_ms": 30_990,
            }
        )
        missing_l2_rescue_passed = (
            runner.metrics.strict_rescue_actions == 0
            and runner.blocked.get("strict_rescue_missing_l2_source") == 1
        )

    passed = all(
        [
            source_quality_allow_passed,
            missing_l2_passed,
            l2_rescue_passed,
            missing_l2_rescue_passed,
        ]
    )
    return {
        "name": "source_quality_and_l2_paths",
        "status": "PASS" if passed else "FAIL",
        "source_quality_allow_passed": source_quality_allow_passed,
        "missing_l2_passed": missing_l2_passed,
        "l2_rescue_passed": l2_rescue_passed,
        "missing_l2_rescue_passed": missing_l2_rescue_passed,
    }


def smoke_risk_seed_closeability_gate() -> dict[str, Any]:
    def run_one(cfg: RunnerConfig) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as td:
            slug = "btc-updown-5m-100"
            runner = DPlusRunner(slug, Path(td), cfg)
            runner.on_book(
                {
                    "ts_ms": 101_000,
                    "yes_ask": 0.50,
                    "no_ask": 0.50,
                    "source_sequence_id": "book-closeability-l1",
                    "event_time_ms": 101_000,
                    "l2_source_sequence_id": "book-closeability-l2",
                    "l2_event_time_ms": 101_000,
                }
            )
            runner.on_trade(
                {
                    "ts_ms": 101_000,
                    "market_side": "YES",
                    "taker_side": "SELL",
                    "price": 0.50,
                    "size": 20.0,
                    "source_sequence_id": "trade-closeability-seed",
                    "event_time_ms": 101_000,
                }
            )
            return {
                "state": comparable_state(runner),
                "events": read_events(Path(td), slug),
            }

    default = run_one(RunnerConfig(cooldown_ms=0))
    default_candidate = next(
        (event for event in default["events"] if event.get("kind") == "candidate"),
        {},
    )
    default_passed = (
        default["state"]["metrics"]["candidates"] == 1
        and default_candidate.get("risk_seed_closeability_net_cap") is None
        and default_candidate.get("closeability_net_pair_cost") is not None
    )

    blocked = run_one(RunnerConfig(cooldown_ms=0, risk_seed_closeability_net_cap=0.95))
    block_event = next(
        (event for event in blocked["events"] if event.get("kind") == "risk_seed_closeability_block"),
        {},
    )
    block_passed = (
        blocked["state"]["metrics"]["candidates"] == 0
        and blocked["state"]["blocked"].get("risk_seed_closeability_net_cap") == 1
        and block_event.get("risk_seed_closeability_net_cap") == 0.95
        and (block_event.get("closeability_net_pair_cost") or 0.0) > 0.95
        and block_event.get("source_sequence_id") == "trade-closeability-seed"
    )

    allowed = run_one(RunnerConfig(cooldown_ms=0, risk_seed_closeability_net_cap=0.99))
    allowed_candidate = next(
        (event for event in allowed["events"] if event.get("kind") == "candidate"),
        {},
    )
    allow_passed = (
        allowed["state"]["metrics"]["candidates"] == 1
        and allowed["state"]["blocked"].get("risk_seed_closeability_net_cap", 0) == 0
        and allowed_candidate.get("risk_seed_closeability_net_cap") == 0.99
        and (allowed_candidate.get("closeability_net_pair_cost") or 2.0) <= 0.99
    )

    passed = default_passed and block_passed and allow_passed
    return {
        "name": "risk_seed_closeability_gate",
        "status": "PASS" if passed else "FAIL",
        "default_passed": default_passed,
        "block_passed": block_passed,
        "allow_passed": allow_passed,
        "default_candidate": default_candidate,
        "block_event": block_event,
        "allowed_candidate": allowed_candidate,
    }


def smoke_risk_seed_closeability_soft_debt_gate() -> dict[str, Any]:
    def run_one(cfg: RunnerConfig, *, expire: bool = False) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as td:
            slug = "btc-updown-5m-100"
            runner = DPlusRunner(slug, Path(td), cfg)
            runner.on_book(
                {
                    "ts_ms": 101_000,
                    "yes_ask": 0.50,
                    "no_ask": 0.50,
                    "source_sequence_id": "book-soft-closeability-l1",
                    "event_time_ms": 101_000,
                    "l2_source_sequence_id": "book-soft-closeability-l2",
                    "l2_event_time_ms": 101_000,
                }
            )
            runner.on_trade(
                {
                    "ts_ms": 101_000,
                    "market_side": "YES",
                    "taker_side": "SELL",
                    "price": 0.50,
                    "size": 20.0,
                    "source_sequence_id": "trade-soft-closeability-seed",
                    "event_time_ms": 101_000,
                }
            )
            if expire:
                runner.on_book(
                    {
                        "ts_ms": 102_000,
                        "yes_ask": 0.50,
                        "no_ask": 0.50,
                        "source_sequence_id": "book-soft-closeability-expire",
                        "event_time_ms": 102_000,
                    }
                )
            return {
                "state": comparable_state(runner),
                "events": read_events(Path(td), slug),
                "closeability_debt_open": round(runner.closeability_debt_open, 12),
            }

    allowed = run_one(
        RunnerConfig(
            cooldown_ms=0,
            risk_seed_closeability_soft_net_cap=0.98,
            risk_seed_closeability_debt_floor=0.95,
            risk_seed_closeability_debt_budget=0.10,
        )
    )
    allowed_candidate = next(
        (event for event in allowed["events"] if event.get("kind") == "candidate"),
        {},
    )
    allow_passed = (
        allowed["state"]["metrics"]["candidates"] == 1
        and allowed["state"]["blocked"].get("risk_seed_closeability_debt_budget", 0) == 0
        and allowed_candidate.get("risk_seed_closeability_soft_net_cap") == 0.98
        and allowed_candidate.get("risk_seed_closeability_debt_floor") == 0.95
        and allowed_candidate.get("risk_seed_closeability_debt_budget") == 0.10
        and allowed_candidate.get("risk_seed_closeability_soft_decision") == "allow"
        and (allowed_candidate.get("closeability_debt") or 0.0) > 0
        and allowed["closeability_debt_open"] == allowed_candidate.get("closeability_debt_post_open")
    )

    budget_blocked = run_one(
        RunnerConfig(
            cooldown_ms=0,
            risk_seed_closeability_soft_net_cap=0.98,
            risk_seed_closeability_debt_floor=0.95,
            risk_seed_closeability_debt_budget=0.01,
        )
    )
    budget_block_event = next(
        (event for event in budget_blocked["events"] if event.get("kind") == "risk_seed_closeability_block"),
        {},
    )
    budget_block_passed = (
        budget_blocked["state"]["metrics"]["candidates"] == 0
        and budget_blocked["state"]["blocked"].get("risk_seed_closeability_debt_budget") == 1
        and budget_block_event.get("block_reason") == "risk_seed_closeability_debt_budget"
        and budget_block_event.get("risk_seed_closeability_soft_decision") == "block_debt_budget"
        and (budget_block_event.get("closeability_debt") or 0.0) > 0.01
    )

    soft_cap_blocked = run_one(
        RunnerConfig(
            cooldown_ms=0,
            risk_seed_closeability_soft_net_cap=0.97,
            risk_seed_closeability_debt_floor=0.95,
            risk_seed_closeability_debt_budget=0.10,
        )
    )
    soft_cap_block_event = next(
        (event for event in soft_cap_blocked["events"] if event.get("kind") == "risk_seed_closeability_block"),
        {},
    )
    soft_cap_block_passed = (
        soft_cap_blocked["state"]["metrics"]["candidates"] == 0
        and soft_cap_blocked["state"]["blocked"].get("risk_seed_closeability_soft_net_cap") == 1
        and soft_cap_block_event.get("block_reason") == "risk_seed_closeability_soft_net_cap"
        and soft_cap_block_event.get("risk_seed_closeability_soft_decision") == "block_soft_cap"
        and (soft_cap_block_event.get("closeability_net_pair_cost") or 0.0) > 0.97
    )

    expired = run_one(
        RunnerConfig(
            cooldown_ms=0,
            order_ttl_ms=500,
            risk_seed_closeability_soft_net_cap=0.98,
            risk_seed_closeability_debt_floor=0.95,
            risk_seed_closeability_debt_budget=0.10,
        ),
        expire=True,
    )
    cancel_event = next(
        (event for event in expired["events"] if event.get("kind") == "cancel"),
        {},
    )
    release_passed = (
        expired["state"]["metrics"]["candidates"] == 1
        and expired["state"]["metrics"]["cancelled_orders"] == 1
        and expired["closeability_debt_open"] == 0.0
        and (cancel_event.get("closeability_debt") or 0.0) > 0.0
        and cancel_event.get("closeability_debt_post_open") == 0.0
    )

    passed = allow_passed and budget_block_passed and soft_cap_block_passed and release_passed
    return {
        "name": "risk_seed_closeability_soft_debt_gate",
        "status": "PASS" if passed else "FAIL",
        "allow_passed": allow_passed,
        "budget_block_passed": budget_block_passed,
        "soft_cap_block_passed": soft_cap_block_passed,
        "release_passed": release_passed,
        "allowed_candidate": allowed_candidate,
        "budget_block_event": budget_block_event,
        "soft_cap_block_event": soft_cap_block_event,
        "cancel_event": cancel_event,
    }


def smoke_risk_seed_closeability_cancel_guard() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        runner = DPlusRunner(
            slug,
            Path(td),
            RunnerConfig(
                cooldown_ms=0,
                seed_offset_max_s=1.5,
                risk_seed_closeability_soft_net_cap=0.98,
                risk_seed_closeability_debt_floor=0.95,
                risk_seed_closeability_debt_budget=0.10,
                risk_seed_cancel_on_closeability_net_cap=0.98,
            ),
        )
        runner.on_book(
            {
                "ts_ms": 101_000,
                "yes_ask": 0.50,
                "no_ask": 0.50,
                "source_sequence_id": "book-cancel-closeability-l1",
                "event_time_ms": 101_000,
                "l2_source_sequence_id": "book-cancel-closeability-l2",
                "l2_event_time_ms": 101_000,
            }
        )
        runner.on_trade(
            {
                "ts_ms": 101_000,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.50,
                "size": 20.0,
                "source_sequence_id": "trade-cancel-closeability-seed",
                "event_time_ms": 101_000,
            }
        )
        candidate = next(
            (event for event in read_events(Path(td), slug) if event.get("kind") == "candidate"),
            {},
        )
        runner.on_book(
            {
                "ts_ms": 102_000,
                "yes_ask": 0.50,
                "no_ask": 0.60,
                "source_sequence_id": "book-cancel-closeability-deteriorated",
                "event_time_ms": 102_000,
                "l2_source_sequence_id": "book-cancel-closeability-deteriorated-l2",
                "l2_event_time_ms": 102_000,
            }
        )
        runner.on_trade(
            {
                "ts_ms": 102_100,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.46,
                "size": 100.0,
                "source_sequence_id": "trade-cancel-closeability-through",
                "event_time_ms": 102_100,
            }
        )
        events = read_events(Path(td), slug)
        cancel_event = next(
            (event for event in events if event.get("kind") == "cancel" and event.get("reason") == "closeability_deterioration"),
            {},
        )
        state = comparable_state(runner)

    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-101"
        protected_runner = DPlusRunner(
            slug,
            Path(td),
            RunnerConfig(
                cooldown_ms=0,
                seed_offset_max_s=1.5,
                risk_seed_closeability_soft_net_cap=0.98,
                risk_seed_closeability_debt_floor=0.95,
                risk_seed_closeability_debt_budget=0.10,
                risk_seed_cancel_on_closeability_net_cap=0.98,
            ),
        )
        protected_runner.lots["NO"].append(
            Lot(
                id=1,
                quote_intent_id="existing-no-lot",
                side="NO",
                qty=5.0,
                px=0.40,
                fill_ms=100_000,
                source_order_id=0,
                source_sequence_id="existing-no-source",
                source_event_time_ms=100_000,
            )
        )
        protected_runner.on_book(
            {
                "ts_ms": 101_000,
                "yes_ask": 0.50,
                "no_ask": 0.50,
                "source_sequence_id": "book-risk-reducing-l1",
                "event_time_ms": 101_000,
                "l2_source_sequence_id": "book-risk-reducing-l2",
                "l2_event_time_ms": 101_000,
            }
        )
        protected_runner.on_trade(
            {
                "ts_ms": 101_000,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.50,
                "size": 20.0,
                "source_sequence_id": "trade-risk-reducing-seed",
                "event_time_ms": 101_000,
            }
        )
        protected_runner.on_book(
            {
                "ts_ms": 102_000,
                "yes_ask": 0.50,
                "no_ask": 0.60,
                "source_sequence_id": "book-risk-reducing-deteriorated",
                "event_time_ms": 102_000,
                "l2_source_sequence_id": "book-risk-reducing-deteriorated-l2",
                "l2_event_time_ms": 102_000,
            }
        )
        protected_runner.on_trade(
            {
                "ts_ms": 102_100,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.46,
                "size": 100.0,
                "source_sequence_id": "trade-risk-reducing-through",
                "event_time_ms": 102_100,
            }
        )
        protected_events = read_events(Path(td), slug)
        protected_candidate = next(
            (event for event in protected_events if event.get("kind") == "candidate"),
            {},
        )
        protected_state = comparable_state(protected_runner)

    candidate_passed = (
        candidate.get("risk_seed_closeability_soft_decision") == "allow"
        and (candidate.get("closeability_net_pair_cost") or 2.0) <= 0.98
        and (candidate.get("closeability_debt") or 0.0) > 0.0
    )
    cancel_passed = (
        cancel_event.get("reason") == "closeability_deterioration"
        and cancel_event.get("risk_seed_cancel_on_closeability_net_cap") == 0.98
        and (cancel_event.get("closeability_current_net_pair_cost") or 0.0) > 0.98
        and cancel_event.get("closeability_debt_post_open") == 0.0
    )
    no_fill_after_cancel_passed = (
        state["metrics"]["queue_supported_fills"] == 0
        and state["metrics"]["cancelled_orders"] == 1
        and state["metrics"]["candidates"] == 1
    )
    risk_reducing_not_cancelled_passed = (
        protected_candidate.get("risk_increasing_seed") is False
        and protected_state["metrics"]["cancelled_orders"] == 0
        and protected_state["metrics"]["queue_supported_fills"] == 1
    )
    passed = candidate_passed and cancel_passed and no_fill_after_cancel_passed and risk_reducing_not_cancelled_passed
    return {
        "name": "risk_seed_closeability_cancel_guard",
        "status": "PASS" if passed else "FAIL",
        "candidate_passed": candidate_passed,
        "cancel_passed": cancel_passed,
        "no_fill_after_cancel_passed": no_fill_after_cancel_passed,
        "risk_reducing_not_cancelled_passed": risk_reducing_not_cancelled_passed,
        "candidate": candidate,
        "cancel_event": cancel_event,
        "state": state,
        "protected_candidate": protected_candidate,
        "protected_state": protected_state,
    }


def smoke_risk_seed_pending_opp_credit_guard() -> dict[str, Any]:
    def run_one(cfg: RunnerConfig) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as td:
            slug = "btc-updown-5m-100"
            runner = DPlusRunner(slug, Path(td), cfg)
            runner.on_book(
                {
                    "ts_ms": 101_000,
                    "yes_ask": 0.50,
                    "no_ask": 0.50,
                    "source_sequence_id": "book-pending-credit-l1",
                    "event_time_ms": 101_000,
                    "l2_source_sequence_id": "book-pending-credit-l2",
                    "l2_event_time_ms": 101_000,
                }
            )
            for ts_ms, side, source in (
                (101_000, "YES", "trade-pending-credit-yes"),
                (101_100, "NO", "trade-pending-credit-no-1"),
                (101_200, "NO", "trade-pending-credit-no-2"),
            ):
                runner.on_trade(
                    {
                        "ts_ms": ts_ms,
                        "market_side": side,
                        "taker_side": "SELL",
                        "price": 0.50,
                        "size": 20.0,
                        "source_sequence_id": source,
                        "event_time_ms": ts_ms,
                    }
                )
            return {
                "state": comparable_state(runner),
                "events": read_events(Path(td), slug),
            }

    legacy = run_one(RunnerConfig(cooldown_ms=0, imbalance_qty_cap=2.0))
    legacy_candidates = [event for event in legacy["events"] if event.get("kind") == "candidate"]
    legacy_last = legacy_candidates[-1] if legacy_candidates else {}
    legacy_passed = (
        legacy["state"]["metrics"]["candidates"] == 3
        and legacy_last.get("side") == "NO"
        and legacy_last.get("risk_seed_pending_opp_credit") == 1.0
        and legacy_last.get("risk_seed_pending_opp_qty") == 2.0
        and legacy_last.get("risk_seed_credited_opp_qty") == 2.0
    )

    filled_only = run_one(
        RunnerConfig(cooldown_ms=0, imbalance_qty_cap=2.0, risk_seed_pending_opp_credit=0.0)
    )
    filled_only_candidates = [event for event in filled_only["events"] if event.get("kind") == "candidate"]
    filled_only_second = filled_only_candidates[-1] if filled_only_candidates else {}
    filled_only_passed = (
        filled_only["state"]["metrics"]["candidates"] == 2
        and filled_only["state"]["blocked"].get("imbalance_qty") == 1
        and filled_only_second.get("risk_seed_pending_opp_credit") == 0.0
        and filled_only_second.get("risk_seed_pending_opp_qty") == 2.0
        and filled_only_second.get("risk_seed_credited_opp_qty") == 0.0
    )

    passed = legacy_passed and filled_only_passed
    return {
        "name": "risk_seed_pending_opp_credit_guard",
        "status": "PASS" if passed else "FAIL",
        "legacy_passed": legacy_passed,
        "filled_only_passed": filled_only_passed,
        "legacy_candidates": legacy_candidates,
        "filled_only_candidates": filled_only_candidates,
        "filled_only_blocked": filled_only["state"]["blocked"],
    }


def smoke_pair_completion_net_cap() -> dict[str, Any]:
    def run_one(cfg: RunnerConfig, *, pre_pair_pnl: float = 0.0) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as td:
            slug = "btc-updown-5m-100"
            runner = DPlusRunner(slug, Path(td), cfg)
            runner.metrics.pair_pnl = pre_pair_pnl
            runner.surplus_bank = pre_pair_pnl
            runner.lots["NO"].append(
                Lot(
                    id=1,
                    quote_intent_id="existing-no-lot",
                    side="NO",
                    qty=1.25,
                    px=0.55,
                    fill_ms=99_000,
                    source_order_id=1,
                    source_sequence_id="existing-no-source",
                    source_event_time_ms=99_000,
                )
            )
            runner.on_book(
                {
                    "ts_ms": 100_000,
                    "yes_ask": 0.45,
                    "no_ask": 0.45,
                    "source_sequence_id": "book-pair-completion",
                    "event_time_ms": 100_000,
                }
            )
            runner.on_trade(
                {
                    "ts_ms": 101_000,
                    "market_side": "YES",
                    "taker_side": "SELL",
                    "price": 0.60,
                    "size": 10.0,
                    "source_sequence_id": "trade-pair-completion",
                    "event_time_ms": 101_000,
                }
            )
            return {"state": comparable_state(runner), "events": read_events(Path(td), slug)}

    default = run_one(RunnerConfig(cooldown_ms=0))
    default_candidate = next((event for event in default["events"] if event.get("kind") == "candidate"), {})
    default_passed = (
        default["state"]["metrics"]["candidates"] == 1
        and default_candidate.get("pair_completion_net_cap") is None
        and (default_candidate.get("pair_completion_worst_net_pair_cost") or 0.0) > 1.0
    )

    blocked = run_one(RunnerConfig(cooldown_ms=0, pair_completion_net_cap=1.0))
    block_event = next((event for event in blocked["events"] if event.get("kind") == "pair_completion_block"), {})
    blocked_passed = (
        blocked["state"]["metrics"]["candidates"] == 0
        and blocked["state"]["blocked"].get("pair_completion_net_cap") == 1
        and block_event.get("block_reason") == "pair_completion_net_cap"
        and block_event.get("pair_completion_decision") == "block_net_cap"
        and (block_event.get("pair_completion_worst_net_pair_cost") or 0.0) > 1.0
    )

    allowed = run_one(RunnerConfig(cooldown_ms=0, pair_completion_net_cap=1.12))
    allowed_candidate = next((event for event in allowed["events"] if event.get("kind") == "candidate"), {})
    allowed_passed = (
        allowed["state"]["metrics"]["candidates"] == 1
        and allowed["state"]["blocked"].get("pair_completion_net_cap", 0) == 0
        and allowed_candidate.get("pair_completion_net_cap") == 1.12
        and (allowed_candidate.get("pair_completion_worst_net_pair_cost") or 2.0) <= 1.12
    )

    floor_blocked = run_one(RunnerConfig(cooldown_ms=0, pair_completion_min_pair_pnl_after=0.0))
    floor_block_event = next((event for event in floor_blocked["events"] if event.get("kind") == "pair_completion_block"), {})
    floor_blocked_passed = (
        floor_blocked["state"]["metrics"]["candidates"] == 0
        and floor_blocked["state"]["blocked"].get("pair_completion_pair_pnl_floor") == 1
        and floor_block_event.get("block_reason") == "pair_completion_pair_pnl_floor"
        and floor_block_event.get("pair_completion_decision") == "block_pair_pnl_floor"
        and (floor_block_event.get("pair_completion_projected_pair_pnl_after") or 0.0) < 0.0
    )

    floor_allowed = run_one(
        RunnerConfig(cooldown_ms=0, pair_completion_min_pair_pnl_after=0.0),
        pre_pair_pnl=0.20,
    )
    floor_allowed_candidate = next((event for event in floor_allowed["events"] if event.get("kind") == "candidate"), {})
    floor_allowed_passed = (
        floor_allowed["state"]["metrics"]["candidates"] == 1
        and floor_allowed["state"]["blocked"].get("pair_completion_pair_pnl_floor", 0) == 0
        and floor_allowed_candidate.get("pair_completion_min_pair_pnl_after") == 0.0
        and (floor_allowed_candidate.get("pair_completion_projected_pair_pnl_after") or -1.0) >= 0.0
    )

    return {
        "name": "pair_completion_net_cap",
        "status": "PASS"
        if default_passed and blocked_passed and allowed_passed and floor_blocked_passed and floor_allowed_passed
        else "FAIL",
        "default_state": default["state"],
        "blocked_state": blocked["state"],
        "allowed_state": allowed["state"],
        "block_event": block_event,
        "allowed_candidate": allowed_candidate,
        "floor_block_event": floor_block_event,
        "floor_allowed_candidate": floor_allowed_candidate,
    }


def smoke_strict_rescue_surplus_floor() -> dict[str, Any]:
    def run_case(pre_pair_pnl: float, surplus_cap: float | None, floor: float | None) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as td:
            slug = "btc-updown-5m-103"
            runner = DPlusRunner(
                slug,
                Path(td),
                RunnerConfig(
                    salvage_net_cap=0.95,
                    salvage_age_ms=30_000,
                    salvage_min_lot_cost=0.25,
                    strict_rescue_mode="source_audit",
                    strict_rescue_l1_age_max_ms=50,
                    strict_rescue_require_book_source=True,
                    strict_rescue_surplus_net_cap=surplus_cap,
                    strict_rescue_min_pair_pnl_after=floor,
                    write_rescue_block_diagnostics=True,
                ),
            )
            runner.metrics.pair_pnl = pre_pair_pnl
            runner.lots["YES"].append(
                Lot(
                    id=1,
                    quote_intent_id="quote-surplus-rescue",
                    side="YES",
                    qty=2.0,
                    px=0.40,
                    fill_ms=1_000,
                    source_order_id=1,
                    source_sequence_id="fill-surplus-rescue",
                    source_event_time_ms=1_000,
                )
            )
            runner.on_book(
                {
                    "ts_ms": 40_000,
                    "yes_ask": 0.40,
                    "no_ask": 0.65,
                    "source_sequence_id": "book-surplus-rescue",
                    "event_time_ms": 39_990,
                }
            )
            events = read_events(Path(td), slug)
            return {
                "state": comparable_state(runner),
                "salvage_events": [event for event in events if event.get("kind") == "fak_salvage"],
                "block_events": [event for event in events if event.get("kind") == "strict_rescue_block"],
            }

    default_blocked = run_case(pre_pair_pnl=1.0, surplus_cap=None, floor=None)
    default_block = default_blocked["block_events"][-1] if default_blocked["block_events"] else {}
    default_passed = (
        not default_blocked["salvage_events"]
        and default_block.get("block_reason") == "strict_rescue_net_pair_cap"
    )

    floor_blocked = run_case(pre_pair_pnl=0.0, surplus_cap=1.10, floor=0.0)
    floor_block = floor_blocked["block_events"][-1] if floor_blocked["block_events"] else {}
    floor_blocked_passed = (
        not floor_blocked["salvage_events"]
        and floor_block.get("block_reason") == "strict_rescue_pair_pnl_floor"
        and (floor_block.get("strict_rescue_projected_pair_pnl_after") or 1.0) < 0.0
    )

    surplus_allowed = run_case(pre_pair_pnl=0.30, surplus_cap=1.10, floor=0.0)
    salvage_event = surplus_allowed["salvage_events"][-1] if surplus_allowed["salvage_events"] else {}
    surplus_allowed_passed = (
        len(surplus_allowed["salvage_events"]) == 1
        and salvage_event.get("strict_rescue_surplus_decision") == "allow_surplus"
        and (salvage_event.get("net_pair_cost") or 0.0) > 0.95
        and (salvage_event.get("strict_rescue_projected_pair_pnl_after") or -1.0) >= 0.0
        and surplus_allowed["state"]["metrics"]["pair_pnl"] >= 0.0
    )

    return {
        "name": "strict_rescue_surplus_floor",
        "status": "PASS" if default_passed and floor_blocked_passed and surplus_allowed_passed else "FAIL",
        "default_block": default_block,
        "floor_block": floor_block,
        "surplus_allowed_event": salvage_event,
        "surplus_allowed_state": surplus_allowed["state"],
    }


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def smoke_normalized_lifecycle_exports() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        out = Path(td)
        runner = DPlusRunner(
            slug,
            out,
            RunnerConfig(
                write_normalized_lifecycle=True,
                source_quality_require_trade_source=True,
                source_quality_require_l1_source=True,
                source_quality_l1_age_max_ms=1000,
                source_quality_require_l2_source=True,
                salvage_net_cap=1.10,
                salvage_age_ms=30_000,
                strict_rescue_mode="source_audit",
                strict_rescue_l1_age_max_ms=50,
                strict_rescue_close_size_haircut=0.50,
                strict_rescue_require_book_source=True,
                strict_rescue_require_l2_source=True,
            ),
        )
        runner.on_book(
            {
                "ts_ms": 101_000,
                "yes_ask": 0.50,
                "no_ask": 0.50,
                "source_sequence_id": "book-l1-seq",
                "event_time_ms": 101_000,
                "l2_source_sequence_id": "book-l2-seq",
                "l2_event_time_ms": 101_000,
            }
        )
        runner.on_trade(
            {
                "ts_ms": 101_000,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.50,
                "size": 20.0,
                "source_sequence_id": "trade-seq",
                "event_time_ms": 101_000,
            }
        )
        runner.lots["YES"].append(
            Lot(
                id=99,
                quote_intent_id="quote-source-99",
                side="YES",
                qty=4.0,
                px=0.40,
                fill_ms=70_000,
                source_order_id=99,
                source_sequence_id="fill-seq-99",
                source_event_time_ms=70_000,
            )
        )
        runner.on_book(
            {
                "ts_ms": 101_010,
                "yes_ask": 0.40,
                "no_ask": 0.50,
                "source_sequence_id": "book-l1-rescue",
                "event_time_ms": 101_000,
                "l2_source_sequence_id": "book-l2-rescue",
                "l2_event_time_ms": 101_000,
            }
        )
        runner.write_summary(final=True)

        manifest_path = out / f"{slug}.normalized_lifecycle_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        actions = read_csv_rows(out / f"{slug}.would_action_decisions.csv")
        orders = read_csv_rows(out / f"{slug}.would_order_events.csv")
        fills = read_csv_rows(out / f"{slug}.would_fill_events.csv")
        inventory = read_csv_rows(out / f"{slug}.simulated_inventory_events.csv")
        residual = read_csv_rows(out / f"{slug}.residual_fifo_lots.csv")
        rescue = read_csv_rows(out / f"{slug}.strict_rescue_closes.csv")
        passed = (
            manifest["orders_sent"] is False
            and manifest["row_counts"]["would_action_decisions"] >= 1
            and len(actions) >= 1
            and any(row["decision"] == "accept" for row in actions)
            and len(orders) >= 1
            and len(fills) >= 1
            and len(inventory) >= 1
            and len(residual) >= 1
            and len(rescue) == 1
            and rescue[0]["strict_rescue_l2_source_sequence_id"] == "book-l2-rescue"
        )
        return {
            "name": "normalized_lifecycle_exports",
            "status": "PASS" if passed else "FAIL",
            "manifest": manifest,
            "row_counts": {
                "actions": len(actions),
                "orders": len(orders),
                "fills": len(fills),
                "inventory": len(inventory),
                "residual": len(residual),
                "rescue": len(rescue),
            },
        }


def smoke_source_linkage_summary() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as td:
        slug = "btc-updown-5m-100"
        out = Path(td)
        runner = DPlusRunner(slug, out, RunnerConfig())
        runner.on_book(
            {
                "ts_ms": 101_000,
                "yes_ask": 0.51,
                "no_ask": 0.49,
                "l1_source_sequence_id": "l1-top",
                "l2_source_sequence_id": "l2-top",
                "event_time_ms": 100_999,
                "l2_event_time_ms": 100_998,
            }
        )
        runner.on_trade(
            {
                "ts_ms": 101_005,
                "market_side": "YES",
                "taker_side": "SELL",
                "price": 0.50,
                "size": 10.0,
                "source_sequence_id": "trade-top",
                "event_time_ms": 101_004,
            }
        )
        runner.write_summary(final=True)
        summary = json.loads((out / f"{slug}.summary.json").read_text())
        source = summary.get("source_linkage", {})
        passed = (
            source.get("book_l1_source_nonempty") == 1
            and source.get("book_l2_source_nonempty") == 1
            and source.get("trade_source_nonempty") == 1
            and source.get("book_l1_first_source_sequence_id") == "l1-top"
            and source.get("book_l2_first_source_sequence_id") == "l2-top"
            and source.get("trade_first_source_sequence_id") == "trade-top"
            and source.get("book_l1_event_time_nonempty") == 1
            and source.get("book_l2_event_time_nonempty") == 1
            and source.get("trade_event_time_nonempty") == 1
        )
        return {
            "name": "source_linkage_summary",
            "status": "PASS" if passed else "FAIL",
            "source_linkage": source,
        }


def smoke_fair_price_admission_gate() -> dict[str, Any]:
    def run_case(fair_probability: float, admission_mode: str = "fair_probability") -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as td:
            slug = "btc-updown-15m-100"
            row = {
                "row_id": f"fair-{fair_probability}-{admission_mode}",
                "market_slug": slug,
                "side": "YES",
                "min_seconds_to_close": 60,
                "max_seconds_to_close": 900,
                "admission_mode": admission_mode,
            }
            if admission_mode != "pair_cost_only":
                row["fair_side_probability"] = fair_probability
            gate = FairPriceAdmissionGate(
                [row],
                min_edge=0.015,
                max_pair_cost=0.975,
                min_seconds_to_close=60,
                max_seconds_to_close=900,
                max_row_age_ms=None,
            )
            runner = DPlusRunner(slug, Path(td), RunnerConfig(), fair_price_gate=gate)
            runner.on_book(
                {
                    "ts_ms": 101_000,
                    "yes_ask": 0.50,
                    "no_ask": 0.47,
                    "source_sequence_id": "book-fair",
                    "event_time_ms": 101_000,
                }
            )
            runner.on_trade(
                {
                    "ts_ms": 101_000,
                    "market_side": "YES",
                    "taker_side": "SELL",
                    "price": 0.50,
                    "size": 20.0,
                    "source_sequence_id": "trade-fair",
                    "event_time_ms": 101_000,
                }
            )
            return {
                "state": comparable_state(runner),
                "events": read_events(Path(td), slug),
            }

    allow = run_case(0.52)
    block = run_case(0.47)
    pair_cost_only = run_case(0.0, admission_mode="pair_cost_only")
    allow_candidates = [event for event in allow["events"] if event.get("kind") == "candidate"]
    block_events = [event for event in block["events"] if event.get("kind") == "fair_price_admission_block"]
    pair_cost_only_candidates = [event for event in pair_cost_only["events"] if event.get("kind") == "candidate"]
    passed = (
        allow["state"]["metrics"]["candidates"] == 1
        and bool(allow_candidates)
        and allow_candidates[0].get("fair_price_admission_decision") == "allow"
        and allow_candidates[0].get("fair_price_pair_cost_after_fee") <= 0.975
        and block["state"]["metrics"]["candidates"] == 0
        and block["state"]["blocked"].get("fair_price_edge_after_fee", 0) == 1
        and bool(block_events)
        and block_events[0].get("fair_price_admission_block_reason") == "fair_price_edge_after_fee"
        and pair_cost_only["state"]["metrics"]["candidates"] == 1
        and bool(pair_cost_only_candidates)
        and pair_cost_only_candidates[0].get("fair_price_admission_mode") == "pair_cost_only"
        and pair_cost_only_candidates[0].get("fair_price_side_probability") is None
        and pair_cost_only_candidates[0].get("fair_price_pair_cost_after_fee") <= 0.975
    )
    return {
        "name": "fair_price_admission_gate",
        "status": "PASS" if passed else "FAIL",
        "allow_state": allow["state"],
        "allow_candidate": allow_candidates[0] if allow_candidates else {},
        "block_state": block["state"],
        "block_event": block_events[0] if block_events else {},
        "pair_cost_only_state": pair_cost_only["state"],
        "pair_cost_only_candidate": pair_cost_only_candidates[0] if pair_cost_only_candidates else {},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    checks = [
        smoke_default_off_parity(),
        smoke_surplus_budget_paths(),
        smoke_strict_rescue_event_fields(),
        smoke_strict_rescue_skip_low_cost_lots(),
        smoke_source_quality_and_l2_paths(),
        smoke_risk_seed_closeability_gate(),
        smoke_risk_seed_closeability_soft_debt_gate(),
        smoke_risk_seed_closeability_cancel_guard(),
        smoke_risk_seed_pending_opp_credit_guard(),
        smoke_pair_completion_net_cap(),
        smoke_strict_rescue_surplus_floor(),
        smoke_normalized_lifecycle_exports(),
        smoke_source_linkage_summary(),
        smoke_fair_price_admission_gate(),
    ]
    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    report = {
        "status": status,
        "artifact": "xuan_no_order_runner_scaffold_smoke",
        "raw_replay_collector_scanned": False,
        "remote_runner_started": False,
        "checks": checks,
    }
    (out_dir / "smoke_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    if status != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
