"""
Common helpers for replay, pair ledger simulation, and metrics.
Extracted / centralized during crazy optimization wave to reduce duplication
across the 40+ experiment scripts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass
class PairLedgerEvent:
    side: str  # "Yes" or "No"
    direction: str  # "Buy"
    size: float
    price: float
    ts: float  # unix or relative
    kind: str = "Fill"


@dataclass
class PairTrancheSnapshot:
    first_side: Optional[str] = None
    first_qty: float = 0.0
    hedge_qty: float = 0.0
    residual_qty: float = 0.0
    pairable_qty: float = 0.0
    first_vwap: float = 0.0
    hedge_vwap: float = 0.0
    pair_cost_tranche: float = 0.0


def pgt_fill(side: str, size: float, price: float) -> PairLedgerEvent:
    """Python mirror of the Rust test helper used in PGT analysis."""
    return PairLedgerEvent(side=side, direction="Buy", size=size, price=price, ts=0.0, kind="Fill")


def simulate_pair_cost(fills: List[PairLedgerEvent]) -> PairTrancheSnapshot:
    """
    Very small pure-Python simulation of the Rust PairLedger recompute logic.
    Useful for quick Python-side checks and for porting analyzers.
    Only handles the simple "one active tranche, buy fills" case for now.
    """
    first_lots: List[Tuple[float, float]] = []
    hedge_lots: List[Tuple[float, float]] = []
    first_side: Optional[str] = None

    for ev in fills:
        if first_side is None:
            first_side = ev.side
        if ev.side == first_side:
            first_lots.append((ev.size, ev.price))
        else:
            hedge_lots.append((ev.size, ev.price))

    first_qty = sum(q for q, _ in first_lots)
    hedge_qty = sum(q for q, _ in hedge_lots)
    pairable = min(first_qty, hedge_qty)
    residual = abs(first_qty - hedge_qty)

    def wavg(lots: List[Tuple[float, float]]) -> float:
        if not lots:
            return 0.0
        total_q = sum(q for q, _ in lots)
        if total_q == 0:
            return 0.0
        return sum(q * p for q, p in lots) / total_q

    return PairTrancheSnapshot(
        first_side=first_side,
        first_qty=first_qty,
        hedge_qty=hedge_qty,
        residual_qty=residual,
        pairable_qty=pairable,
        first_vwap=wavg(first_lots),
        hedge_vwap=wavg(hedge_lots),
        pair_cost_tranche=(wavg(first_lots) + wavg(hedge_lots)) if pairable > 0 else 0.0,
    )


def build_simple_ledger_snapshot(first_side: str, first_fills: List[Tuple[float, float]]) -> PairTrancheSnapshot:
    """Convenience for tests / quick experiments."""
    events = [pgt_fill(first_side, q, p) for q, p in first_fills]
    return simulate_pair_cost(events)


# --- Pair Arb specific profit simulation helpers (crazy money-making iteration) ---

@dataclass
class PairArbSimState:
    net_diff: float = 0.0
    paired_pnl: float = 0.0
    excess_captured: float = 0.0
    residual_cost: float = 0.0

def simulate_pair_arb_add(state: PairArbSimState, side: str, size: float, price: float, pair_target: float, open_edge: float = 0.0) -> PairArbSimState:
    """Minimal simulation of pair_arb risk/pairing add for rapid Python-side tuning.
    Use with real ticks or replay to estimate edge capture vs residual risk.
    """
    if side.upper() == "YES":
        state.net_diff += size
    else:
        state.net_diff -= size
    excess = max(0.0, (pair_target - 0.0) - price * 2)  # rough; plug real mid_sum - target
    state.excess_captured += excess * size * 0.5  # simplified
    state.paired_pnl += max(0.0, (1.0 - (price * 2 - 0.01))) * min(abs(state.net_diff), size)  # toy
    state.residual_cost = abs(state.net_diff) * pair_target
    return state

def simulate_nagi777_5m_seed(state: PairArbSimState, side: str, size: float, price: float, pair_target: float, local_confidence: float = 0.8) -> PairArbSimState:
    """Minimal sim for nagi777-style 5m (high participation short windows + local price timing).
    Deep dig V1: 30338 5m / L2 depth 1460 / rescore low res 0.025. Use local_conf for boundary/uncertainty boost (parallel absorption).
    local_confidence from self-built agg boosts "safe" participation while controlling cost/residual.
    Use with V1 5m boundary data.
    """
    # Simulate early seed on local signal
    if local_confidence > 0.7:  # nagi-like safe high part
        if side.upper() == "YES":
            state.net_diff += size
        else:
            state.net_diff -= size
        # Low cost via local timing
        state.excess_captured += (1.0 - pair_target) * size * local_confidence
        state.paired_pnl += max(0.0, (1.0 - price*2)) * size * 0.6
    state.residual_cost = abs(state.net_diff) * pair_target
    return state