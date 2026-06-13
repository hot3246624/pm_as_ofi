"""
pm_research - Shared library for Polymarket research / replay / pair-ledger analysis.

Goal (crazy iteration wave): stop the sprawl of one-off analyze_*.py / pull_*.py scripts.
Common loaders, pair cost / tranche simulation, metrics, local-agg dataset helpers.

Usage:
    from pm_research.replay import load_replay_round
    from pm_research.pair_ledger import simulate_pair_cost
"""

__version__ = "0.1.0-extracted-wave"

from .common import pgt_fill, build_simple_ledger_snapshot, simulate_pair_arb_add, PairArbSimState, simulate_nagi777_5m_seed  # re-exports for convenience

__all__ = ["pgt_fill", "build_simple_ledger_snapshot", "load_replay_round", "simulate_pair_cost", "simulate_pair_arb_add", "PairArbSimState", "simulate_nagi777_5m_seed"]