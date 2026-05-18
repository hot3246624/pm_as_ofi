use serde_json::{json, Value};

use super::{
    Book, StrategyCoordinator, StrategyInventoryMetrics, StrategyKind, XuanB27DplusMode,
    XuanB27DplusStrategyConfig,
};
use crate::polymarket::messages::{
    FillStatus, InventorySnapshot, InventoryState, OrderAttemptTrace, XuanB27DplusSourceTruthEvent,
};
use crate::polymarket::pair_ledger::{CapitalState, PairLedgerSnapshot, PairTranche};
use crate::polymarket::types::Side;
use crate::polymarket::xuan_b27_dplus_correlation::{
    XuanB27DplusCorrelationIds, XuanB27DplusTruthVerdict,
};
use crate::polymarket::xuan_b27_dplus_execution_controller::{
    evaluate_xuan_b27_dplus_controller, XuanB27DplusControllerInput,
};
use crate::polymarket::xuan_b27_dplus_oms_adapter::{
    adapt_xuan_b27_dplus_to_oms_commands, XuanB27DplusOmsAdapterInput,
};
use crate::polymarket::xuan_b27_dplus_order_plan::XuanB27DplusOrderPlanInput;

impl StrategyCoordinator {
    pub(super) fn handle_xuan_b27_dplus_source_truth_event(
        &mut self,
        event: XuanB27DplusSourceTruthEvent,
    ) {
        if self.cfg.strategy != StrategyKind::XuanB27Dplus {
            return;
        }
        let mut source_truth = self.xuan_b27_dplus_runtime_source_truth();
        match event {
            XuanB27DplusSourceTruthEvent::OrderAccepted {
                venue_order_id_present,
                ..
            } => {
                source_truth.order_truth = if venue_order_id_present {
                    super::XuanB27DplusRuntimeTruthStatus::Pass
                } else {
                    super::XuanB27DplusRuntimeTruthStatus::Unknown
                };
            }
            XuanB27DplusSourceTruthEvent::UserWsFillParsed {
                has_order_id,
                has_trade_id,
                has_liquidity_role,
                has_fee_rate_bps,
                status,
                ..
            } => {
                source_truth.fill_truth = if status == FillStatus::Failed {
                    super::XuanB27DplusRuntimeTruthStatus::Fail
                } else if has_order_id && has_trade_id && has_liquidity_role && has_fee_rate_bps {
                    super::XuanB27DplusRuntimeTruthStatus::Pass
                } else {
                    super::XuanB27DplusRuntimeTruthStatus::Unknown
                };
            }
            XuanB27DplusSourceTruthEvent::WalletSnapshot { valid, .. } => {
                source_truth.wallet_truth = if valid {
                    super::XuanB27DplusRuntimeTruthStatus::Pass
                } else {
                    super::XuanB27DplusRuntimeTruthStatus::Fail
                };
            }
            XuanB27DplusSourceTruthEvent::RedeemResult {
                has_redeem_attempt_id,
                has_redeem_tx_hash,
                has_redeem_confirmation,
                ..
            } => {
                source_truth.redeem_truth =
                    if has_redeem_attempt_id && (has_redeem_tx_hash || has_redeem_confirmation) {
                        super::XuanB27DplusRuntimeTruthStatus::Pass
                    } else {
                        super::XuanB27DplusRuntimeTruthStatus::Unknown
                    };
            }
            XuanB27DplusSourceTruthEvent::CashflowSnapshot {
                has_cashflow_snapshot_id,
                ..
            } => {
                source_truth.cashflow_truth = if has_cashflow_snapshot_id {
                    super::XuanB27DplusRuntimeTruthStatus::Pass
                } else {
                    super::XuanB27DplusRuntimeTruthStatus::Unknown
                };
            }
        }
        self.set_xuan_b27_dplus_runtime_source_truth(source_truth);
    }

    pub(super) fn emit_xuan_b27_dplus_observer_tick(
        &self,
        inv: &InventorySnapshot,
        book: &Book,
        metrics: &StrategyInventoryMetrics,
    ) {
        if self.cfg.strategy != StrategyKind::XuanB27Dplus {
            return;
        }
        let cfg = &self.cfg.xuan_b27_dplus;
        if matches!(cfg.mode, XuanB27DplusMode::Disabled) {
            return;
        }
        let (Some(recorder), Some(meta)) = (&self.recorder, &self.recorder_meta) else {
            return;
        };

        let market_enabled = cfg
            .market_slug
            .as_deref()
            .map(|slug| xuan_b27_dplus_market_filter_matches(slug, &meta.slug))
            .unwrap_or(true);
        let event = if cfg.mode == XuanB27DplusMode::Canary {
            "xuan_b27_dplus_canary_blocked_not_implemented"
        } else {
            "xuan_b27_dplus_observer_tick"
        };
        let market_session_id = format!("{}:{}", meta.slug, meta.condition_id);
        let candidate_prefix = format!(
            "{}:{}:{}",
            StrategyKind::XuanB27Dplus.as_str(),
            meta.slug,
            self.stats.ticks
        );
        let payload = build_xuan_b27_dplus_observer_payload(
            cfg,
            self.cfg.tick_size,
            self.stats.ticks,
            market_enabled,
            &market_session_id,
            &candidate_prefix,
            inv,
            book,
            metrics,
        );

        recorder.emit_strategy_event(meta, event, payload);
    }

    pub(super) async fn maybe_dispatch_xuan_b27_dplus_canary_commands(
        &mut self,
        inv: &InventorySnapshot,
        book: &Book,
        metrics: &StrategyInventoryMetrics,
    ) -> XuanB27DplusRuntimeDispatchSummary {
        if self.cfg.strategy != StrategyKind::XuanB27Dplus {
            return XuanB27DplusRuntimeDispatchSummary::blocked("strategy_not_xuan_b27_dplus");
        }

        let cfg = &self.cfg.xuan_b27_dplus;
        if cfg.mode != XuanB27DplusMode::Canary {
            return XuanB27DplusRuntimeDispatchSummary::blocked("mode_not_canary");
        }
        let source_truth = self.xuan_b27_dplus_runtime_source_truth();
        let runtime_source_truth_ready = source_truth.account_truth_ready();
        let account_truth_ready = cfg.account_truth_ready && runtime_source_truth_ready;

        let market_enabled = self
            .recorder_meta
            .as_ref()
            .and_then(|meta| {
                cfg.market_slug
                    .as_deref()
                    .map(|slug| xuan_b27_dplus_market_filter_matches(slug, &meta.slug))
            })
            .unwrap_or(true);
        let market_session_id = self
            .recorder_meta
            .as_ref()
            .map(|meta| format!("{}:{}", meta.slug, meta.condition_id))
            .unwrap_or_else(|| "xuan_b27_dplus:local_runtime_gate".to_string());
        let candidate_prefix = self
            .recorder_meta
            .as_ref()
            .map(|meta| {
                format!(
                    "{}:{}:{}",
                    StrategyKind::XuanB27Dplus.as_str(),
                    meta.slug,
                    self.stats.ticks
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "{}:local:{}",
                    StrategyKind::XuanB27Dplus.as_str(),
                    self.stats.ticks
                )
            });

        let controller = evaluate_xuan_b27_dplus_controller(XuanB27DplusControllerInput {
            plan_input: XuanB27DplusOrderPlanInput {
                cfg,
                tick_size: self.cfg.tick_size,
                market_enabled,
                explicit_canary_approval: cfg.explicit_canary_approval,
                account_truth_ready,
                book: *book,
                market_session_id: &market_session_id,
                candidate_prefix: &candidate_prefix,
            },
            controller_enabled: cfg.runtime_wiring_enabled,
            oms_adapter_implemented: cfg.oms_adapter_enabled,
        });
        let adapter = adapt_xuan_b27_dplus_to_oms_commands(XuanB27DplusOmsAdapterInput {
            controller_decision: &controller,
            explicit_canary_approval: cfg.explicit_canary_approval,
            account_truth_ready,
            stop_on_unknown: cfg.stop_on_unknown,
            risk_caps_ready: xuan_b27_dplus_g2_risk_caps_ready(cfg),
            adapter_enabled: cfg.oms_adapter_enabled,
        });

        let mut summary = XuanB27DplusRuntimeDispatchSummary {
            status: if adapter.commands.is_empty() {
                "BLOCKED"
            } else {
                "COMMANDS_READY"
            },
            controller_status: controller.status.as_str(),
            adapter_status: adapter.status.as_str(),
            planner_status: controller.planner_status.as_str(),
            command_count: adapter.command_count,
            sent_count: 0,
            orders_submitted: false,
            block_reasons: adapter.block_reasons.clone(),
        };

        if !adapter.commands.is_empty() {
            for command in adapter.commands {
                if self.om_tx.send(command).await.is_ok() {
                    summary.sent_count += 1;
                } else {
                    summary.status = "SEND_FAILED";
                    push_unique_reason(&mut summary.block_reasons, "oms_channel_closed");
                    break;
                }
            }
        }

        if let (Some(recorder), Some(meta)) = (&self.recorder, &self.recorder_meta) {
            recorder.emit_strategy_event(
                meta,
                "xuan_b27_dplus_canary_runtime_gate",
                json!({
                    "schema_version": 1,
                    "strategy": StrategyKind::XuanB27Dplus.as_str(),
                    "mode": cfg.mode.as_str(),
                    "market_enabled": market_enabled,
                    "controller_status": summary.controller_status,
                    "adapter_status": summary.adapter_status,
                    "planner_status": summary.planner_status,
                    "command_count": summary.command_count,
                    "sent_count": summary.sent_count,
                    "orders_submitted": summary.orders_submitted,
                    "block_reasons": summary.block_reasons,
                    "gates": {
                        "explicit_canary_approval": cfg.explicit_canary_approval,
                        "config_account_truth_ready": cfg.account_truth_ready,
                        "runtime_source_truth_ready": runtime_source_truth_ready,
                        "account_truth_ready": account_truth_ready,
                        "stop_on_unknown": cfg.stop_on_unknown,
                        "risk_caps_ready": xuan_b27_dplus_g2_risk_caps_ready(cfg),
                        "runtime_wiring_enabled": cfg.runtime_wiring_enabled,
                        "oms_adapter_enabled": cfg.oms_adapter_enabled,
                    },
                    "source_truth": source_truth_payload(source_truth),
                    "metrics": {
                        "paired_qty": metrics.paired_qty,
                        "pair_cost": metrics.pair_cost,
                        "residual_qty": metrics.residual_qty,
                    },
                    "inventory": {
                        "settled": inventory_state_payload(&inv.settled),
                        "working": inventory_state_payload(&inv.working),
                    },
                }),
            );
        }

        summary
    }
}

fn source_truth_payload(source_truth: super::XuanB27DplusRuntimeSourceTruth) -> Value {
    json!({
        "account_truth_ready": source_truth.account_truth_ready(),
        "order_truth": source_truth.order_truth.as_str(),
        "fill_truth": source_truth.fill_truth.as_str(),
        "wallet_truth": source_truth.wallet_truth.as_str(),
        "redeem_truth": source_truth.redeem_truth.as_str(),
        "cashflow_truth": source_truth.cashflow_truth.as_str(),
        "missing_components": source_truth.missing_components(),
        "failed_components": source_truth.failed_components(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct XuanB27DplusRuntimeDispatchSummary {
    pub status: &'static str,
    pub controller_status: &'static str,
    pub adapter_status: &'static str,
    pub planner_status: &'static str,
    pub command_count: usize,
    pub sent_count: usize,
    pub orders_submitted: bool,
    pub block_reasons: Vec<&'static str>,
}

impl XuanB27DplusRuntimeDispatchSummary {
    fn blocked(reason: &'static str) -> Self {
        Self {
            status: "BLOCKED",
            controller_status: "NOT_EVALUATED",
            adapter_status: "NOT_EVALUATED",
            planner_status: "NOT_EVALUATED",
            command_count: 0,
            sent_count: 0,
            orders_submitted: false,
            block_reasons: vec![reason],
        }
    }
}

fn xuan_b27_dplus_g2_risk_caps_ready(cfg: &XuanB27DplusStrategyConfig) -> bool {
    cfg.post_only
        && !cfg.allow_passive_taker
        && cfg.max_active_markets == 1
        && cfg.max_live_orders > 0
        && cfg.max_live_orders <= 2
        && cfg.target_qty > 0.0
        && cfg.target_qty <= 5.0
        && cfg.max_open_cost_usdc > 0.0
        && cfg.max_open_cost_usdc <= 100.0
}

fn push_unique_reason(out: &mut Vec<&'static str>, value: &'static str) {
    if !out.contains(&value) {
        out.push(value);
    }
}

fn xuan_b27_dplus_market_filter_matches(filter: &str, market_slug: &str) -> bool {
    let filter = filter.trim();
    if filter.is_empty() {
        return true;
    }
    market_slug == filter || market_slug.starts_with(&format!("{filter}-"))
}

fn build_xuan_b27_dplus_observer_payload(
    cfg: &XuanB27DplusStrategyConfig,
    tick_size: f64,
    tick_seq: u64,
    market_enabled: bool,
    market_session_id: &str,
    candidate_prefix: &str,
    inv: &InventorySnapshot,
    book: &Book,
    metrics: &StrategyInventoryMetrics,
) -> Value {
    let yes_candidate_id = format!("{}:yes", candidate_prefix);
    let no_candidate_id = format!("{}:no", candidate_prefix);
    let yes_trace =
        xuan_b27_dplus_order_attempt_trace(candidate_prefix, market_session_id, &yes_candidate_id);
    let no_trace =
        xuan_b27_dplus_order_attempt_trace(candidate_prefix, market_session_id, &no_candidate_id);
    let yes_candidate = xuan_b27_dplus_side_candidate(
        "yes",
        book.yes_bid,
        book.yes_ask,
        cfg,
        tick_size,
        market_enabled,
        &yes_candidate_id,
        &yes_trace,
    );
    let no_candidate = xuan_b27_dplus_side_candidate(
        "no",
        book.no_bid,
        book.no_ask,
        cfg,
        tick_size,
        market_enabled,
        &no_candidate_id,
        &no_trace,
    );

    json!({
            "schema_version": 1,
            "strategy": StrategyKind::XuanB27Dplus.as_str(),
            "mode": cfg.mode.as_str(),
            "tick_seq": tick_seq,
            "correlation": {
                "market_session_id": market_session_id,
                "candidate_prefix": candidate_prefix,
                "order_attempt_preview_only": true,
                "preview_order_attempt_ids": [
                    yes_trace.order_attempt_id,
                    no_trace.order_attempt_id
                ],
            },
            "source_of_truth_gate": source_of_truth_gate_payload(
                candidate_prefix,
                market_session_id,
                &yes_candidate_id,
                &no_candidate_id,
            ),
            "market_enabled": market_enabled,
            "quotes_forced_empty": true,
            "orders_sent_by_this_module": false,
            "order_controller_implemented": false,
            "mode_allows_order_submission": cfg.mode.allows_order_submission(),
            "safety": {
                "post_only": cfg.post_only,
                "allow_passive_taker": cfg.allow_passive_taker,
                "stop_on_unknown": cfg.stop_on_unknown,
                "canary_block_reason": if cfg.mode == XuanB27DplusMode::Canary {
                    "rust_order_controller_not_implemented"
                } else {
                    "observer_mode"
                },
            },
            "config": {
                "edge": cfg.edge,
                "target_qty": cfg.target_qty,
                "seed_px_lo": cfg.seed_px_lo,
                "seed_px_hi": cfg.seed_px_hi,
                "imbalance_qty_cap": cfg.imbalance_qty_cap,
                "salvage_net_cap": cfg.salvage_net_cap,
                "max_open_cost_usdc": cfg.max_open_cost_usdc,
                "max_strategy_exposure_usdc": cfg.max_strategy_exposure_usdc,
                "max_active_markets": cfg.max_active_markets,
                "max_live_orders": cfg.max_live_orders,
            },
            "book": {
                "yes_bid": book.yes_bid,
                "yes_ask": book.yes_ask,
                "no_bid": book.no_bid,
                "no_ask": book.no_ask,
            },
            "observer_candidates": [yes_candidate, no_candidate],
            "inventory": {
                "settled": inventory_state_payload(&inv.settled),
                "working": inventory_state_payload(&inv.working),
                "pending_yes_qty": inv.pending_yes_qty,
                "pending_no_qty": inv.pending_no_qty,
                "fragile": inv.fragile,
            },
            "metrics": {
                "paired_qty": metrics.paired_qty,
                "pair_cost": metrics.pair_cost,
                "paired_locked_pnl": metrics.paired_locked_pnl,
                "total_spent": metrics.total_spent,
                "worst_case_outcome_pnl": metrics.worst_case_outcome_pnl,
                "dominant_side": side_label(metrics.dominant_side),
                "residual_qty": metrics.residual_qty,
                "residual_inventory_value": metrics.residual_inventory_value,
            },
            "pair_ledger": pair_ledger_payload(&inv.pair_ledger),
            "observer_note": "D+ runtime scaffold only: records passive/passive candidate state and force-clears quotes.",
    })
}

fn source_of_truth_gate_payload(
    run_id: &str,
    market_session_id: &str,
    yes_candidate_id: &str,
    no_candidate_id: &str,
) -> Value {
    let yes =
        XuanB27DplusCorrelationIds::with_candidate(run_id, market_session_id, yes_candidate_id);
    let no = XuanB27DplusCorrelationIds::with_candidate(run_id, market_session_id, no_candidate_id);
    let redeem = XuanB27DplusCorrelationIds {
        run_id: Some(run_id.to_string()),
        market_session_id: Some(market_session_id.to_string()),
        ..Default::default()
    };

    json!({
        "verdict": XuanB27DplusTruthVerdict::Unknown.as_str(),
        "candidate_only_is_unknown": true,
        "yes_order_fill_missing_fields": yes.missing_order_fill_truth_fields(),
        "no_order_fill_missing_fields": no.missing_order_fill_truth_fields(),
        "redeem_cashflow_missing_fields": redeem.missing_redeem_cashflow_truth_fields(),
        "required_before_shadow_promotion": [
            "order_attempt_id",
            "client_order_id",
            "venue_order_id",
            "fill_event_id",
            "trade_id",
            "lot_id",
            "redeem_attempt_id",
            "redeem_tx_hash",
            "cashflow_snapshot_id"
        ],
    })
}

fn xuan_b27_dplus_side_candidate(
    side: &str,
    bid: f64,
    ask: f64,
    cfg: &XuanB27DplusStrategyConfig,
    tick_size: f64,
    market_enabled: bool,
    candidate_id: &str,
    trace: &OrderAttemptTrace,
) -> Value {
    let has_ask = ask.is_finite() && ask > 0.0;
    let observer_price = if has_ask {
        quantize_down_to_tick(ask - cfg.edge, tick_size)
    } else {
        0.0
    };
    let in_band = has_ask
        && observer_price >= cfg.seed_px_lo
        && observer_price <= cfg.seed_px_hi
        && observer_price > 0.0
        && observer_price < 1.0;
    let post_only_ok = !has_ask || observer_price + 1e-9 < ask;
    let would_track = market_enabled && in_band && post_only_ok;
    let blocked_reason = if !market_enabled {
        "market_slug_filter"
    } else if !has_ask {
        "missing_ask"
    } else if !in_band {
        "price_outside_seed_band"
    } else if !post_only_ok {
        "post_only_cross"
    } else {
        "none"
    };

    json!({
        "candidate_id": candidate_id,
        "side": side,
        "book_bid": bid,
        "book_ask": ask,
        "observer_price": observer_price,
        "target_qty": cfg.target_qty,
        "pricing_model": "ask_minus_edge_observer",
        "would_track": would_track,
        "would_place": false,
        "blocked_reason": blocked_reason,
        "order_attempt_trace_preview": order_attempt_trace_preview_payload(trace),
    })
}

fn xuan_b27_dplus_order_attempt_trace(
    run_id: &str,
    market_session_id: &str,
    candidate_id: &str,
) -> OrderAttemptTrace {
    OrderAttemptTrace {
        strategy: StrategyKind::XuanB27Dplus.as_str().to_string(),
        run_id: run_id.to_string(),
        market_session_id: market_session_id.to_string(),
        candidate_id: candidate_id.to_string(),
        order_attempt_id: format!("{}:attempt:preview", candidate_id),
    }
}

fn order_attempt_trace_preview_payload(trace: &OrderAttemptTrace) -> Value {
    json!({
        "strategy": trace.strategy,
        "run_id": trace.run_id,
        "market_session_id": trace.market_session_id,
        "candidate_id": trace.candidate_id,
        "order_attempt_id": trace.order_attempt_id,
        "preview_only": true,
        "submitted": false,
        "venue_order_id": Value::Null,
    })
}

fn quantize_down_to_tick(price: f64, tick_size: f64) -> f64 {
    let tick = tick_size.max(1e-9);
    let max_ticks = (1.0 / tick).floor() - 1.0;
    if max_ticks < 1.0 {
        return 0.0;
    }
    let min_price = tick;
    let max_price = max_ticks * tick;
    let clamped = price.clamp(min_price, max_price);
    let steps = ((clamped / tick) + 1e-9).floor();
    (steps * tick).clamp(min_price, max_price)
}

fn inventory_state_payload(state: &InventoryState) -> Value {
    json!({
        "yes_qty": state.yes_qty,
        "no_qty": state.no_qty,
        "yes_avg_cost": state.yes_avg_cost,
        "no_avg_cost": state.no_avg_cost,
        "net_diff": state.net_diff,
        "portfolio_cost": state.portfolio_cost,
    })
}

fn pair_ledger_payload(ledger: &PairLedgerSnapshot) -> Value {
    json!({
        "active_tranche": ledger.active_tranche.map(pair_tranche_payload),
        "residual_side": side_label(ledger.residual_side),
        "residual_qty": ledger.residual_qty,
        "buy_fill_count": ledger.buy_fill_count,
        "surplus_bank": ledger.surplus_bank,
        "repair_budget_available": ledger.repair_budget_available,
        "capital_state": capital_state_payload(&ledger.capital_state),
        "recent_closed_pairable_qty": ledger
            .recent_closed
            .iter()
            .flatten()
            .map(|tranche| tranche.pairable_qty.max(0.0))
            .sum::<f64>(),
    })
}

fn pair_tranche_payload(tranche: PairTranche) -> Value {
    json!({
        "id": tranche.id,
        "state": format!("{:?}", tranche.state),
        "first_side": side_label(tranche.first_side),
        "first_qty": tranche.first_qty,
        "first_vwap": tranche.first_vwap,
        "hedge_qty": tranche.hedge_qty,
        "hedge_vwap": tranche.hedge_vwap,
        "residual_qty": tranche.residual_qty,
        "pairable_qty": tranche.pairable_qty,
        "pair_cost_tranche": tranche.pair_cost_tranche,
        "gross_surplus": tranche.gross_surplus,
        "spendable_surplus": tranche.spendable_surplus,
        "repair_spent": tranche.repair_spent,
        "same_side_add_count": tranche.same_side_add_count,
        "path_kind": format!("{:?}", tranche.path_kind),
    })
}

fn capital_state_payload(state: &CapitalState) -> Value {
    json!({
        "working_capital": state.working_capital,
        "locked_in_active_tranches": state.locked_in_active_tranches,
        "locked_in_pair_covered": state.locked_in_pair_covered,
        "mergeable_full_sets": state.mergeable_full_sets,
        "locked_capital_ratio": state.locked_capital_ratio,
        "would_block_new_open_due_to_capital": state.would_block_new_open_due_to_capital,
        "would_trigger_merge_due_to_capital": state.would_trigger_merge_due_to_capital,
        "capital_pressure_merge_batch_shadow": state.capital_pressure_merge_batch_shadow,
    })
}

fn side_label(side: Option<Side>) -> Option<&'static str> {
    side.map(|side| match side {
        Side::Yes => "yes",
        Side::No => "no",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::coordinator::CoordinatorConfig;

    fn zero_metrics() -> StrategyInventoryMetrics {
        StrategyInventoryMetrics {
            paired_qty: 0.0,
            pair_cost: 0.0,
            paired_locked_pnl: 0.0,
            total_spent: 0.0,
            worst_case_outcome_pnl: 0.0,
            dominant_side: None,
            residual_qty: 0.0,
            residual_inventory_value: 0.0,
        }
    }

    #[test]
    fn xuan_b27_dplus_market_filter_accepts_5m_slug_prefix() {
        assert!(xuan_b27_dplus_market_filter_matches(
            "btc-updown-5m",
            "btc-updown-5m-1778842800"
        ));
        assert!(xuan_b27_dplus_market_filter_matches(
            "btc-updown-5m-1778842800",
            "btc-updown-5m-1778842800"
        ));
        assert!(!xuan_b27_dplus_market_filter_matches(
            "btc-updown-5m",
            "eth-updown-5m-1778842800"
        ));
    }

    #[test]
    fn xuan_b27_dplus_observer_payload_is_correlated_and_no_order() {
        let mut cfg = CoordinatorConfig::default().xuan_b27_dplus;
        cfg.mode = XuanB27DplusMode::Observer;
        let book = Book {
            yes_bid: 0.42,
            yes_ask: 0.46,
            no_bid: 0.52,
            no_ask: 0.56,
        };
        let payload = build_xuan_b27_dplus_observer_payload(
            &cfg,
            0.01,
            17,
            true,
            "btc-updown-5m:cond",
            "xuan_b27_dplus:btc-updown-5m:17",
            &InventorySnapshot::default(),
            &book,
            &zero_metrics(),
        );

        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["strategy"], "xuan_b27_dplus");
        assert_eq!(payload["mode"], "observer");
        assert_eq!(payload["tick_seq"], 17);
        assert_eq!(payload["quotes_forced_empty"], true);
        assert_eq!(payload["orders_sent_by_this_module"], false);
        assert_eq!(payload["order_controller_implemented"], false);
        assert_eq!(
            payload["correlation"]["candidate_prefix"],
            "xuan_b27_dplus:btc-updown-5m:17"
        );
        assert_eq!(payload["correlation"]["order_attempt_preview_only"], true);
        assert_eq!(
            payload["correlation"]["preview_order_attempt_ids"][0],
            "xuan_b27_dplus:btc-updown-5m:17:yes:attempt:preview"
        );
        assert_eq!(payload["source_of_truth_gate"]["verdict"], "UNKNOWN");
        assert_eq!(
            payload["source_of_truth_gate"]["candidate_only_is_unknown"],
            true
        );
        assert_eq!(
            payload["source_of_truth_gate"]["yes_order_fill_missing_fields"][0],
            "order_attempt_id"
        );
        assert_eq!(
            payload["source_of_truth_gate"]["redeem_cashflow_missing_fields"][0],
            "pair_id"
        );
        assert_eq!(
            payload["observer_candidates"][0]["candidate_id"],
            "xuan_b27_dplus:btc-updown-5m:17:yes"
        );
        assert_eq!(
            payload["observer_candidates"][0]["order_attempt_trace_preview"]["order_attempt_id"],
            "xuan_b27_dplus:btc-updown-5m:17:yes:attempt:preview"
        );
        assert_eq!(
            payload["observer_candidates"][0]["order_attempt_trace_preview"]["preview_only"],
            true
        );
        assert_eq!(
            payload["observer_candidates"][0]["order_attempt_trace_preview"]["submitted"],
            false
        );
        assert_eq!(payload["observer_candidates"][0]["would_place"], false);
        assert_eq!(payload["observer_candidates"][0]["would_track"], true);
        assert_eq!(payload["observer_candidates"][0]["observer_price"], 0.42);
        assert_eq!(payload["observer_candidates"][1]["observer_price"], 0.52);
    }

    #[test]
    fn xuan_b27_dplus_observer_payload_blocks_wrong_market() {
        let mut cfg = CoordinatorConfig::default().xuan_b27_dplus;
        cfg.mode = XuanB27DplusMode::Observer;
        let book = Book {
            yes_bid: 0.42,
            yes_ask: 0.46,
            no_bid: 0.52,
            no_ask: 0.56,
        };
        let payload = build_xuan_b27_dplus_observer_payload(
            &cfg,
            0.01,
            18,
            false,
            "other:cond",
            "xuan_b27_dplus:other:18",
            &InventorySnapshot::default(),
            &book,
            &zero_metrics(),
        );

        assert_eq!(payload["market_enabled"], false);
        assert_eq!(payload["observer_candidates"][0]["would_track"], false);
        assert_eq!(
            payload["observer_candidates"][0]["blocked_reason"],
            "market_slug_filter"
        );
    }
}
