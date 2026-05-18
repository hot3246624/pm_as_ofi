use std::collections::VecDeque;

use super::types::Side;

const LOT_EPS: f64 = 1e-9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XuanB27DplusFillRole {
    Unknown,
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct XuanB27DplusFill {
    pub side: Side,
    pub qty: f64,
    pub price: f64,
    pub fee: f64,
    pub role: XuanB27DplusFillRole,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct XuanB27DplusLot {
    pub id: u64,
    pub side: Side,
    pub qty: f64,
    pub price: f64,
    pub fee: f64,
    pub role: XuanB27DplusFillRole,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct XuanB27DplusPair {
    pub yes_lot_id: u64,
    pub no_lot_id: u64,
    pub qty: f64,
    pub gross_pair_cost: f64,
    pub net_pair_cost: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct XuanB27DplusLedgerSnapshot {
    pub paired_qty: f64,
    pub total_pair_cost: f64,
    pub total_fee: f64,
    pub yes_residual_qty: f64,
    pub no_residual_qty: f64,
    pub residual_cost: f64,
    pub gross_pair_cost_avg: f64,
    pub net_pair_cost_avg: f64,
}

#[derive(Debug, Default)]
pub struct XuanB27DplusLedger {
    next_lot_id: u64,
    yes_lots: VecDeque<XuanB27DplusLot>,
    no_lots: VecDeque<XuanB27DplusLot>,
    paired_qty: f64,
    total_pair_cost: f64,
    total_fee: f64,
}

impl XuanB27DplusLedger {
    pub fn record_fill(&mut self, fill: XuanB27DplusFill) -> Vec<XuanB27DplusPair> {
        if fill.qty <= LOT_EPS || fill.price <= 0.0 || fill.price >= 1.0 {
            return Vec::new();
        }
        let lot = XuanB27DplusLot {
            id: self.allocate_lot_id(),
            side: fill.side,
            qty: fill.qty,
            price: fill.price,
            fee: fill.fee.max(0.0),
            role: fill.role,
        };
        match lot.side {
            Side::Yes => self.yes_lots.push_back(lot),
            Side::No => self.no_lots.push_back(lot),
        }
        self.pair_fifo()
    }

    pub fn snapshot(&self) -> XuanB27DplusLedgerSnapshot {
        let yes_residual_qty = self
            .yes_lots
            .iter()
            .map(|lot| lot.qty.max(0.0))
            .sum::<f64>();
        let no_residual_qty = self.no_lots.iter().map(|lot| lot.qty.max(0.0)).sum::<f64>();
        let residual_cost = self
            .yes_lots
            .iter()
            .chain(self.no_lots.iter())
            .map(|lot| (lot.qty * lot.price + lot.fee).max(0.0))
            .sum::<f64>();
        let gross_pair_cost_avg = if self.paired_qty > LOT_EPS {
            self.total_pair_cost / self.paired_qty
        } else {
            0.0
        };
        let net_pair_cost_avg = if self.paired_qty > LOT_EPS {
            (self.total_pair_cost + self.total_fee) / self.paired_qty
        } else {
            0.0
        };
        XuanB27DplusLedgerSnapshot {
            paired_qty: self.paired_qty,
            total_pair_cost: self.total_pair_cost,
            total_fee: self.total_fee,
            yes_residual_qty,
            no_residual_qty,
            residual_cost,
            gross_pair_cost_avg,
            net_pair_cost_avg,
        }
    }

    fn allocate_lot_id(&mut self) -> u64 {
        let id = self.next_lot_id;
        self.next_lot_id = self.next_lot_id.saturating_add(1);
        id
    }

    fn pair_fifo(&mut self) -> Vec<XuanB27DplusPair> {
        let mut pairs = Vec::new();
        while let (Some(yes), Some(no)) = (self.yes_lots.front_mut(), self.no_lots.front_mut()) {
            let qty = yes.qty.min(no.qty);
            if qty <= LOT_EPS {
                break;
            }
            let yes_fee = prorated_fee(*yes, qty);
            let no_fee = prorated_fee(*no, qty);
            let gross_pair_cost = yes.price + no.price;
            let net_pair_cost = gross_pair_cost + (yes_fee + no_fee) / qty;

            pairs.push(XuanB27DplusPair {
                yes_lot_id: yes.id,
                no_lot_id: no.id,
                qty,
                gross_pair_cost,
                net_pair_cost,
            });
            self.paired_qty += qty;
            self.total_pair_cost += qty * gross_pair_cost;
            self.total_fee += yes_fee + no_fee;
            yes.qty -= qty;
            no.qty -= qty;
            yes.fee = (yes.fee - yes_fee).max(0.0);
            no.fee = (no.fee - no_fee).max(0.0);

            if self
                .yes_lots
                .front()
                .map(|lot| lot.qty <= LOT_EPS)
                .unwrap_or(false)
            {
                self.yes_lots.pop_front();
            }
            if self
                .no_lots
                .front()
                .map(|lot| lot.qty <= LOT_EPS)
                .unwrap_or(false)
            {
                self.no_lots.pop_front();
            }
        }
        pairs
    }
}

fn prorated_fee(lot: XuanB27DplusLot, qty: f64) -> f64 {
    if lot.qty <= LOT_EPS {
        0.0
    } else {
        (lot.fee * (qty / lot.qty)).max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(side: Side, qty: f64, price: f64) -> XuanB27DplusFill {
        XuanB27DplusFill {
            side,
            qty,
            price,
            fee: 0.0,
            role: XuanB27DplusFillRole::Maker,
        }
    }

    #[test]
    fn xuan_b27_dplus_ledger_pairs_fifo_and_tracks_residual() {
        let mut ledger = XuanB27DplusLedger::default();
        assert!(ledger.record_fill(fill(Side::Yes, 3.0, 0.45)).is_empty());

        let pairs = ledger.record_fill(fill(Side::No, 2.0, 0.46));
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].qty, 2.0);
        assert_eq!(pairs[0].gross_pair_cost, 0.91);

        let snap = ledger.snapshot();
        assert_eq!(snap.paired_qty, 2.0);
        assert_eq!(snap.yes_residual_qty, 1.0);
        assert_eq!(snap.no_residual_qty, 0.0);
        assert!((snap.residual_cost - 0.45).abs() < 1e-9);
        assert!((snap.gross_pair_cost_avg - 0.91).abs() < 1e-9);

        let pairs = ledger.record_fill(fill(Side::No, 1.0, 0.47));
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].yes_lot_id, 0);
        assert_eq!(pairs[0].no_lot_id, 2);
        assert_eq!(pairs[0].qty, 1.0);

        let snap = ledger.snapshot();
        assert_eq!(snap.paired_qty, 3.0);
        assert_eq!(snap.yes_residual_qty, 0.0);
        assert_eq!(snap.no_residual_qty, 0.0);
        assert_eq!(snap.residual_cost, 0.0);
        assert!((snap.gross_pair_cost_avg - ((2.0 * 0.91 + 0.92) / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn xuan_b27_dplus_ledger_prorates_fee_into_net_pair_cost() {
        let mut ledger = XuanB27DplusLedger::default();
        ledger.record_fill(XuanB27DplusFill {
            side: Side::Yes,
            qty: 2.0,
            price: 0.40,
            fee: 0.02,
            role: XuanB27DplusFillRole::Maker,
        });
        let pairs = ledger.record_fill(XuanB27DplusFill {
            side: Side::No,
            qty: 1.0,
            price: 0.50,
            fee: 0.01,
            role: XuanB27DplusFillRole::Taker,
        });

        assert_eq!(pairs.len(), 1);
        assert!((pairs[0].net_pair_cost - 0.92).abs() < 1e-9);
        let snap = ledger.snapshot();
        assert_eq!(snap.paired_qty, 1.0);
        assert!((snap.total_fee - 0.02).abs() < 1e-9);
        assert!((snap.residual_cost - 0.41).abs() < 1e-9);
    }
}
