use crate::polymarket::types::{
    DesiredOrder, Order, OrderAction, OrderBook, OrderEvent, OrderStatus, Side,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

pub struct OrderManager {
    open: HashMap<String, Order>,
    default_ttl: Duration,
}

impl OrderManager {
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            open: HashMap::new(),
            default_ttl,
        }
    }

    pub fn has_pending(&self) -> bool {
        self.open
            .values()
            .any(|o| o.status == OrderStatus::PendingNew || o.status == OrderStatus::PendingCancel)
    }

    pub fn open_orders(&self) -> Vec<Order> {
        self.open.values().cloned().collect()
    }

    pub fn on_order_event(&mut self, event: OrderEvent) {
        if let Some(order) = self.open.get_mut(&event.id) {
            match event.status {
                OrderStatus::Open => {
                    order.status = OrderStatus::Open;
                }
                OrderStatus::PartiallyFilled => {
                    order.status = OrderStatus::PartiallyFilled;
                    if let Some(rem) = event.remaining_qty {
                        order.remaining_qty = rem;
                    }
                }
                OrderStatus::Filled => {
                    order.status = OrderStatus::Filled;
                    order.remaining_qty = 0.0;
                }
                OrderStatus::Canceled => {
                    order.status = OrderStatus::Canceled;
                    order.remaining_qty = 0.0;
                }
                OrderStatus::Rejected => {
                    order.status = OrderStatus::Rejected;
                    order.remaining_qty = 0.0;
                }
                OrderStatus::PendingCancel => {
                    order.status = OrderStatus::PendingCancel;
                }
                OrderStatus::PendingNew => {
                    order.status = OrderStatus::PendingNew;
                }
            }
        }

        if matches!(
            event.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            self.open.remove(&event.id);
        }
    }

    pub fn sync(
        &mut self,
        desired: &[DesiredOrder],
        now: Instant,
        book: &OrderBook,
    ) -> Vec<OrderAction> {
        // 有 pending 状态时不再发新单，避免竞态
        if self.has_pending() {
            return Vec::new();
        }

        let mut actions = Vec::new();

        // 取消：过期、已不需要、或将变成 taker 的订单
        let mut to_cancel = Vec::new();
        for (id, order) in &self.open {
            if order.status != OrderStatus::Open && order.status != OrderStatus::PartiallyFilled {
                continue;
            }

            if order.is_expired(now) {
                to_cancel.push(id.clone());
                continue;
            }

            if !self.is_still_desired(order, desired) {
                to_cancel.push(id.clone());
                continue;
            }

            if !self.is_maker(order, book) {
                to_cancel.push(id.clone());
            }
        }

        for id in to_cancel {
            if let Some(order) = self.open.get_mut(&id) {
                order.status = OrderStatus::PendingCancel;
            }
            actions.push(OrderAction::Cancel { id });
        }

        // 下发缺失的目标订单
        for desired in desired {
            if self.find_matching(desired).is_none() {
                let client_id = Uuid::new_v4().to_string();
                let order = Order {
                    id: client_id.clone(),
                    side: desired.side,
                    price: desired.price,
                    qty: desired.qty,
                    remaining_qty: desired.qty,
                    status: OrderStatus::PendingNew,
                    created_at: now,
                    ttl: self.default_ttl,
                };
                self.open.insert(client_id.clone(), order);
                actions.push(OrderAction::Place {
                    client_id,
                    order: desired.clone(),
                });
            }
        }

        actions
    }

    fn is_still_desired(&self, order: &Order, desired: &[DesiredOrder]) -> bool {
        desired.iter().any(|d| self.matches(order, d))
    }

    fn find_matching(&self, desired: &DesiredOrder) -> Option<&Order> {
        self.open.values().find(|o| self.matches(o, desired))
    }

    fn matches(&self, order: &Order, desired: &DesiredOrder) -> bool {
        order.side == desired.side
            && (order.price - desired.price).abs() < 1e-9
            && (order.qty - desired.qty).abs() < 1e-9
            && (order.status == OrderStatus::Open || order.status == OrderStatus::PartiallyFilled)
    }

    fn is_maker(&self, order: &Order, book: &OrderBook) -> bool {
        match order.side {
            Side::Yes => order.price < book.yes_ask,
            Side::No => order.price < book.no_ask,
        }
    }
}
