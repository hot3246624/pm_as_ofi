use axum::{routing::{get, post}, Router, extract::State, Json};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use crate::AppState;
use alloy_primitives::Address;
use tracing::info;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::pool_syncer::PoolSyncer;

#[derive(Serialize, Deserialize)]
pub struct MonitorTokensBody {
    pub tokens: Vec<String>,
}

pub async fn serve_admin<P>(state: AppState, syncer: Arc<RwLock<PoolSyncer<P>>>, addr: SocketAddr)
where
    P: alloy_provider::Provider + Clone + 'static,
{
    let app = Router::new()
        .route("/monitor-tokens", get(get_monitor_tokens).post(update_monitor_tokens::<P>))
        .with_state((state, syncer));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_monitor_tokens(State((state, _)): State<(AppState, Arc<RwLock<PoolSyncer<impl alloy_provider::Provider + Clone + 'static>>>)>) -> Json<Vec<String>> {
    let tokens = state.monitor_tokens.read().await;
    let token_strs = tokens.iter().map(|a| format!("{:?}", a)).collect();
    Json(token_strs)
}

async fn update_monitor_tokens<P>(
    State((state, syncer_arc)): State<(AppState, Arc<RwLock<PoolSyncer<P>>>)>,
    Json(body): Json<MonitorTokensBody>
) -> Json<usize>
where
    P: alloy_provider::Provider + Clone + 'static,
{
    let mut new_tokens = Vec::new();
    for s in body.tokens {
        if let Ok(addr) = s.parse::<Address>() {
            new_tokens.push(addr);
        }
    }
    let count = new_tokens.len();
    let tokens_for_task = new_tokens.clone();

    {
        let mut tokens_guard = state.monitor_tokens.write().await;
        *tokens_guard = new_tokens;
    }
    info!("收到监控资产更新通知，触发重新发现与同步。新监控资产数量: {}", count);

    // 在后台触发重新同步
    let syncer_clone = syncer_arc.clone();
    tokio::spawn(async move {
        let mut syncer = syncer_clone.write().await;
        // 使用更新后的监控资产重新运行发现
        syncer.set_monitor_tokens(tokens_for_task);
        if let Err(e) = syncer.discover_v3_pools_from_env().await {
            tracing::error!("Admin API 触发 V3 池重新发现失败: {:?}", e);
        }
        if let Err(e) = syncer.discover_v2_pools_from_env().await {
            tracing::error!("Admin API 触发 V2 池重新发现失败: {:?}", e);
        }
        info!("Admin API 触发的池重新发现与同步完成。当前总池数量: {}", syncer.get_pools().len());
    });

    Json(count)
}


