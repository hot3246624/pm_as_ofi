use anyhow::Result;
use dotenv::dotenv;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

use std::sync::Arc;
use mev_backrun_rs_cu::pool_syncer::{self, PoolSyncer};
use alloy_provider::{Provider, ProviderBuilder};
use mev_backrun_rs_cu::{admin, AppState};
use tokio::{sync::RwLock, task};
use std::net::SocketAddr;
use alloy_primitives::Address;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    info!("Runner (Alloy) 启动");

    let rpc_url = std::env::var("BASE_RPC_URL")
        .or_else(|_| std::env::var("RPC_URL"))
        .expect("请在 .env 中设置 BASE_RPC_URL 或 RPC_URL 环境变量");
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let provider_arc = Arc::new(provider);

    // -- 状态管理重构 --
    // 1. 创建共享的 AppState 和 PoolSyncer
    let state = AppState { monitor_tokens: Arc::new(RwLock::new(Vec::<Address>::new())) };
    
    let config = pool_syncer::load_config(8453);
    let syncer = PoolSyncer::new(config, provider_arc.clone());
    let syncer_arc = Arc::new(RwLock::new(syncer));

    // 2. 启动 Admin API，传入共享的 syncer
    if let Ok(admin_addr) = std::env::var("ADMIN_ADDR") {
        if let Ok(sock) = admin_addr.parse::<SocketAddr>() {
            let state_clone = state.clone();
            let syncer_clone = syncer_arc.clone();
            task::spawn(async move { admin::serve_admin(state_clone, syncer_clone, sock).await; });
            info!("Admin API started at {}", admin_addr);
        }
    }

    // 3. 执行初始同步
    {
        let mut syncer_guard = syncer_arc.write().await;
        // 添加备用 RPC
        if let Ok(fallback_urls) = std::env::var("FALLBACK_RPC_URLS") {
            let fallback_providers: Result<Vec<_>, _> = fallback_urls
                .split(',')
                .map(|url| url.trim().parse().map(|parsed_url| ProviderBuilder::new().connect_http(parsed_url)))
                .collect();
            if let Ok(fallbacks) = fallback_providers {
                let fallback_arcs: Vec<_> = fallbacks.into_iter().map(|p| Arc::new(p)).collect();
                *syncer_guard = syncer_guard.clone().with_fallback_providers(fallback_arcs);
                info!("已配置 {} 个备用 RPC 提供者", syncer_guard.fallback_providers.len());
            }
        }
        syncer_guard.sync_pools().await?;
        
        let pools = syncer_guard.get_pools();
        info!("初始同步完成，共 {} 个池", pools.len());
        for (addr, pool) in pools {
            info!("  - 池地址: {}", addr);
            info!("    协议: {:?}", pool.protocol);
            info!("    Token0: {}", pool.token0);
            info!("    Token1: {}", pool.token1);
            match &pool.amm_data {
                mev_backrun_rs_cu::AmmData::V3(data) => {
                    info!("    V3 Data: sqrt_price={}, tick={}, liquidity={}, fee={}",
                            data.sqrt_price_x96, data.tick, data.liquidity, data.fee);
                },
                mev_backrun_rs_cu::AmmData::V2(data) => {
                    info!("    V2 Data: reserve0={}, reserve1={}",
                            data.reserve0, data.reserve1);
                },
                mev_backrun_rs_cu::AmmData::Balancer(_) => {
                    info!("    Balancer Data: (reserves and weights are currently placeholders)");
                },
                mev_backrun_rs_cu::AmmData::Fluid(_) => {
                    info!("    Fluid Data: (data is currently a placeholder)");
                },
                mev_backrun_rs_cu::AmmData::Aerodrome(data) => {
                    info!("    Aerodrome V3 Data: sqrt_price={}, tick={}, liquidity={}, fee={}",
                            data.sqrt_price_x96, data.tick, data.liquidity, data.fee);
                },
                mev_backrun_rs_cu::AmmData::SushiSwapV3(data) => {
                    info!("    SushiSwap V3 Data: sqrt_price={}, tick={}, liquidity={}, fee={}",
                            data.sqrt_price_x96, data.tick, data.liquidity, data.fee);
                },
                mev_backrun_rs_cu::AmmData::PancakeV2(data) => {
                    info!("    PancakeV2 Data: reserve0={}, reserve1={}",
                            data.reserve0, data.reserve1);
                },
                mev_backrun_rs_cu::AmmData::PancakeV3(data) => {
                    info!("    PancakeV3 Data: sqrt_price={}, tick={}, liquidity={}, fee={}",
                            data.sqrt_price_x96, data.tick, data.liquidity, data.fee);
                },
            }
        }
    }

    // 4. 保持进程存活以服务 Admin API
    info!("初始化完成，等待 Admin API 的热更新通知...");
    futures::future::pending::<()>().await;

    Ok(())
}
