use anyhow::Result;
use alloy_primitives::{aliases::{I24, U24}, Address, U256};
use alloy_provider::Provider;
use alloy_eips::eip1898::BlockId;
use amms_rs::amms::{amm::AutomatedMarketMaker, uniswap_v3::UniswapV3Pool, balancer::BalancerPool};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, info, warn};
use alloy_sol_types::sol;
use tokio::time::sleep;

use crate::{AmmData, Config, PoolState, Protocol, UniswapV3Data};

#[derive(Clone)]
pub struct PoolSyncer<P>
where
    P: Provider + Clone + 'static,
{
    config: Config,
    pools: HashMap<String, PoolState>,
    provider: Arc<P>,
    pub fallback_providers: Vec<Arc<P>>,
    monitor_tokens: Option<Vec<Address>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct FactoryConfig {
    protocol: Protocol,
    address: String,
    #[serde(default)]
    fee_tiers: Vec<u32>,
}

impl<P> PoolSyncer<P>
where
    P: Provider + Clone + 'static,
{
    pub fn new(config: Config, provider: Arc<P>) -> Self {
        Self {
            config,
            pools: HashMap::new(),
            provider,
            fallback_providers: vec![],
            monitor_tokens: None,
        }
    }

    pub fn with_fallback_providers(mut self, fallbacks: Vec<Arc<P>>) -> Self {
        self.fallback_providers = fallbacks;
        self
    }

    async fn call_with_retry<F, T>(&self, mut call_fn: F) -> Result<T>
    where
        F: FnMut(&Arc<P>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + '_>>,
    {
        let mut providers = vec![self.provider.clone()];
        providers.extend(self.fallback_providers.clone());
        
        let mut last_error = None;
        for (attempt, provider) in providers.iter().enumerate() {
            match call_fn(provider).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < providers.len() - 1 {
                        let delay = Duration::from_millis(100 * (2_u64.pow(attempt as u32).min(8)));
                        warn!("Provider {} 调用失败，{}ms 后重试: {}", attempt, delay.as_millis(), last_error.as_ref().unwrap());
                        sleep(delay).await;
                    }
                }
            }
        }
        Err(last_error.unwrap())
    }

    pub async fn sync_pools(&mut self) -> Result<()> {
        debug!("开始同步所有配置的池...");

        self.sync_uniswap_v3_pools().await?;
        self.sync_balancer_pools().await?;
        self.sync_v2_pools().await?;
        self.sync_aerodrome_pools().await?;
        self.sync_pancake_v3_pools().await?;
        self.sync_sushiswap_v3_pools().await?;
        // ENV 驱动的自动发现
        self.discover_v3_pools_from_env().await?;
        self.discover_v2_pools_from_env().await?;

        info!("所有池同步完成，总池数量: {}", self.pools.len());
        Ok(())
    }

    pub async fn discover_v3_pools_from_env(&mut self) -> Result<()> {
        use serde_json::from_str;
        // 优先使用内部状态，否则回退到 ENV
        let monitor_tokens_as_str: Vec<String> = if let Some(tokens) = &self.monitor_tokens {
            tokens.iter().map(|a| format!("{:?}", a)).collect()
        } else {
            match std::env::var("MONITOR_TOKENS") {
                Ok(v) => match from_str(&v) { Ok(xs) => xs, Err(e) => { warn!("解析 MONITOR_TOKENS 失败: {}", e); vec![] } },
                Err(_) => vec![],
            }
        };

        let factories: Vec<FactoryConfig> = match std::env::var("FACTORIES_JSON") {
            Ok(v) => match from_str(&v) { Ok(xs) => xs, Err(e) => { warn!("解析 FACTORIES_JSON 失败: {}", e); vec![] } },
            Err(_) => vec![],
        };
        if monitor_tokens_as_str.len() < 2 || factories.is_empty() {
            debug!("发现跳过：MONITOR_TOKENS 少于2个或 FACTORIES_JSON 为空");
            return Ok(());
        }
        // 生成 token 对组合
        let mut token_addrs: Vec<Address> = Vec::with_capacity(monitor_tokens_as_str.len());
        for s in &monitor_tokens_as_str {
            if let Ok(a) = s.parse() { token_addrs.push(a); } else { warn!("无效监控资产地址: {}", s); }
        }
        if token_addrs.len() < 2 { return Ok(()); }

        sol! {
            #[sol(rpc)]
            interface IGenericV3FactoryUint {
                function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address);
            }
        }

        info!("开始 ENV 驱动的 V3 池发现: tokens={} factories={}", token_addrs.len(), factories.len());
        for f in factories {
            let Ok(factory_addr) = f.address.parse::<Address>() else { warn!("无效工厂地址: {}", f.address); continue; };
            match f.protocol {
                Protocol::Aerodrome => {
                    // 使用已存在的 IAerodromeFactory (int24)
                    sol! { #[sol(rpc)] interface IAerodromeFactory { function getPool(address tokenA, address tokenB, int24 fee) external view returns (address); } }
                    let factory = IAerodromeFactory::new(factory_addr, self.provider.clone());
                    let fee_tiers = if f.fee_tiers.is_empty() { vec![100u32, 500u32, 3000u32, 10000u32] } else { f.fee_tiers.clone() };
                    for i in 0..token_addrs.len() {
                        for j in i+1..token_addrs.len() {
                            let a = token_addrs[i];
                            let b = token_addrs[j];
                            for fee in &fee_tiers {
                                let call = factory.getPool(a, b, I24::from_limbs([*fee as u64]));
                                if let Ok(ret) = call.call().await {
                                    if !ret.0.is_zero() {
                                        let pool_addr: Address = Address::from(ret.0);
                                        self.insert_v3_like_pool(pool_addr, Protocol::Aerodrome, *fee).await;
                                    }
                                }
                            }
                        }
                    }
                }
                Protocol::UniswapV3 | Protocol::SushiSwapV3 | Protocol::PancakeV3 => {
                    let factory = IGenericV3FactoryUint::new(factory_addr, self.provider.clone());
                    let fee_tiers = if f.fee_tiers.is_empty() { vec![100u32, 500u32, 3000u32, 10000u32] } else { f.fee_tiers.clone() };
                    for i in 0..token_addrs.len() {
                        for j in i+1..token_addrs.len() {
                            let a = token_addrs[i];
                            let b = token_addrs[j];
                            for fee in &fee_tiers {
                                let call = factory.getPool(a, b, U24::from(*fee));
                                if let Ok(ret) = call.call().await {
                                    if !ret.0.is_zero() {
                                        let pool_addr: Address = Address::from(ret.0);
                                        self.insert_v3_like_pool(pool_addr, f.protocol.clone(), *fee).await;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => { /* 非 V3 协议，忽略 */ }
            }
        }
        info!("ENV 驱动的 V3 池发现完成。目前池数量: {}", self.pools.len());
        Ok(())
    }

    pub async fn discover_v2_pools_from_env(&mut self) -> Result<()> {
        use serde_json::from_str;
        sol! {
            #[sol(rpc)]
            interface IUniswapV2Factory {
                function getPair(address tokenA, address tokenB) external view returns (address);
            }
        }
        // 优先使用内部状态，否则回退到 ENV
        let monitor_tokens_as_str: Vec<String> = if let Some(tokens) = &self.monitor_tokens {
            tokens.iter().map(|a| format!("{:?}", a)).collect()
        } else {
            match std::env::var("MONITOR_TOKENS") {
                Ok(v) => match from_str(&v) { Ok(xs) => xs, Err(e) => { warn!("解析 MONITOR_TOKENS 失败: {}", e); vec![] } },
                Err(_) => vec![],
            }
        };
        let factories: Vec<FactoryConfig> = match std::env::var("FACTORIES_JSON") {
            Ok(v) => match from_str(&v) { Ok(xs) => xs, Err(e) => { warn!("解析 FACTORIES_JSON 失败: {}", e); vec![] } },
            Err(_) => vec![],
        };
        if monitor_tokens_as_str.len() < 2 || factories.is_empty() {
            debug!("V2 发现跳过：MONITOR_TOKENS 少于2个或 FACTORIES_JSON 为空");
            return Ok(());
        }
        let mut token_addrs: Vec<Address> = Vec::with_capacity(monitor_tokens_as_str.len());
        for s in &monitor_tokens_as_str {
            if let Ok(a) = s.parse() { token_addrs.push(a); } else { warn!("无效监控资产地址: {}", s); }
        }
        if token_addrs.len() < 2 { return Ok(()); }

        info!("开始 ENV 驱动的 V2 池发现: tokens={} factories={}", token_addrs.len(), factories.len());
        for f in factories {
            if !matches!(f.protocol, Protocol::UniswapV2 | Protocol::SushiSwap | Protocol::PancakeV2) {
                continue;
            }
            let Ok(factory_addr) = f.address.parse::<Address>() else { warn!("无效 V2 工厂地址: {}", f.address); continue; };
            let _factory = IUniswapV2Factory::new(factory_addr, self.provider.clone());
            for i in 0..token_addrs.len() {
                for j in i+1..token_addrs.len() {
                    let a = token_addrs[i];
                    let b = token_addrs[j];
                    match self.call_with_retry(|provider| {
                        let factory = IUniswapV2Factory::new(factory_addr, provider.clone());
                        Box::pin(async move { factory.getPair(a, b).call().await.map_err(|e| anyhow::Error::from(e)) })
                    }).await {
                        Ok(ret) => {
                            if !ret.0.is_zero() {
                                let pair_addr: Address = Address::from(ret.0);
                                self.insert_v2_pool(pair_addr, f.protocol.clone()).await;
                            }
                        }
                        Err(e) => {
                            if e.to_string().contains("returned no data") {
                                // 这是预期的“未找到”情况，当池不存在时发生。
                                // 降低日志级别或完全忽略，以避免日志刷屏。
                                debug!("V2 pair not found on factory {} ({:?}), which is expected.", f.address, f.protocol);
                            } else {
                                // 这是意外错误，需要关注。
                                warn!("V2 工厂 {} 查询失败 ({:?}): {}", f.address, f.protocol, e);
                            }
                        }
                    }
                }
            }
        }
        info!("ENV 驱动的 V2 池发现完成。目前池数量: {}", self.pools.len());
        Ok(())
    }

    async fn insert_v2_pool(&mut self, pair_address: Address, protocol: Protocol) {
        if self.pools.contains_key(&format!("{:?}", pair_address)) { return; }
        sol! {
            #[sol(rpc)]
            interface IUniswapV2Pair {
                function token0() external view returns (address);
                function token1() external view returns (address);
                function getReserves() external view returns (uint112, uint112, uint32);
            }
        }
        match self.call_with_retry(|provider| {
            Box::pin(async move {
                let pair = IUniswapV2Pair::new(pair_address, provider.clone());
                let t0b = pair.token0();
                let t1b = pair.token1();
                let rb = pair.getReserves();
                tokio::try_join!(t0b.call(), t1b.call(), rb.call())
                    .map_err(|e| anyhow::Error::from(e))
            })
        }).await {
            Ok((t0r, t1r, rr)) => {
                let reserve0 = U256::from(rr._0);
                let reserve1 = U256::from(rr._1);
                let ps = PoolState {
                    address: pair_address,
                    protocol: protocol.clone(),
                    token0: t0r.0.into(),
                    token1: t1r.0.into(),
                    amm_data: AmmData::V2(crate::UniswapV2Data { reserve0, reserve1 }),
                };
                self.pools.insert(format!("{:?}", pair_address), ps);
                info!("发现并加入 V2 池: {:?} (protocol={:?})", pair_address, protocol);
                    }
                    Err(e) => {
                warn!("V2 池 {} 状态获取失败，跳过: {}", pair_address, e);
            }
        }
    }

    async fn insert_v3_like_pool(&mut self, pool_address: Address, protocol: Protocol, fee: u32) {
        // 已存在则跳过
        if self.pools.contains_key(&format!("{:?}", pool_address)) { return; }
        sol! { #[sol(rpc)] interface IUniswapV3PoolMinimal { function token0() external view returns (address); function token1() external view returns (address); function slot0() external view returns (uint160 sqrtPriceX96, int24 tick); function liquidity() external view returns (uint128); } }
        let pool = IUniswapV3PoolMinimal::new(pool_address, self.provider.clone());
        // Use builders to extend lifetime
        let t0_builder = pool.token0();
        let t1_builder = pool.token1();
        let s0_builder = pool.slot0();
        let liq_builder = pool.liquidity();
        let t0 = t0_builder.call();
        let t1 = t1_builder.call();
        let s0 = s0_builder.call();
        let liq = liq_builder.call();
        if let Ok((t0r, t1r, s0r, liqr)) = tokio::try_join!(t0, t1, s0, liq) {
            let protocol_for_state = protocol.clone();
            let ps = PoolState {
                address: pool_address,
                protocol: protocol_for_state,
                token0: t0r.0.into(),
                token1: t1r.0.into(),
                amm_data: match protocol {
                    Protocol::Aerodrome => AmmData::Aerodrome(UniswapV3Data { sqrt_price_x96: U256::from(s0r.sqrtPriceX96), tick: s0r.tick.as_i32(), liquidity: liqr, fee }),
                    Protocol::SushiSwapV3 => AmmData::SushiSwapV3(UniswapV3Data { sqrt_price_x96: U256::from(s0r.sqrtPriceX96), tick: s0r.tick.as_i32(), liquidity: liqr, fee }),
                    Protocol::PancakeV3 => AmmData::PancakeV3(UniswapV3Data { sqrt_price_x96: U256::from(s0r.sqrtPriceX96), tick: s0r.tick.as_i32(), liquidity: liqr, fee }),
                    _ => AmmData::V3(UniswapV3Data { sqrt_price_x96: U256::from(s0r.sqrtPriceX96), tick: s0r.tick.as_i32(), liquidity: liqr, fee }),
                },
            };
            self.pools.insert(format!("{:?}", pool_address), ps);
            info!("发现并加入 V3 池: {:?} (protocol={:?}, fee={})", pool_address, protocol, fee);
        }
    }

    async fn sync_v2_pools(&mut self) -> Result<()> {
        // 使用 alloy 直读，以避免 amms-rs V2 的未实现 panic
        sol! {
            #[sol(rpc)]
            interface IUniswapV2Pair {
                function token0() external view returns (address);
                function token1() external view returns (address);
                function getReserves() external view returns (uint112, uint112, uint32);
            }
        }

        let v2_protocols = [Protocol::SushiSwap, Protocol::UniswapV2, Protocol::PancakeV2];
        let v2_pools: Vec<_> = self.config.pools.iter()
            .filter(|p| v2_protocols.contains(&p.protocol))
            .cloned()
            .collect();

        if v2_pools.is_empty() {
            debug!("配置中未找到 V2-style 池，跳过同步。");
            return Ok(());
        }

        info!("开始同步 {} 个 V2-style 池 (alloy 直读)...", v2_pools.len());

        for pool_config in v2_pools {
            let Ok(addr) = pool_config.address.parse::<Address>() else { warn!("无效 V2 池地址: {}", pool_config.address); continue; };
            match self.call_with_retry(|provider| {
                Box::pin(async move {
                    let pair = IUniswapV2Pair::new(addr, provider.clone());
                    let t0_builder = pair.token0();
                    let t1_builder = pair.token1();
                    let r_builder = pair.getReserves();
                    tokio::try_join!(t0_builder.call(), t1_builder.call(), r_builder.call())
                        .map_err(|e| anyhow::Error::from(e))
                })
            }).await {
                Ok((t0r, t1r, rr)) => {
                    let reserve0 = U256::from(rr._0);
                    let reserve1 = U256::from(rr._1);
                    let ps = PoolState {
                        address: addr,
                        protocol: pool_config.protocol.clone(),
                        token0: t0r.0.into(),
                        token1: t1r.0.into(),
                        amm_data: AmmData::V2(crate::UniswapV2Data { reserve0, reserve1 }),
                    };
                    self.pools.insert(format!("{:?}", addr), ps);
                    info!("成功同步 V2 池: {} ({:?})", pool_config.name, pool_config.protocol);
                }
                Err(e) => {
                    warn!("同步 V2 池失败 {}: {}", pool_config.address, e);
                }
            }
        }
        Ok(())
    }

    async fn sync_uniswap_v3_pools(&mut self) -> Result<()> {
        let v3_pools: Vec<_> = self.config.pools.iter()
            .filter(|pool| pool.protocol == Protocol::UniswapV3)
            .collect();

        if v3_pools.is_empty() {
            debug!("配置中未找到 UniswapV3 池，跳过同步。");
            return Ok(());
        }

        info!("从配置中找到 {} 个 UniswapV3 池", v3_pools.len());
        
        for pool_config in v3_pools {
            if let Ok(addr) = pool_config.address.parse::<Address>() {
                match UniswapV3Pool::new(addr)
                    .init(BlockId::latest(), self.provider.clone())
                    .await
                {
                    Ok(v3) => {
                        let ps = PoolState {
                            address: v3.address,
                            protocol: Protocol::UniswapV3,
                            token0: v3.token_a.address,
                            token1: v3.token_b.address,
                            amm_data: AmmData::V3(UniswapV3Data {
                                sqrt_price_x96: v3.sqrt_price,
                                tick: v3.tick,
                                liquidity: v3.liquidity,
                                fee: v3.fee,
                            }),
                        };
                        self.pools.insert(format!("{:?}", v3.address), ps);
                        info!("成功同步 UniswapV3 池: {} ({})", pool_config.name, pool_config.address);
                    }
                    Err(e) => {
                        warn!("同步 UniswapV3 池失败 {}: {}", pool_config.address, e);
                    }
                }
            }
        }
        Ok(())
    }

    async fn sync_balancer_pools(&mut self) -> Result<()> {
        let balancer_pools: Vec<_> = self.config.pools.iter()
            .filter(|pool| pool.protocol == Protocol::Balancer)
            .collect();

        if balancer_pools.is_empty() {
            debug!("配置中未找到 Balancer 池，跳过同步。");
            return Ok(());
        }

        info!("从配置中找到 {} 个 Balancer 池", balancer_pools.len());
        
        for pool_config in balancer_pools {
            if let Ok(addr) = pool_config.address.parse::<Address>() {
                match BalancerPool::new(addr)
                    .init(BlockId::latest(), self.provider.clone())
                    .await
                {
                    Ok(balancer) => {
                        let token0_addr: Address = match pool_config.token0.parse() {
                            Ok(addr) => addr,
                            Err(e) => {
                                warn!("解析 Balancer 池 {} 的 token0 地址失败: {}", pool_config.name, e);
                                continue;
                            }
                        };
                        let token1_addr: Address = match pool_config.token1.parse() {
                            Ok(addr) => addr,
                            Err(e) => {
                                warn!("解析 Balancer 池 {} 的 token1 地址失败: {}", pool_config.name, e);
                                continue;
                            }
                        };

                        let ps = PoolState {
                            address: balancer.address(),
                            protocol: Protocol::Balancer,
                            token0: token0_addr,
                            token1: token1_addr,
                            amm_data: AmmData::Balancer(balancer),
                        };
                        self.pools.insert(format!("{:?}", ps.address), ps);
                        info!("成功同步 Balancer 池: {} ({})", pool_config.name, pool_config.address);
                    }
                    Err(e) => {
                        warn!("同步 Balancer 池失败 {}: {}", pool_config.address, e);
                    }
                }
            }
        }
        Ok(())
    }

    async fn sync_aerodrome_pools(&mut self) -> Result<()> {
        sol! {
            #[sol(rpc)]
            interface IAerodromeFactory {
                function getPool(address tokenA, address tokenB, int24 fee) external view returns (address);
            }
            #[sol(rpc)]
            interface IUniswapV3PoolMinimal {
                function token0() external view returns (address);
                function token1() external view returns (address);
                function slot0() external view returns (uint160 sqrtPriceX96, int24 tick);
                function liquidity() external view returns (uint128);
            }
        }

        let factory_address: Address = "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A".parse()?;
        let factory = IAerodromeFactory::new(factory_address, self.provider.clone());

        let pools_configs: Vec<_> = self.config.pools.iter()
            .filter(|p| p.protocol == Protocol::Aerodrome)
            .cloned()
            .collect();

        if pools_configs.is_empty() {
            debug!("配置中未找到 Aerodrome 池，跳过同步。");
            return Ok(());
        }

        info!("开始同步 {} 个 Aerodrome 池...", pools_configs.len());

        for pool_config in pools_configs {
            let t0_str = &pool_config.token0;
            let t1_str = &pool_config.token1;

            let token0_addr: Address = match t0_str.parse() { Ok(addr) => addr, Err(e) => { warn!("(Aerodrome) 无效的 token0 地址 '{}' for pool '{}': {}", t0_str, pool_config.name, e); continue; } };
            let token1_addr: Address = match t1_str.parse() { Ok(addr) => addr, Err(e) => { warn!("(Aerodrome) 无效的 token1 地址 '{}' for pool '{}': {}", t1_str, pool_config.name, e); continue; } };
            let fee = match pool_config.fee { Some(f) => f, None => { warn!("Aerodrome 池 '{}' 未在配置中指定 'fee'，已跳过。", pool_config.name); continue; } };

            let get_pool_builder = factory.getPool(token0_addr, token1_addr, I24::from_limbs([fee as u64]));
            let pool_address_res = match get_pool_builder.call().await {
                Ok(result) => { if result.0.is_zero() { warn!("Aerodrome factory 未找到池: {} ({} / {}, fee {})", pool_config.name, t0_str, t1_str, fee); continue; } result.0 },
                Err(e) => { warn!("调用 Aerodrome factory getPool 失败 for {}: {}", pool_config.name, e); continue; }
            };
            let pool_address = Address::from(pool_address_res);

            info!("Aerodrome factory resolved pool '{}' to address {}", pool_config.name, pool_address);
            let pool_contract = IUniswapV3PoolMinimal::new(pool_address, self.provider.clone());

            let token0_builder = pool_contract.token0();
            let token1_builder = pool_contract.token1();
            let slot0_builder = pool_contract.slot0();
            let liquidity_builder = pool_contract.liquidity();

            let token0_call = token0_builder.call();
            let token1_call = token1_builder.call();
            let slot0_call = slot0_builder.call();
            let liquidity_call = liquidity_builder.call();

            match tokio::try_join!( token0_call, token1_call, slot0_call, liquidity_call ) {
                Ok((token0_res, token1_res, slot0_res, liquidity_res)) => {
                    let mut actual_token0: Address = token0_res.0.into();
                    let mut actual_token1: Address = token1_res.0.into();
                    if actual_token0 > actual_token1 { std::mem::swap(&mut actual_token0, &mut actual_token1); }

                    let ps = PoolState {
                        address: pool_address,
                        protocol: Protocol::Aerodrome,
                        token0: actual_token0,
                        token1: actual_token1,
                        amm_data: AmmData::Aerodrome(UniswapV3Data {
                            sqrt_price_x96: U256::from(slot0_res.sqrtPriceX96),
                            tick: slot0_res.tick.as_i32(),
                            liquidity: liquidity_res,
                            fee,
                        }),
                    };
                    self.pools.insert(format!("{:?}", pool_address), ps);
                    info!("成功同步 Aerodrome(V3 适配) 池: {} ({})", pool_config.name, pool_address);
                }
                Err(e) => { warn!("同步 Aerodrome 池 {} ({}) 状态失败: {}", pool_config.name, pool_address, e); }
            }
        }
        info!("Aerodrome 池同步完成。");
        Ok(())
    }

    async fn sync_pancake_v3_pools(&mut self) -> Result<()> {
        sol! {
            #[sol(rpc)]
            interface IPancakeV3Factory {
                function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address);
            }
            #[sol(rpc)]
            interface IUniswapV3PoolMinimal {
                function token0() external view returns (address);
                function token1() external view returns (address);
                function slot0() external view returns (uint160 sqrtPriceX96, int24 tick);
                function liquidity() external view returns (uint128);
            }
        }

        let factory_address: Address = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865".parse()?;
        let factory = IPancakeV3Factory::new(factory_address, self.provider.clone());

        let pools_configs: Vec<_> = self.config.pools.iter()
            .filter(|p| p.protocol == Protocol::PancakeV3)
            .cloned()
            .collect();

        if pools_configs.is_empty() {
            debug!("配置中未找到 PancakeV3 池，跳过同步。");
            return Ok(());
        }

        info!("开始同步 {} 个 PancakeV3 池...", pools_configs.len());

        for pool_config in pools_configs {
            let t0_str = &pool_config.token0;
            let t1_str = &pool_config.token1;

            let token0_addr: Address = match t0_str.parse() { Ok(addr) => addr, Err(e) => { warn!("(PancakeV3) 无效的 token0 地址 '{}' for pool '{}': {}", t0_str, pool_config.name, e); continue; } };
            let token1_addr: Address = match t1_str.parse() { Ok(addr) => addr, Err(e) => { warn!("(PancakeV3) 无效的 token1 地址 '{}' for pool '{}': {}", t1_str, pool_config.name, e); continue; } };
            let fee = match pool_config.fee { Some(f) => f, None => { warn!("PancakeV3 池 '{}' 未在配置中指定 'fee'，已跳过。", pool_config.name); continue; } };

            let get_pool_builder = factory.getPool(token0_addr, token1_addr, U24::from(fee));
            let pool_address_res = match get_pool_builder.call().await {
                Ok(result) => { if result.0.is_zero() { warn!("PancakeV3 factory 未找到池: {} ({} / {}, fee {})", pool_config.name, t0_str, t1_str, fee); continue; } result.0 },
                Err(e) => { warn!("调用 PancakeV3 factory getPool 失败 for {}: {}", pool_config.name, e); continue; }
            };
            let pool_address = Address::from(pool_address_res);

            info!("PancakeV3 factory resolved pool '{}' to address {}", pool_config.name, pool_address);
            let pool_contract = IUniswapV3PoolMinimal::new(pool_address, self.provider.clone());

            let token0_builder = pool_contract.token0();
            let token1_builder = pool_contract.token1();
            let slot0_builder = pool_contract.slot0();
            let liquidity_builder = pool_contract.liquidity();

            let token0_call = token0_builder.call();
            let token1_call = token1_builder.call();
            let slot0_call = slot0_builder.call();
            let liquidity_call = liquidity_builder.call();

            match tokio::try_join!( token0_call, token1_call, slot0_call, liquidity_call ) {
                Ok((token0_res, token1_res, slot0_res, liquidity_res)) => {
                    let mut actual_token0: Address = token0_res.0.into();
                    let mut actual_token1: Address = token1_res.0.into();
                    if actual_token0 > actual_token1 { std::mem::swap(&mut actual_token0, &mut actual_token1); }

                    let ps = PoolState {
                        address: pool_address,
                        protocol: Protocol::PancakeV3,
                        token0: actual_token0,
                        token1: actual_token1,
                        amm_data: AmmData::PancakeV3(UniswapV3Data {
                            sqrt_price_x96: U256::from(slot0_res.sqrtPriceX96),
                            tick: slot0_res.tick.as_i32(),
                            liquidity: liquidity_res,
                            fee,
                        }),
                    };
                    self.pools.insert(format!("{:?}", pool_address), ps);
                    info!("成功同步 PancakeV3(V3 适配) 池: {} ({})", pool_config.name, pool_address);
                }
                Err(e) => { warn!("同步 PancakeV3 池 {} ({}) 状态失败: {}", pool_config.name, pool_address, e); }
            }
        }
        info!("PancakeV3 池同步完成。");
        Ok(())
    }

    async fn sync_sushiswap_v3_pools(&mut self) -> Result<()> {
        sol! {
            #[sol(rpc)]
            interface IUniswapV3PoolMinimal {
                function token0() external view returns (address);
                function token1() external view returns (address);
                function slot0() external view returns (uint160 sqrtPriceX96, int24 tick);
                function liquidity() external view returns (uint128);
            }
        }

        let pools_configs: Vec<_> = self.config.pools.iter()
            .filter(|p| p.protocol == Protocol::SushiSwapV3)
            .cloned()
            .collect();

        if pools_configs.is_empty() {
            debug!("配置中未找到 SushiSwapV3 池，跳过同步。");
            return Ok(());
        }

        info!("开始同步 {} 个 SushiSwapV3 池...", pools_configs.len());

        for pool_config in pools_configs {
            let pool_address: Address = match pool_config.address.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    warn!("(SushiSwapV3) 无效的池地址 '{}' for pool '{}': {}", pool_config.address, pool_config.name, e);
                    continue;
                }
            };

            let fee = match pool_config.fee {
                Some(f) => f,
                None => {
                    warn!("SushiSwapV3 池 '{}' 未在配置中指定 'fee'，已跳过。", pool_config.name);
                    continue;
                }
            };
            
            let pool_contract = IUniswapV3PoolMinimal::new(pool_address, self.provider.clone());

            let token0_builder = pool_contract.token0();
            let token1_builder = pool_contract.token1();
            let slot0_builder = pool_contract.slot0();
            let liquidity_builder = pool_contract.liquidity();

            match tokio::try_join!(
                token0_builder.call(),
                token1_builder.call(),
                slot0_builder.call(),
                liquidity_builder.call()
            ) {
                Ok((token0_res, token1_res, slot0_res, liquidity_res)) => {
                    let ps = PoolState {
                        address: pool_address,
                        protocol: Protocol::SushiSwapV3,
                        token0: token0_res.0.into(),
                        token1: token1_res.0.into(),
                        amm_data: AmmData::SushiSwapV3(UniswapV3Data {
                            sqrt_price_x96: U256::from(slot0_res.sqrtPriceX96),
                            tick: slot0_res.tick.as_i32(),
                            liquidity: liquidity_res,
                            fee,
                        }),
                    };
                    self.pools.insert(format!("{:?}", pool_address), ps);
                    info!("成功同步 SushiSwapV3 池: {} ({})", pool_config.name, pool_address);
                }
                Err(e) => {
                    warn!("同步 SushiSwapV3 池 {} ({}) 状态失败: {}", pool_config.name, pool_address, e);
                }
            }
        }
        info!("SushiSwapV3 池同步完成。");
        Ok(())
    }

    pub fn get_pools(&self) -> HashMap<String, PoolState> {
        self.pools.clone()
    }

    pub fn set_monitor_tokens(&mut self, tokens: Vec<Address>) {
        info!("PoolSyncer 内部监控资产列表已更新，数量: {}", tokens.len());
        self.monitor_tokens = Some(tokens);
    }
}

pub fn load_config(chain_id: u64) -> Config {
    use serde_json;
    
    if let Ok(config_json) = std::env::var("CONFIG_JSON") {
        if let Ok(config) = serde_json::from_str::<crate::Config>(&config_json) {
            info!("从 CONFIG_JSON 环境变量加载了配置。");
            return config;
        }
    }
    
    let env_chain_id = std::env::var("CHAIN_ID")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(chain_id);
    
    let mut tokens_loaded = false;
    let mut pools_loaded = false;
    let mut tokens = vec![];
    if let Ok(tokens_json) = std::env::var("TOKENS_JSON") {
        if let Ok(parsed_tokens) = serde_json::from_str::<Vec<crate::TokenConfig>>(&tokens_json) {
            tokens = parsed_tokens;
            tokens_loaded = true;
        }
    }
    
    let mut pools = vec![];
    if let Ok(pools_json) = std::env::var("POOLS_JSON") {
        if let Ok(parsed_pools) = serde_json::from_str::<Vec<crate::PoolConfig>>(&pools_json) {
            pools = parsed_pools;
            pools_loaded = true;
        }
    }
    
    if tokens_loaded || pools_loaded {
        info!("从 TOKENS_JSON 和 POOLS_JSON 环境变量加载了配置。");
        return Config {
            chain_id: env_chain_id,
            tokens,
            pools,
        };
    }
    
    info!("未找到环境变量配置，加载 chain_id {} 的默认配置。", chain_id);
    match chain_id {
        8453 => {
            Config {
                chain_id: 8453,
                tokens: vec![
                    crate::TokenConfig {
                        name: "WETH".to_string(),
                        address: "0x4200000000000000000000000000000000000006".to_string(),
                        decimals: 18,
                    },
                    crate::TokenConfig {
                        name: "USDC".to_string(),
                        address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".to_string(),
                        decimals: 6,
                    },
                ],
                pools: vec![
                    crate::PoolConfig {
                        name: "UniswapV3: WETH/USDC".to_string(),
                        address: "0xd0b53d9277642d899df5c87a3966a349a798f224".to_string(),
                        token0: "0x4200000000000000000000000000000000000006".to_string(),
                        token1: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".to_string(),
                        protocol: crate::Protocol::UniswapV3,
                        fee: Some(500),
                    },
                    crate::PoolConfig {
                        name: "SushiSwap V2: WETH/USDC".to_string(),
                        address: "0x2f8818d1b0f3e3e295440c1c0cddf40aaa21fa87".to_string(), // Correct V2 address
                        token0: "0x4200000000000000000000000000000000000006".to_string(),
                        token1: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".to_string(),
                        protocol: crate::Protocol::SushiSwap,
                        fee: None,
                    },
                    crate::PoolConfig {
                        name: "SushiSwap V3: WETH/USDC".to_string(),
                        address: "0x57713f7716e0b0f65ec116912f834e49805480d2".to_string(), // Correct V3 address
                        token0: "0x4200000000000000000000000000000000000006".to_string(),
                        token1: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".to_string(),
                        protocol: crate::Protocol::SushiSwapV3,
                        fee: Some(100), // Common 0.01% fee for stable pairs
                    },
                    crate::PoolConfig {
                        name: "Aerodrome: WETH/USDC".to_string(),
                        address: "".to_string(),
                        token0: "0x4200000000000000000000000000000000000006".to_string(),
                        token1: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".to_string(),
                        protocol: crate::Protocol::Aerodrome,
                        fee: Some(100),
                    },
                    crate::PoolConfig {
                        name: "PancakeV2: WETH/USDC".to_string(),
                        address: "0x832353533b53439A1355A38312A56C95A9453943".to_string(), // Placeholder address
                        token0: "0x4200000000000000000000000000000000000006".to_string(),
                        token1: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".to_string(),
                        protocol: crate::Protocol::PancakeV2,
                        fee: None,
                    },
                    crate::PoolConfig {
                        name: "PancakeV3: WETH/USDC".to_string(),
                        address: "".to_string(),
                        token0: "0x4200000000000000000000000000000000000006".to_string(),
                        token1: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".to_string(),
                        protocol: crate::Protocol::PancakeV3,
                        fee: Some(100), // Example fee tier
                    },
                ],
            }
        },
        1 => Config { chain_id: 1, tokens: vec![], pools: vec![] },
        _ => {
            warn!("未找到 chain_id {} 的默认配置，将使用空配置。", chain_id);
            Config { chain_id, tokens: vec![], pools: vec![] }
        }
    }
}
