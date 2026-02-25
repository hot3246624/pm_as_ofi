use alloy_primitives::{Address, U256};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use tracing::debug;

use crate::{AmmData, Path, PoolState, Swap, SwapSimulationResult};

// 暂时不使用 amms-rs，实现基于 sqrtPriceX96 的真实 V3 模拟

#[derive(Clone)]
pub struct PathEvaluator {
    pools: HashMap<String, PoolState>,
    // 闪电贷可借资产列表（默认起点）
    flashloan_assets: HashSet<Address>,
}

impl PathEvaluator {
    pub fn new(pools: &HashMap<String, PoolState>) -> Self {
        Self {
            pools: pools.clone(),
            flashloan_assets: HashSet::new(),
        }
    }

    pub fn new_with_flashloan_assets(
        pools: &HashMap<String, PoolState>,
        flashloan_assets: HashSet<Address>,
    ) -> Self {
        Self {
            pools: pools.clone(),
            flashloan_assets,
        }
    }

    pub fn find_paths_states(
        &self,
        start_token: Address,
        end_token: Address,
        max_hops: usize,
    ) -> Vec<Vec<PoolState>> {
        let mut all_paths = Vec::new();
        self.dfs_find_paths_states(
            start_token,
            end_token,
            &mut Vec::new(),
            &mut all_paths,
            &mut HashSet::new(),
            max_hops,
        );
        all_paths
    }

    /// 查找从闪电贷资产到目标代币的路径（优先使用闪电贷起点）
    pub fn find_flashloan_paths_states(
        &self,
        end_token: Address,
        max_hops: usize,
    ) -> Vec<Vec<PoolState>> {
        let mut all_paths = Vec::new();

        // 如果目标代币本身就是闪电贷资产，直接返回空路径（无需套利）
        if self.flashloan_assets.contains(&end_token) {
            return all_paths;
        }

        // 尝试从每个闪电贷资产开始寻找路径
        for &start_token in &self.flashloan_assets {
            let mut paths_from_start = Vec::new();
            self.dfs_find_paths_states(
                start_token,
                end_token,
                &mut Vec::new(),
                &mut paths_from_start,
                &mut HashSet::new(),
                max_hops,
            );
            all_paths.extend(paths_from_start);
        }

        all_paths
    }

    fn dfs_find_paths_states(
        &self,
        current_token: Address,
        end_token: Address,
        current_path: &mut Vec<PoolState>,
        all_paths: &mut Vec<Vec<PoolState>>,
        visited_pools: &mut HashSet<Address>,
        hops_left: usize,
    ) {
        if current_token == end_token && !current_path.is_empty() {
            all_paths.push(current_path.clone());
            return;
        }
        if hops_left == 0 {
            return;
        }

        for pool in self.pools.values() {
            if visited_pools.contains(&pool.address) {
                continue;
            }
            let next_token = if pool.token0 == current_token {
                pool.token1
            } else if pool.token1 == current_token {
                pool.token0
            } else {
                continue;
            };

            visited_pools.insert(pool.address);
            current_path.push(pool.clone());
            self.dfs_find_paths_states(
                next_token,
                end_token,
                current_path,
                all_paths,
                visited_pools,
                hops_left - 1,
            );
            current_path.pop();
            visited_pools.remove(&pool.address);
        }
    }

    pub async fn evaluate_path(
        &self,
        path: &mut Path,
        amount_in: U256,
        pools: &HashMap<String, PoolState>,
    ) -> Result<()> {
        path.amount_in = Some(amount_in);
        let mut current_amount = amount_in;
        let mut temp_pools = pools.clone();

        for hop in path.hops.iter_mut() {
            let pool_state = temp_pools
                .get(&hop.address.to_string())
                .ok_or_else(|| anyhow::anyhow!("Pool not found in temp map"))?;

            let sim_result = self
                .simulate_hop(pool_state, current_amount, hop.token_in)
                .await?;
            current_amount = sim_result.amount_out;
            hop.amount_out = Some(current_amount);
            temp_pools.insert(hop.address.to_string(), sim_result.updated_pool);
        }

        path.estimated_output = Some(current_amount);
        Ok(())
    }

    async fn simulate_hop(
        &self,
        pool_state: &PoolState,
        amount_in: U256,
        token_in: Address,
    ) -> Result<SwapSimulationResult> {
        match &pool_state.amm_data {
            AmmData::V3(data) => {
                debug!("模拟 V3 池: {} (fee: {})", pool_state.address, data.fee);

                debug!("使用改进的 V3 模拟算法 - 考虑流动性影响");

                // 基于 sqrtPriceX96 和流动性的更精确计算
                let sqrt_price = data.sqrt_price_x96;
                let liquidity = data.liquidity;
                let zero_for_one = token_in == pool_state.token0;

                // 计算基础价格 (简化)
                let price_ratio = if zero_for_one {
                    // token0 -> token1: 基于 sqrtPriceX96
                    let sqrt_price_scaled = sqrt_price / U256::from(10).pow(U256::from(12));
                    let price_scaled = (sqrt_price_scaled * sqrt_price_scaled)
                        / U256::from(10).pow(U256::from(24));
                    price_scaled
                } else {
                    // token1 -> token0: 反向计算
                    let sqrt_price_scaled = sqrt_price / U256::from(10).pow(U256::from(12));
                    let price_scaled = (sqrt_price_scaled * sqrt_price_scaled)
                        / U256::from(10).pow(U256::from(24));
                    U256::from(10).pow(U256::from(48)) / price_scaled
                };

                // 考虑手续费
                let fee_multiplier = U256::from(10000) - U256::from(data.fee);
                let amount_in_after_fee = (amount_in * fee_multiplier) / U256::from(10000);

                // 计算价格影响 - 基于输入数量相对于流动性的比例
                let liquidity_u256 = U256::from(liquidity);

                // 计算流动性比率 (输入数量 / 流动性)
                // 这里需要更仔细的计算，避免除零和溢出
                let liquidity_ratio = if liquidity_u256 > U256::from(0u64) {
                    (amount_in_after_fee * U256::from(10000)) / liquidity_u256
                } else {
                    U256::from(0u64)
                };

                debug!(
                    "价格影响计算: amount_in_after_fee={}, liquidity={}, liquidity_ratio={}",
                    amount_in_after_fee, liquidity_u256, liquidity_ratio
                );

                // 价格影响计算 - 基于流动性比率
                let price_impact_bps = if liquidity_ratio > U256::from(1000) {
                    // 大额交易，显著的价格影响 (5% + 额外影响)
                    U256::from(500) + ((liquidity_ratio - U256::from(1000)) / U256::from(50))
                } else if liquidity_ratio > U256::from(100) {
                    // 中等交易，适度的价格影响 (1% + 额外影响)
                    U256::from(100) + ((liquidity_ratio - U256::from(100)) / U256::from(20))
                } else if liquidity_ratio > U256::from(10) {
                    // 小额交易，轻微的价格影响 (0.1% + 额外影响)
                    U256::from(10) + ((liquidity_ratio - U256::from(10)) / U256::from(100))
                } else {
                    // 极小交易，最小价格影响
                    liquidity_ratio / U256::from(1000)
                };

                // 确保价格影响在合理范围内 (0-1000 bps = 0-10%)
                let price_impact_bps = if price_impact_bps > U256::from(1000) {
                    U256::from(1000)
                } else if price_impact_bps < U256::from(1) {
                    U256::from(1) // 至少 0.01% 的价格影响
                } else {
                    price_impact_bps
                };

                debug!("计算的价格影响: {} bps", price_impact_bps);

                // 应用价格影响 - 大额交易应该有更差的价格
                let price_impact_multiplier = U256::from(10000) - price_impact_bps;
                let adjusted_price_ratio =
                    (price_ratio * price_impact_multiplier) / U256::from(10000);

                // 计算输出数量
                let amount_out = (amount_in_after_fee * adjusted_price_ratio)
                    / U256::from(10).pow(U256::from(18));

                debug!("改进的 V3 模拟结果: {} -> {} (sqrtPrice: {}, liquidity: {}, price_impact: {} bps)", 
                       amount_in, amount_out, sqrt_price, liquidity, price_impact_bps);

                Ok(SwapSimulationResult {
                    amount_out,
                    updated_pool: pool_state.clone(),
                })
            }
            _ => pool_state
                .simulate_swap(amount_in, token_in)
                .ok_or_else(|| {
                    anyhow::anyhow!("Local simulation failed for pool {}", pool_state.address)
                }),
        }
    }

    pub async fn simulate_swap(
        &self,
        swap: &Swap,
        pool_state: &PoolState,
    ) -> Result<SwapSimulationResult> {
        let (token_in, amount_in) = match swap {
            Swap::V2 {
                path, amount_in, ..
            } => (path[0], *amount_in),
            Swap::V3 {
                token_in,
                amount_in,
                ..
            } => (*token_in, *amount_in),
        };
        self.simulate_hop(pool_state, amount_in, token_in).await
    }
}
