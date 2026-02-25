// pub mod flashbots; // temporarily disabled during alloy migration
#[cfg(feature = "amms")]
pub mod admin;
pub mod gamma_http;
#[cfg(feature = "amms")]
pub mod path_evaluator;
pub mod polymarket;
#[cfg(feature = "amms")]
pub mod pool_syncer;
use std::sync::Arc;
use tokio::sync::RwLock;

use alloy_primitives::{Address, U256};
#[cfg(feature = "amms")]
use amms_rs::amms::balancer::BalancerPool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --- Configuration Structs ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub chain_id: u64,
    pub tokens: Vec<TokenConfig>,
    pub pools: Vec<PoolConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenConfig {
    pub name: String,
    pub address: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub name: String,
    pub address: String,
    pub token0: String,
    pub token1: String,
    pub protocol: Protocol,
    pub fee: Option<u32>, // Add this line
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Protocol {
    UniswapV2,
    UniswapV3,
    SushiSwap,
    SushiSwapV3, // Add this
    Balancer,
    Fluid,
    Aerodrome,
    PancakeV2,
    PancakeV3,
}

// --- AMM Data & State Structs ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapV2Data {
    pub reserve0: U256,
    pub reserve1: U256,
}

#[derive(Debug, Clone)]
pub struct UniswapV3Data {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    pub fee: u32,
}

#[derive(Debug, Clone)]
pub struct BalancerData {
    pub pool_id: [u8; 32],
    pub reserves: HashMap<Address, U256>,
    pub weights: HashMap<Address, U256>,
    pub swap_fee: U256,
}

#[derive(Debug, Clone)]
pub struct FluidData {
    pub reserves: HashMap<Address, U256>,
}

#[derive(Debug, Clone)]
pub enum AmmData {
    V2(UniswapV2Data),
    V3(UniswapV3Data),
    SushiSwapV3(UniswapV3Data),
    #[cfg(feature = "amms")]
    Balancer(BalancerPool),
    Fluid(FluidData),
    Aerodrome(UniswapV3Data),
    PancakeV2(UniswapV2Data),
    PancakeV3(UniswapV3Data),
}

#[derive(Debug, Clone)]
pub struct PoolState {
    pub address: Address,
    pub protocol: Protocol,
    pub token0: Address,
    pub token1: Address,
    pub amm_data: AmmData,
}

#[derive(Debug, Clone)]
pub struct SwapSimulationResult {
    pub amount_out: U256,
    pub updated_pool: PoolState,
}

// --- Shared App State (admin API) ---
#[derive(Debug, Clone)]
pub struct AppState {
    pub monitor_tokens: Arc<RwLock<Vec<Address>>>,
}

impl PoolState {
    pub fn simulate_swap(
        &self,
        amount_in: U256,
        token_in: Address,
    ) -> Option<SwapSimulationResult> {
        match &self.amm_data {
            AmmData::V2(data) => {
                let (reserve_in, reserve_out) = if token_in == self.token0 {
                    (data.reserve0, data.reserve1)
                } else {
                    (data.reserve1, data.reserve0)
                };

                if reserve_in.is_zero() || reserve_out.is_zero() {
                    return None;
                }

                // Standard xy=k formula with 0.3% fee
                let amount_in_with_fee = amount_in * U256::from(997u64);
                let numerator = amount_in_with_fee * reserve_out;
                let denominator = reserve_in * U256::from(1000u64) + amount_in_with_fee;
                let amount_out = numerator / denominator;

                let new_reserve_in = reserve_in + amount_in;
                let new_reserve_out = reserve_out - amount_out;

                let updated_data = UniswapV2Data {
                    reserve0: if token_in == self.token0 {
                        new_reserve_in
                    } else {
                        new_reserve_out
                    },
                    reserve1: if token_in == self.token0 {
                        new_reserve_out
                    } else {
                        new_reserve_in
                    },
                };

                let updated_amm_data = match self.protocol {
                    Protocol::UniswapV2 => AmmData::V2(updated_data),
                    Protocol::SushiSwap => AmmData::V2(updated_data),
                    Protocol::PancakeV2 => AmmData::PancakeV2(updated_data),
                    _ => return None, // Should not happen for V2 data
                };

                Some(SwapSimulationResult {
                    amount_out,
                    updated_pool: PoolState {
                        amm_data: updated_amm_data,
                        ..self.clone()
                    },
                })
            }
            AmmData::V3(data)
            | AmmData::Aerodrome(data)
            | AmmData::PancakeV3(data)
            | AmmData::SushiSwapV3(data) => {
                // Simplified V3 simulation, does not account for ticks
                // This is a placeholder and should be replaced with a proper V3 simulation logic
                let amount_in_with_fee =
                    amount_in * U256::from(1000000 - data.fee as u64) / U256::from(1000000);
                // This is a rough estimation and not accurate
                let amount_out = amount_in_with_fee;
                Some(SwapSimulationResult {
                    amount_out,
                    updated_pool: self.clone(),
                })
            }
            #[cfg(feature = "amms")]
            AmmData::Balancer(_) => {
                // Balancer simulation is complex and not implemented yet
                None
            }
            AmmData::Fluid(_) => {
                // Fluid simulation is complex and not implemented yet
                None
            }
            AmmData::PancakeV2(data) => {
                // PancakeV2 uses the same logic as UniswapV2
                let (reserve_in, reserve_out) = if token_in == self.token0 {
                    (data.reserve0, data.reserve1)
                } else {
                    (data.reserve1, data.reserve0)
                };

                if reserve_in.is_zero() || reserve_out.is_zero() {
                    return None;
                }

                // Standard xy=k formula with 0.3% fee
                let amount_in_with_fee = amount_in * U256::from(997u64);
                let numerator = amount_in_with_fee * reserve_out;
                let denominator = reserve_in * U256::from(1000u64) + amount_in_with_fee;
                let amount_out = numerator / denominator;

                let new_reserve_in = reserve_in + amount_in;
                let new_reserve_out = reserve_out - amount_out;

                let updated_data = UniswapV2Data {
                    reserve0: if token_in == self.token0 {
                        new_reserve_in
                    } else {
                        new_reserve_out
                    },
                    reserve1: if token_in == self.token0 {
                        new_reserve_out
                    } else {
                        new_reserve_in
                    },
                };

                Some(SwapSimulationResult {
                    amount_out,
                    updated_pool: PoolState {
                        amm_data: AmmData::PancakeV2(updated_data),
                        ..self.clone()
                    },
                })
            }
        }
    }

    // Helper functions to get reserves for path evaluator
    pub fn get_reserve0(&self) -> Option<U256> {
        match &self.amm_data {
            AmmData::V2(data) => Some(data.reserve0),
            AmmData::PancakeV2(data) => Some(data.reserve0),
            _ => None,
        }
    }

    pub fn get_reserve1(&self) -> Option<U256> {
        match &self.amm_data {
            AmmData::V2(data) => Some(data.reserve1),
            AmmData::PancakeV2(data) => Some(data.reserve1),
            _ => None,
        }
    }
}

// --- Path Structs ---

#[derive(Debug, Clone)]
pub struct Hop {
    pub address: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_out: Option<U256>,
}

#[derive(Debug, Clone)]
pub struct Path {
    pub hops: Vec<Hop>,
    pub amount_in: Option<U256>,
    pub estimated_output: Option<U256>,
}

#[derive(Debug, Clone)]
pub enum Swap {
    V2 {
        amount_in: U256,
        amount_out_min: U256,
        path: Vec<Address>,
    },
    V3 {
        token_in: Address,
        token_out: Address,
        fee: u32,
        amount_in: U256,
        amount_out_min: U256,
    },
}
