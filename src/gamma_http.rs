// 轻量级 Gamma API HTTP 客户端
// 绕过 SDK 复杂类型，直接用 HTTP + serde_json

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

#[derive(Debug, Deserialize)]
pub struct GammaEvent {
    pub id: Option<String>,
    pub title: Option<String>,
    pub slug: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub markets: Option<Vec<GammaMarket>>,
}

#[derive(Debug, Deserialize)]
pub struct GammaMarket {
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    pub question: Option<String>,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: String,  // JSON字符串数组，如 "[\"123\", \"456\"]"
}

pub struct GammaClient {
    client: reqwest::Client,
}

impl GammaClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// 通过 slug 获取事件（支持时间相关市场）
    pub async fn get_event_by_slug(&self, slug: &str) -> Result<GammaEvent> {
        // Gamma API使用查询参数而不是路径段
        let url = format!("{}/events?slug={}", GAMMA_API_BASE, slug);
        
        let resp = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to request Gamma API")?;
        
        if !resp.status().is_success() {
            anyhow::bail!("Gamma API returned status: {}", resp.status());
        }
        
        // API返回数组，取第一个
        let events: Vec<GammaEvent> = resp.json().await
            .context("Failed to parse Gamma API response")?;
        
        events.into_iter().next()
            .ok_or_else(|| anyhow::anyhow!("No event found for slug: {}", slug))
    }

    /// 从事件中提取最新的活跃市场
    pub fn extract_latest_market(event: &GammaEvent) -> Result<&GammaMarket> {
        let markets = event.markets.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No markets in event"))?;
        
        // 对于时间相关市场，通常最后一个是最新的
        markets.last()
            .ok_or_else(|| anyhow::anyhow!("No markets found"))
    }

    /// 从市场中提取 YES/NO token IDs
    pub fn extract_tokens(market: &GammaMarket) -> Result<(String, String)> {
        // clobTokenIds 是JSON字符串数组: "[\"yes_id\", \"no_id\"]"
        let token_ids: Vec<String> = serde_json::from_str(&market.clob_token_ids)
            .context("Failed to parse clobTokenIds")?;
        
        if token_ids.len() < 2 {
            anyhow::bail!("Expected at least 2 token IDs, got {}", token_ids.len());
        }
        
        // 通常第一个是YES（Up），第二个是NO（Down）
        Ok((token_ids[0].clone(), token_ids[1].clone()))
    }
}

// Note: 实际测试需要连接真实API，建议在集成测试时手动验证
// cargo run --bin polymarket_mm with POLYMARKET_MARKET_SLUG set
