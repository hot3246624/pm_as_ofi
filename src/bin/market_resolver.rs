// 市场解析函数：使用 Gamma API 或手动配置
async fn resolve_market(settings: &Settings) -> anyhow::Result<(String, String, String)> {
    use polymarket_client_sdk::gamma;

    // 1. 优先使用 slug + Gamma API
    if let Some(slug) = &settings.market_slug {
        info!("🔍 Fetching market via Gamma API: {}", slug);
        
        let gamma_client = gamma::Client::default();
        
        // 使用 markets_by_slug 方法
        let market = gamma_client
            .market_by_slug(slug)
            .await
            .context("Failed to fetch market by slug")?;
        
        info!("   Question: {}", market.question);
        info!("   Active: {}, Closed: {}", market.active.unwrap_or(false), market.closed);
        
        // 提取 YES 和 NO token IDs
        let yes_token = market
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
            .ok_or_else(|| anyhow::anyhow!("YES token not found"))?;
        
        let no_token = market
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("no"))
            .ok_or_else(|| anyhow::anyhow!("NO token not found"))?;
        
        return Ok((
            market.condition_id,
            yes_token.token_id.clone(),
            no_token.token_id.clone(),
        ));
    }
    
    // 2. 备选：使用手动配置的 ID
    if let (Some(market_id), Some(yes_id), Some(no_id)) = (
        &settings.market_id,
        &settings.yes_asset_id,
        &settings.no_asset_id,
    ) {
        info!("📌 Using manual market configuration");
        return Ok((market_id.clone(), yes_id.clone(), no_id.clone()));
    }
    
    Err(anyhow::anyhow!("Insufficient market configuration"))
}
