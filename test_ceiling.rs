fn main() {
    let inv_yes_qty = 25.5;
    let inv_no_qty = 15.5;
    let inv_yes_avg_cost = 0.3929;
    let inv_no_avg_cost = 0.4301;
    let net_diff = 10.0;
    
    let hedge_size = 10.0;
    let target = 0.98;
    
    let q = inv_no_qty;
    let target_no_avg = target - inv_yes_avg_cost; // 0.98 - 0.3929 = 0.5871
    let existing_cost = q * inv_no_avg_cost; // 15.5 * 0.4301 = 6.66655
    let total_allowed = target_no_avg * (q + hedge_size); // 0.5871 * (15.5 + 10.0) = 0.5871 * 25.5 = 14.97105
    let incremental_allowed = total_allowed - existing_cost; // 14.97105 - 6.66655 = 8.3045
    let ceiling = incremental_allowed / hedge_size; // 8.3045 / 10.0 = 0.83045
    
    println!("ceiling: {}", ceiling);
    
    // Now simulate buying exactly at ceiling
    let new_no_qty = inv_no_qty + hedge_size;
    let new_no_cost = existing_cost + (hedge_size * ceiling);
    let new_no_avg = new_no_cost / new_no_qty;
    
    println!("new_no_avg: {}", new_no_avg);
    println!("new pair cost: {}", inv_yes_avg_cost + new_no_avg);
}
