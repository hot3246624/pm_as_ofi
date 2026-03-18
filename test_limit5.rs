fn main() {
    let inv_yes_qty = 5.0;
    let inv_yes_avg_cost = 0.44;
    let target = 0.98;
    
    // We want to buy 5 YES. How much can we pay for NO such that the pair cost is 0.98?
    // Well, wait. We don't have pairs yet. 
    // If we have 5 YES at 0.44, we need 5 NO.
    // The ceiling for NO should be target - inv_yes_avg_cost.
    // 0.98 - 0.44 = 0.54.
    
    // So the absolute MAXIMUM we can pay for NO is 0.54!
    // But in the log, state_unified set raw_no = 0.65, and it bought NO at 0.66!
    
    let mid_no = 0.65;
    let raw_no = f64::min(mid_no, target - inv_yes_avg_cost);
    println!("raw_no should be capped at: {}", raw_no);
}
