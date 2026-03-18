fn main() {
    // What if the market is not equal mid? What if YES is 0.8 and NO is 0.2?
    let mid_yes = 0.8;
    let mid_no = 0.2;
    let target = 0.98;
    let max_net_diff = 10.0;
    
    // Simulate being long NO (net_diff = -10) - we want to buy YES
    let net_diff = -10.0_f64;
    
    let excess = f64::max(0.0, (mid_yes + mid_no) - target);
    let skew = (net_diff / max_net_diff).clamp(-1.0, 1.0);
    
    // With time decay k=2.0 and at the very end of the market (elapsed_frac=1.0)
    let as_skew_factor = 0.03;
    let effective_skew_factor = as_skew_factor * (1.0 + 2.0 * 1.0); // 0.09
    let skew_shift = skew * effective_skew_factor; // -1.0 * 0.09 = -0.09
    
    // skew_shift is -0.09
    // raw_yes = 0.8 - (0.02 / 2.0) - (-0.09) = 0.8 - 0.01 + 0.09 = 0.88
    // raw_no = 0.2 - (0.02 / 2.0) + (-0.09) = 0.2 - 0.01 - 0.09 = 0.10
    let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
    let mut raw_no = mid_no - (excess / 2.0) + skew_shift;
    
    println!("excess: {}", excess);
    println!("skew: {}", skew);
    println!("effective_skew_factor: {}", effective_skew_factor);
    println!("skew_shift: {}", skew_shift);
    println!("raw_yes: {}", raw_yes);
    println!("raw_no: {}", raw_no);
    println!("sum: {}", raw_yes + raw_no);
    
    if raw_yes + raw_no > target {
        let overflow = (raw_yes + raw_no) - target;
        raw_yes -= overflow / 2.0;
        raw_no -= overflow / 2.0;
    }
    
    println!("after overflow check - raw_yes: {}", raw_yes);
    println!("after overflow check - raw_no: {}", raw_no);
    println!("after overflow check - sum: {}", raw_yes + raw_no);
}
