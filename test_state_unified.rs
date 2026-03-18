fn main() {
    let mut mid_yes = 0.5;
    let mut mid_no = 0.5;
    let target = 0.98;
    let max_net_diff = 10.0;
    
    // Simulate what happened at 16:26:17
    // The previous state was YES=5.0 NO=5.0 | net=0.0
    // Then it bought NO at 0.66.
    
    // So let's imagine the market moved and NO became expensive:
    mid_yes = 0.35;
    mid_no = 0.65;
    let net_diff = 0.0_f64;
    
    let excess = f64::max(0.0, (mid_yes + mid_no) - target);
    let skew = (net_diff / max_net_diff).clamp(-1.0, 1.0);
    
    let as_skew_factor = 0.03;
    let effective_skew_factor = as_skew_factor * 1.0; 
    let skew_shift = skew * effective_skew_factor;
    
    let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
    let mut raw_no = mid_no - (excess / 2.0) + skew_shift;
    
    println!("raw_yes: {:.3}", raw_yes);
    println!("raw_no: {:.3}", raw_no);
}
