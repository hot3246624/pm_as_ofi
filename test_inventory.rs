fn main() {
    let mut ledger: Vec<(f64, f64, &str)> = Vec::new(); // size, price, side
    
    // Simulating sequence of trades:
    // 2026-03-17T16:25:52 YES=5.0@0.4400 NO=5.0@0.5300 | net=0.0 cost=0.9700
    // So beforehand we had NO 5@0.53
    ledger.push((5.0, 0.53, "NO"));
    ledger.push((5.0, 0.44, "YES"));
    
    // 2026-03-17T16:26:17 📦 Fill: No 5.00@0.660 status=Matched id=0x042e78 → YES=5.0@0.4400 NO=10.0@0.5950 | net=-5.0 cost=1.0350
    ledger.push((5.0, 0.66, "NO"));
    
    // 2026-03-17T16:26:23 📦 Fill: No 5.00@0.650 status=Matched id=0x7558b5 → YES=5.0@0.4400 NO=15.0@0.6133 | net=-10.0 cost=1.0533
    ledger.push((5.0, 0.65, "NO"));
    
    // Calculate 
    let mut yes_qty = 0.0;
    let mut yes_cost_sum = 0.0;
    let mut no_qty = 0.0;
    let mut no_cost_sum = 0.0;
    
    for (size, price, side) in ledger {
        if side == "YES" {
            yes_qty += size;
            yes_cost_sum += size * price;
        } else {
            no_qty += size;
            no_cost_sum += size * price;
        }
        
        let yes_avg = if yes_qty > 0.0 { yes_cost_sum / yes_qty } else { 0.0 };
        let no_avg = if no_qty > 0.0 { no_cost_sum / no_qty } else { 0.0 };
        let cost = yes_avg + no_avg;
        println!("YES={:.1}@{:.4} NO={:.1}@{:.4} cost={:.4}", yes_qty, yes_avg, no_qty, no_avg, cost);
    }
}
