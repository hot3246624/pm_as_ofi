fn main() {
    let buy_vol = 100.0_f64;
    let sell_vol = 0.0_f64;
    let total = buy_vol + sell_vol;
    let ofi = (buy_vol - sell_vol).abs();
    let ratio = ofi / total;
    println!("ratio: {}", ratio);
}
