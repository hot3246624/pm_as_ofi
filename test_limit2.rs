fn main() {
    let mut raw_yes = 0.35; // This is what the state unified computed based on mid
    let mut raw_no = 0.65;
    let target = 0.98;
    
    // In strict maker clamp
    // If book is YES: bid=0.40 ask=0.45, NO: bid=0.55 ask=0.60
    // And if the safety margin is 0.01
    
    let yes_ask = 0.45;
    let no_ask = 0.60;
    let yes_margin = 0.01;
    let no_margin = 0.01;
    
    let yes_clamp = yes_ask - yes_margin;
    let no_clamp = no_ask - no_margin;
    
    raw_yes = f64::min(raw_yes, yes_clamp);
    raw_no = f64::min(raw_no, no_clamp);
    
    println!("raw_yes: {}", raw_yes);
    println!("raw_no: {}", raw_no);
    println!("sum: {}", raw_yes + raw_no);
}
