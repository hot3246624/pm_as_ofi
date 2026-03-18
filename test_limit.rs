fn main() {
    let mut raw_yes = 0.35;
    let mut raw_no = 0.65;
    let target = 0.98;
    
    if raw_yes + raw_no > target {
        let overflow = (raw_yes + raw_no) - target;
        raw_yes -= overflow / 2.0;
        raw_no -= overflow / 2.0;
    }
    
    println!("raw_yes: {}", raw_yes);
    println!("raw_no: {}", raw_no);
    println!("sum: {}", raw_yes + raw_no);
}
