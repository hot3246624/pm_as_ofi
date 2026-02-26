with open('src/bin/polymarket_v2.rs', 'r', encoding='utf-8') as f:
    c = f.read()
with open('src/bin/polymarket_v2.rs', 'w', encoding='utf-8') as f:
    f.write(c.replace('mev_backrun_rs_cu', 'pm_as_ofi'))
