#!/usr/bin/env python3
import argparse
import re
from collections import defaultdict
from pathlib import Path
from statistics import mean

ROUND_RE = re.compile(r'slug=[^ ]+-([0-9]{10})\b')
KV_RE = re.compile(r'(\w+)=([^\s]+)')

FULL_SHADOW_PATTERNS = {
    'accepted': ('local_price_agg_full_shadow_vs_rtds |',),
    'filtered': ('local_price_agg_full_shadow_filtered |',),
    'unresolved': ('local_price_agg_full_shadow_unresolved |',),
}

LEGACY_PATTERNS = {
    'accepted': ('local_price_agg_vs_rtds |',),
    'filtered': ('local_price_agg_vs_rtds_filtered |',),
    'unresolved': ('local_price_agg_vs_rtds_unresolved |',),
}

BOUNDARY_SHADOW_PATTERNS = {
    'accepted': ('local_price_agg_boundary_shadow_vs_rtds |',),
    'filtered': ('local_price_agg_boundary_shadow_filtered |',),
    'unresolved': ('local_price_agg_boundary_shadow_unresolved |',),
}


def parse_line(line: str, kind_patterns):
    for kind, pats in kind_patterns.items():
        if any(pat in line for pat in pats):
            m = ROUND_RE.search(line)
            if not m:
                return None
            row = {'kind': kind, 'round': m.group(1)}
            for k, v in KV_RE.findall(line):
                row[k] = v
            return row
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('logfile', type=Path)
    ap.add_argument('--last', type=int, default=6)
    ap.add_argument(
        '--since-iso',
        default='',
        help='Only include log lines whose leading ISO8601 timestamp is >= this value (for example 2026-04-27T06:58:53).',
    )
    ap.add_argument(
        '--mode',
        choices=('close-only', 'full-shadow', 'boundary-shadow', 'auto'),
        default='close-only',
        help='Which comparison stream to summarize. close-only matches the RTDS-open truth path; full-shadow summarizes fully local open+close shadow diagnostics; boundary-shadow summarizes boundary challengers by policy.',
    )
    args = ap.parse_args()

    lines = args.logfile.read_text(errors='ignore').splitlines()
    if args.mode == 'full-shadow':
        kind_patterns = FULL_SHADOW_PATTERNS
    elif args.mode == 'boundary-shadow':
        kind_patterns = BOUNDARY_SHADOW_PATTERNS
    elif args.mode == 'close-only':
        kind_patterns = LEGACY_PATTERNS
    else:
        if any('local_price_agg_boundary_shadow_vs_rtds |' in line for line in lines):
            kind_patterns = BOUNDARY_SHADOW_PATTERNS
        elif any(
            'local_price_agg_full_shadow_vs_rtds |' in line for line in lines
        ):
            kind_patterns = FULL_SHADOW_PATTERNS
        else:
            kind_patterns = LEGACY_PATTERNS

    group_by_policy = kind_patterns is BOUNDARY_SHADOW_PATTERNS
    rounds = defaultdict(lambda: defaultdict(lambda: {'accepted': [], 'filtered': [], 'unresolved': []}))
    symbols = defaultdict(lambda: defaultdict(lambda: {'accepted': 0, 'filtered': 0, 'unresolved': 0, 'mismatch': 0}))

    since_iso = args.since_iso.strip()
    for line in lines:
        if since_iso and len(line) >= 19 and line[:19] < since_iso[:19]:
            continue
        row = parse_line(line, kind_patterns)
        if not row:
            continue
        bucket_key = row.get('policy', 'close_only') if group_by_policy else 'close_only'
        rounds[row['round']][bucket_key][row['kind']].append(row)
        sym = row.get('symbol')
        if sym:
            symbols[sym][bucket_key][row['kind']] += 1
            if row['kind'] == 'accepted' and row.get('side_match_vs_rtds_open') == 'false':
                symbols[sym][bucket_key]['mismatch'] += 1

    round_ids = sorted(rounds.keys())
    print('rounds_seen:', len(round_ids))
    print('recent_rounds:', ', '.join(round_ids[-args.last:]))
    print()
    for r in round_ids[-args.last:]:
        policies = sorted(rounds[r].keys())
        for policy in policies:
            bucket = rounds[r][policy]
            accepted = bucket['accepted']
            filtered = bucket['filtered']
            unresolved = bucket['unresolved']
            mismatch = [x for x in accepted if x.get('side_match_vs_rtds_open') == 'false']
            bps_values = [float(x['close_diff_bps']) for x in accepted if x.get('close_diff_bps')]
            acc_syms = ', '.join(x.get('symbol', '?') for x in accepted)
            filt_syms = ', '.join(f"{x.get('symbol','?')}:{x.get('reason','?')}" for x in filtered)
            unr_syms = ', '.join(f"{x.get('symbol','?')}:{x.get('reason','?')}" for x in unresolved)
            prefix = f'ROUND {r}' if policy == policies[0] else ' ' * (len(r) + 6)
            label = f'{prefix} [{policy}]'
            metrics = ''
            if bps_values:
                metrics = f" mean_bps={mean(bps_values):.6f} max_bps={max(bps_values):.6f}"
            print(f'{label}: accepted={len(accepted)} mismatch={len(mismatch)} filtered={len(filtered)} unresolved={len(unresolved)}{metrics}')
            if acc_syms:
                print(f'  accepted:   {acc_syms}')
            if filt_syms:
                print(f'  filtered:   {filt_syms}')
            if unr_syms:
                print(f'  unresolved: {unr_syms}')
            if mismatch:
                detail = ', '.join(f"{x.get('symbol')}:{x.get('local_side_vs_rtds_open')}!={x.get('rtds_side')}@{x.get('close_diff_bps')}bps" for x in mismatch)
                print(f'  mismatches: {detail}')
        print()

    print('per_symbol:')
    for sym in sorted(symbols):
        for policy in sorted(symbols[sym]):
            s = symbols[sym][policy]
            print(f"  {sym} [{policy}]: accepted={s['accepted']} mismatch={s['mismatch']} filtered={s['filtered']} unresolved={s['unresolved']}")

if __name__ == '__main__':
    main()
