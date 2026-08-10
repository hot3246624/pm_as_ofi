#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import {
  fileLineCount,
  inferTwapWindow,
  readJsonl,
  sha256File,
} from "./collect_twap_boundary_shadow.mjs";

function parseArgs(argv) {
  const args = { runDir: null, output: null };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--run-dir") {
      args.runDir = argv[++index];
    } else if (arg === "--output") {
      args.output = argv[++index];
    } else if (arg === "--help") {
      console.log("Usage: audit_twap_boundary_shadow_candidate_labels.mjs --run-dir DIR [--output FILE]");
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!args.runDir) throw new Error("--run-dir is required");
  return args;
}

function timestampMs(value) {
  const number = Number(value);
  if (Number.isFinite(number)) return number < 100_000_000_000 ? number * 1000 : number;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function selectTick(rows, boundaryMs, windowS) {
  let before = null;
  let after = null;
  for (const row of rows) {
    if (row.window_s !== windowS) continue;
    const observedMs = timestampMs(row.observation_ts);
    if (observedMs == null) continue;
    if (observedMs <= boundaryMs && (!before || observedMs > timestampMs(before.observation_ts))) before = row;
    if (observedMs >= boundaryMs && (!after || observedMs < timestampMs(after.observation_ts))) after = row;
  }
  return before || after || null;
}

function inputManifest(runDir) {
  const names = ["market_metadata.jsonl", "twap_ticks.jsonl", "settlement_observations.jsonl"];
  return Object.fromEntries(names.map((name) => {
    const filePath = path.join(runDir, name);
    return [name, {
      sha256: sha256File(filePath),
      bytes: fs.statSync(filePath).size,
      lines: fileLineCount(filePath),
    }];
  }));
}

function audit(args) {
  const runDir = path.resolve(args.runDir);
  const metadata = readJsonl(path.join(runDir, "market_metadata.jsonl"));
  const ticks = readJsonl(path.join(runDir, "twap_ticks.jsonl"));
  const settlements = readJsonl(path.join(runDir, "settlement_observations.jsonl"));
  const metadataBySlug = new Map(metadata.map((row) => [row.slug, row]));
  const ticksByAsset = new Map();
  for (const tick of ticks) {
    const rows = ticksByAsset.get(tick.asset) || [];
    rows.push(tick);
    ticksByAsset.set(tick.asset, rows);
  }

  const rows = settlements.map((settlement) => {
    const market = metadataBySlug.get(settlement.slug);
    const windowS = inferTwapWindow(market?.description);
    const assetTicks = ticksByAsset.get(market?.asset) || [];
    const startTick = market ? selectTick(assetTicks, market.start_ts * 1000, windowS) : null;
    const endTick = market ? selectTick(assetTicks, market.end_ts * 1000, windowS) : null;
    let candidateSide = "unknown";
    if (startTick?.full_accuracy_value != null && endTick?.full_accuracy_value != null) {
      candidateSide = BigInt(endTick.full_accuracy_value) >= BigInt(startTick.full_accuracy_value) ? "Up" : "Down";
    }
    return {
      slug: settlement.slug,
      asset: market?.asset ?? null,
      description_window_s: windowS,
      start_observation_ts: startTick?.observation_ts ?? null,
      end_observation_ts: endTick?.observation_ts ?? null,
      candidate_side: candidateSide,
      raw_collector_candidate_side: settlement.candidate_side ?? null,
      gamma_public_outcome: settlement.outcome ?? null,
      candidate_match: candidateSide === "unknown" ? null : candidateSide === settlement.outcome,
    };
  });
  const scored = rows.filter((row) => row.candidate_match != null);
  const matches = scored.filter((row) => row.candidate_match).length;
  return {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    run_dir: runDir,
    method: "posthoc_exact_e18_candidate_side_using_explicit_gamma_stream_window",
    verdict: "POSTHOC_LABEL_ALIGNMENT_ONLY",
    input_files: inputManifest(runDir),
    counts: {
      metadata: metadata.length,
      twap_ticks: ticks.length,
      settlements: settlements.length,
      scored: scored.length,
      matches,
      mismatches: scored.length - matches,
    },
    match_rate: scored.length ? matches / scored.length : null,
    explicit_windows: [...new Set(metadata.map((row) => inferTwapWindow(row.description)))],
    rows,
    caveats: [
      "This recomputes the observed RTDS tick side against a public Gamma outcome label.",
      "It is not an external-source/local-synthetic predictor accuracy result.",
      "It grants no alpha, PnL, execution, account-ledger, or live-readiness claim.",
    ],
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const report = audit(args);
  const output = path.resolve(args.output || path.join(args.runDir, "posthoc_candidate_label_audit.json"));
  fs.writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ output, verdict: report.verdict, counts: report.counts, match_rate: report.match_rate }, null, 2));
}

main();
