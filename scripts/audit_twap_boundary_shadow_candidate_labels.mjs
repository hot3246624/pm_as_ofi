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
  const args = { runDir: null, output: null, maxBoundaryDistanceMs: 5000 };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--run-dir") {
      args.runDir = argv[++index];
    } else if (arg === "--output") {
      args.output = argv[++index];
    } else if (arg === "--max-boundary-distance-ms") {
      args.maxBoundaryDistanceMs = Number(argv[++index]);
    } else if (arg === "--help") {
      console.log("Usage: audit_twap_boundary_shadow_candidate_labels.mjs --run-dir DIR [--output FILE] [--max-boundary-distance-ms N]");
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!args.runDir) throw new Error("--run-dir is required");
  if (!Number.isFinite(args.maxBoundaryDistanceMs) || args.maxBoundaryDistanceMs < 0) {
    throw new Error("--max-boundary-distance-ms must be non-negative");
  }
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
  const row = before || after || null;
  const observationMs = timestampMs(row?.observation_ts);
  return {
    row,
    selection: before ? "latest_at_or_before" : after ? "first_at_or_after" : "missing",
    signed_distance_ms: observationMs == null ? null : observationMs - boundaryMs,
    absolute_distance_ms: observationMs == null ? null : Math.abs(observationMs - boundaryMs),
  };
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
  const exit = JSON.parse(fs.readFileSync(path.join(runDir, "EXIT.json"), "utf8"));
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
    const startSelection = market ? selectTick(assetTicks, market.start_ts * 1000, windowS) : selectTick([], 0, windowS);
    const endSelection = market ? selectTick(assetTicks, market.end_ts * 1000, windowS) : selectTick([], 0, windowS);
    const startTick = startSelection.row;
    const endTick = endSelection.row;
    let diagnosticCandidateSide = "unknown";
    if (startTick?.full_accuracy_value != null && endTick?.full_accuracy_value != null) {
      diagnosticCandidateSide = BigInt(endTick.full_accuracy_value) >= BigInt(startTick.full_accuracy_value) ? "Up" : "Down";
    }
    const boundaryCoverageComplete = startSelection.absolute_distance_ms != null
      && endSelection.absolute_distance_ms != null
      && startSelection.absolute_distance_ms <= args.maxBoundaryDistanceMs
      && endSelection.absolute_distance_ms <= args.maxBoundaryDistanceMs;
    const candidateSide = boundaryCoverageComplete ? diagnosticCandidateSide : "unknown";
    return {
      slug: settlement.slug,
      asset: market?.asset ?? null,
      description_window_s: windowS,
      start_observation_ts: startTick?.observation_ts ?? null,
      end_observation_ts: endTick?.observation_ts ?? null,
      start_tick_selection: startSelection.selection,
      end_tick_selection: endSelection.selection,
      start_boundary_signed_distance_ms: startSelection.signed_distance_ms,
      end_boundary_signed_distance_ms: endSelection.signed_distance_ms,
      max_boundary_distance_ms: args.maxBoundaryDistanceMs,
      boundary_coverage_complete: boundaryCoverageComplete,
      candidate_side: candidateSide,
      diagnostic_candidate_side: diagnosticCandidateSide,
      raw_collector_candidate_side: settlement.candidate_side ?? null,
      gamma_public_outcome: settlement.outcome ?? null,
      candidate_match: candidateSide === "unknown" ? null : candidateSide === settlement.outcome,
      diagnostic_candidate_match: diagnosticCandidateSide === "unknown" ? null : diagnosticCandidateSide === settlement.outcome,
    };
  });
  const scored = rows.filter((row) => row.candidate_match != null);
  const matches = scored.filter((row) => row.candidate_match).length;
  const diagnosticScored = rows.filter((row) => row.diagnostic_candidate_match != null);
  const diagnosticMatches = diagnosticScored.filter((row) => row.diagnostic_candidate_match).length;
  const lastTickReceiveMs = ticks.reduce((latest, row) => Math.max(latest, Number(row.receive_ms) || -Infinity), -Infinity);
  const exitedAtMs = timestampMs(exit.exited_at);
  const rtdsTailSilenceMs = Number.isFinite(lastTickReceiveMs) && exitedAtMs != null ? exitedAtMs - lastTickReceiveMs : null;
  const verdict = scored.length > 0
    ? "POSTHOC_LABEL_ALIGNMENT_ONLY"
    : "POSTHOC_LABEL_ALIGNMENT_INCOMPLETE_BOUNDARY_COVERAGE";
  return {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    run_dir: runDir,
    method: "posthoc_exact_e18_candidate_side_using_explicit_gamma_stream_window_with_boundary_freshness_gate",
    verdict,
    input_files: inputManifest(runDir),
    counts: {
      metadata: metadata.length,
      twap_ticks: ticks.length,
      settlements: settlements.length,
      scored: scored.length,
      matches,
      mismatches: scored.length - matches,
      incomplete_boundary_coverage: rows.filter((row) => !row.boundary_coverage_complete).length,
      diagnostic_scored: diagnosticScored.length,
      diagnostic_matches: diagnosticMatches,
      diagnostic_mismatches: diagnosticScored.length - diagnosticMatches,
    },
    match_rate: scored.length ? matches / scored.length : null,
    diagnostic_match_rate: diagnosticScored.length ? diagnosticMatches / diagnosticScored.length : null,
    rtds_coverage: {
      first_tick_receive_ms: ticks.length ? Math.min(...ticks.map((row) => Number(row.receive_ms)).filter(Number.isFinite)) : null,
      last_tick_receive_ms: Number.isFinite(lastTickReceiveMs) ? lastTickReceiveMs : null,
      exited_at_ms: exitedAtMs,
      tail_silence_ms: rtdsTailSilenceMs,
      tail_silence_exceeds_boundary_gate: rtdsTailSilenceMs != null && rtdsTailSilenceMs > args.maxBoundaryDistanceMs,
    },
    explicit_windows: [...new Set(metadata.map((row) => inferTwapWindow(row.description)))],
    rows,
    caveats: [
      "Only rows whose selected start and end RTDS observations are within the configured boundary distance are scored.",
      "diagnostic_candidate_side is unqualified when boundary coverage is incomplete and must not be reported as accuracy.",
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
