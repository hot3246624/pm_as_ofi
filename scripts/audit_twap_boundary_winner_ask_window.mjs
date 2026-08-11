#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { forEachJsonl, readJsonl } from "./collect_twap_boundary_shadow.mjs";

const DEFAULT_THRESHOLDS = [0.9, 0.95, 0.98, 0.99, 1.0];

function parseArgs(argv) {
  const args = { runDir: null, output: null, horizonMs: 120_000, thresholds: DEFAULT_THRESHOLDS };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = () => {
      if (index + 1 >= argv.length) throw new Error(`missing value for ${arg}`);
      index += 1;
      return argv[index];
    };
    if (arg === "--run-dir") args.runDir = next();
    else if (arg === "--output") args.output = next();
    else if (arg === "--horizon-ms") args.horizonMs = Number(next());
    else if (arg === "--thresholds") args.thresholds = next().split(",").map(Number);
    else if (arg === "--help") {
      console.log("Usage: audit_twap_boundary_winner_ask_window.mjs --run-dir DIR [--output FILE] [--horizon-ms N] [--thresholds 0.9,0.95,0.98,0.99,1.0]");
      process.exit(0);
    } else throw new Error(`unknown argument: ${arg}`);
  }
  if (!args.runDir) throw new Error("--run-dir is required");
  if (!Number.isFinite(args.horizonMs) || args.horizonMs <= 0) throw new Error("--horizon-ms must be positive");
  if (!args.thresholds.length || args.thresholds.some((value) => !Number.isFinite(value) || value <= 0 || value > 1)) {
    throw new Error("--thresholds must contain values in (0, 1]");
  }
  args.thresholds = [...new Set(args.thresholds)].sort((left, right) => left - right);
  return args;
}

function finiteNumber(value) {
  if (value == null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function timestampMs(value) {
  if (value == null) return null;
  const number = Number(value);
  if (Number.isFinite(number)) return number < 100_000_000_000 ? number * 1000 : number;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function quoteForRow(row) {
  const eventBestBid = finiteNumber(row?.event_best_bid);
  const eventBestAsk = finiteNumber(row?.event_best_ask);
  return {
    best_bid: eventBestBid ?? finiteNumber(row?.best_bid),
    best_ask: eventBestAsk ?? finiteNumber(row?.best_ask),
    source: eventBestBid != null || eventBestAsk != null ? "event_best_quote" : "reconstructed_book",
  };
}

function depthAtAsk(row, bestAsk) {
  if (bestAsk == null) return null;
  const level = (Array.isArray(row?.asks) ? row.asks : []).find((candidate) => finiteNumber(candidate.price) === bestAsk);
  return level ? finiteNumber(level.size) : null;
}

function compactEvent(row) {
  if (!row) return null;
  const quote = quoteForRow(row);
  return {
    receive_ms: finiteNumber(row.receive_ms),
    event_ts_ms: timestampMs(row.event_ts_ms ?? row.event_ts),
    event_kind: row.event_kind ?? null,
    best_bid: quote.best_bid,
    best_ask: quote.best_ask,
    quote_source: quote.source,
    ask_depth_at_best: depthAtAsk(row, quote.best_ask),
  };
}

function distribution(values) {
  const sorted = values.filter(Number.isFinite).sort((left, right) => left - right);
  const quantile = (q) => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * q))] : null;
  return {
    count: sorted.length,
    min: quantile(0),
    p50: quantile(0.5),
    p90: quantile(0.9),
    p95: quantile(0.95),
    max: quantile(1),
  };
}

function initializeThresholds(state, thresholds) {
  if (state.thresholds) return;
  const baselineAsk = quoteForRow(state.baseline).best_ask;
  state.thresholds = Object.fromEntries(thresholds.map((threshold) => [String(threshold), {
    threshold,
    buyable_at_boundary: baselineAsk != null && baselineAsk < threshold,
    boundary_window_end: null,
    first_post_buyable: null,
    first_post_buyable_window_end: null,
  }]));
}

function processPostEvent(state, row, thresholds) {
  initializeThresholds(state, thresholds);
  const ask = quoteForRow(row).best_ask;
  for (const thresholdState of Object.values(state.thresholds)) {
    const buyable = ask != null && ask < thresholdState.threshold;
    if (thresholdState.buyable_at_boundary && !thresholdState.boundary_window_end && !buyable) {
      thresholdState.boundary_window_end = row;
    }
    if (!thresholdState.first_post_buyable && buyable) {
      thresholdState.first_post_buyable = row;
    } else if (thresholdState.first_post_buyable && !thresholdState.first_post_buyable_window_end && !buyable) {
      thresholdState.first_post_buyable_window_end = row;
    }
  }
}

function latestRowsBySlug(rows) {
  const bySlug = new Map();
  for (const row of rows) bySlug.set(row.slug, row);
  return bySlug;
}

function audit(args) {
  const runDir = path.resolve(args.runDir);
  const metadata = latestRowsBySlug(readJsonl(path.join(runDir, "market_metadata.jsonl")));
  const boundaries = latestRowsBySlug(readJsonl(path.join(runDir, "boundary_observations.jsonl")));
  const settlements = latestRowsBySlug(readJsonl(path.join(runDir, "settlement_observations.jsonl")));
  const summary = JSON.parse(fs.readFileSync(path.join(runDir, "summary.json"), "utf8"));
  const manifest = JSON.parse(fs.readFileSync(path.join(runDir, "manifest.json"), "utf8"));
  const clobGapMs = (summary.gap_events || [])
    .filter((row) => row.source === "clob")
    .map((row) => timestampMs(row.ts))
    .filter(Number.isFinite);

  const statesByToken = new Map();
  const setupErrors = [];
  for (const [slug, settlement] of settlements) {
    const market = metadata.get(slug);
    const boundary = boundaries.get(slug);
    const outcomes = Array.isArray(market?.outcomes) ? market.outcomes.map(String) : [];
    const tokenIds = Array.isArray(market?.token_ids) ? market.token_ids.map(String) : [];
    const outcomeIndex = outcomes.findIndex((outcome) => outcome.toLowerCase() === String(settlement.outcome || "").toLowerCase());
    if (!market || !boundary || outcomeIndex < 0 || !tokenIds[outcomeIndex]) {
      setupErrors.push({ slug, market_present: Boolean(market), boundary_present: Boolean(boundary), outcome_index: outcomeIndex });
      continue;
    }
    const winnerToken = tokenIds[outcomeIndex];
    statesByToken.set(winnerToken, {
      slug,
      asset: market.asset,
      winner_side: settlement.outcome,
      winner_token: winnerToken,
      end_ms: Number(boundary.end_ts) * 1000,
      baseline: null,
      latest_snapshot_before: null,
      first_post: null,
      first_snapshot_post: null,
      thresholds: null,
    });
  }

  let eventRowsScanned = 0;
  let winnerRowsScanned = 0;
  forEachJsonl(path.join(runDir, "book_events.jsonl"), (row) => {
    eventRowsScanned += 1;
    const state = statesByToken.get(String(row.asset_id));
    if (!state) return;
    winnerRowsScanned += 1;
    const receiveMs = finiteNumber(row.receive_ms);
    if (receiveMs == null) return;
    if (receiveMs <= state.end_ms) {
      state.baseline = row;
      if (row.event_kind === "book") state.latest_snapshot_before = row;
      return;
    }
    if (receiveMs > state.end_ms + args.horizonMs) return;
    if (!state.first_post) state.first_post = row;
    if (!state.first_snapshot_post && row.event_kind === "book") state.first_snapshot_post = row;
    processPostEvent(state, row, args.thresholds);
  });

  const rows = [...statesByToken.values()].map((state) => {
    initializeThresholds(state, args.thresholds);
    const snapshotReceiveMs = finiteNumber(state.latest_snapshot_before?.receive_ms);
    const gapsAfterSnapshot = clobGapMs.filter((gapMs) => gapMs > (snapshotReceiveMs ?? -Infinity) && gapMs <= state.end_ms);
    const thresholdRows = Object.fromEntries(Object.entries(state.thresholds).map(([key, thresholdState]) => {
      const boundaryWindowEnd = compactEvent(thresholdState.boundary_window_end);
      const firstPostBuyable = compactEvent(thresholdState.first_post_buyable);
      const firstPostBuyableEnd = compactEvent(thresholdState.first_post_buyable_window_end);
      return [key, {
        threshold: thresholdState.threshold,
        buyable_at_boundary: thresholdState.buyable_at_boundary,
        boundary_public_window_receive_ms: thresholdState.buyable_at_boundary && boundaryWindowEnd?.receive_ms != null
          ? boundaryWindowEnd.receive_ms - state.end_ms
          : null,
        boundary_public_window_event_ms: thresholdState.buyable_at_boundary && boundaryWindowEnd?.event_ts_ms != null
          ? boundaryWindowEnd.event_ts_ms - state.end_ms
          : null,
        boundary_window_right_censored: thresholdState.buyable_at_boundary && !boundaryWindowEnd,
        first_post_buyable_lag_ms: firstPostBuyable?.receive_ms != null ? firstPostBuyable.receive_ms - state.end_ms : null,
        first_post_buyable_window_ms: firstPostBuyable?.receive_ms != null && firstPostBuyableEnd?.receive_ms != null
          ? firstPostBuyableEnd.receive_ms - firstPostBuyable.receive_ms
          : null,
        boundary_window_end: boundaryWindowEnd,
        first_post_buyable: firstPostBuyable,
      }];
    }));
    const baseline = compactEvent(state.baseline);
    const firstPost = compactEvent(state.first_post);
    return {
      slug: state.slug,
      asset: state.asset,
      winner_side: state.winner_side,
      winner_token: state.winner_token,
      end_ms: state.end_ms,
      latest_snapshot_before: compactEvent(state.latest_snapshot_before),
      snapshot_age_at_boundary_ms: snapshotReceiveMs == null ? null : state.end_ms - snapshotReceiveMs,
      clob_gap_after_latest_snapshot_before_boundary: gapsAfterSnapshot.length > 0,
      clob_gap_times_ms: gapsAfterSnapshot,
      baseline,
      baseline_age_at_boundary_ms: baseline?.receive_ms == null ? null : state.end_ms - baseline.receive_ms,
      first_post: firstPost,
      first_post_receive_lag_ms: firstPost?.receive_ms == null ? null : firstPost.receive_ms - state.end_ms,
      thresholds: thresholdRows,
    };
  });

  const aggregateByThreshold = Object.fromEntries(args.thresholds.map((threshold) => {
    const key = String(threshold);
    const reliable = rows.filter((row) => row.latest_snapshot_before && !row.clob_gap_after_latest_snapshot_before_boundary);
    const buyable = rows.filter((row) => row.thresholds[key].buyable_at_boundary);
    const reliableBuyable = reliable.filter((row) => row.thresholds[key].buyable_at_boundary);
    return [key, {
      rows: rows.length,
      reliable_rows: reliable.length,
      buyable_at_boundary_count: buyable.length,
      reliable_buyable_at_boundary_count: reliableBuyable.length,
      boundary_public_window_receive_ms: distribution(reliableBuyable.map((row) => row.thresholds[key].boundary_public_window_receive_ms)),
      right_censored_count: reliableBuyable.filter((row) => row.thresholds[key].boundary_window_right_censored).length,
      first_post_buyable_count: reliable.filter((row) => row.thresholds[key].first_post_buyable_lag_ms != null).length,
      first_post_buyable_lag_ms: distribution(reliable.map((row) => row.thresholds[key].first_post_buyable_lag_ms)),
    }];
  }));

  return {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    run_dir: runDir,
    verdict: "PUBLIC_WINNER_ASK_MECHANISM_AUDIT_ONLY",
    method: {
      winner_label: "Gamma public outcome mapped to its CLOB token ID after the round",
      timing: "local EC2 receive_ms relative to slug-derived round end",
      quote: "event_best_bid/event_best_ask when present, otherwise reconstructed compact book",
      boundary_window: "contiguous interval from round_end while winning-token best_ask is below the threshold",
      horizon_ms: args.horizonMs,
      thresholds: args.thresholds,
    },
    input_binding: {
      source_commit: manifest.source_commit ?? null,
      book_events: manifest.files?.["book_events.jsonl"] ?? null,
      boundary_observations: manifest.files?.["boundary_observations.jsonl"] ?? null,
      settlement_observations: manifest.files?.["settlement_observations.jsonl"] ?? null,
    },
    counts: {
      setup_rows: statesByToken.size,
      setup_errors: setupErrors.length,
      event_rows_scanned: eventRowsScanned,
      winner_event_rows_scanned: winnerRowsScanned,
      rows_with_snapshot_before_boundary: rows.filter((row) => row.latest_snapshot_before).length,
      rows_with_gap_after_snapshot_before_boundary: rows.filter((row) => row.clob_gap_after_latest_snapshot_before_boundary).length,
      rows_with_winner_ask_at_boundary: rows.filter((row) => row.baseline?.best_ask != null).length,
    },
    baseline_winner_best_ask: distribution(rows.map((row) => row.baseline?.best_ask)),
    baseline_age_at_boundary_ms: distribution(rows.map((row) => row.baseline_age_at_boundary_ms)),
    first_post_receive_lag_ms: distribution(rows.map((row) => row.first_post_receive_lag_ms)),
    aggregate_by_threshold: aggregateByThreshold,
    setup_errors: setupErrors,
    rows,
    caveats: [
      "Gamma outcome is used only as a posthoc winner label and is not a causal strategy feature.",
      "Public quote visibility does not prove order acceptance, fill, queue position, or private execution truth.",
      "This audit measures whether a winning-token ask remained publicly visible, not whether a local synthetic candidate was ready in time.",
      "A result with no reliable winning ask means the current public tape does not evidence a taker stale-ask opportunity.",
    ],
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const report = audit(args);
  const output = path.resolve(args.output || path.join(args.runDir, "winner_ask_window_audit.json"));
  fs.writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ output, verdict: report.verdict, counts: report.counts, aggregate_by_threshold: report.aggregate_by_threshold }, null, 2));
}

main();
