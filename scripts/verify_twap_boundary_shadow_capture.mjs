#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileLineCount, forEachJsonl, inferTwapWindow, readJsonl, sha256File } from "./collect_twap_boundary_shadow.mjs";

function parseArgs(argv) {
  const args = { runDir: null, output: null, expectSourceCommit: null, expectCodeSha256: null };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = () => {
      if (i + 1 >= argv.length) throw new Error(`missing value for ${arg}`);
      i += 1;
      return argv[i];
    };
    if (arg === "--run-dir") args.runDir = next();
    else if (arg === "--output") args.output = next();
    else if (arg === "--expect-source-commit") args.expectSourceCommit = next();
    else if (arg === "--expect-code-sha256") args.expectCodeSha256 = next();
    else if (arg === "--help") {
      console.log("Usage: verify_twap_boundary_shadow_capture.mjs --run-dir DIR [--output FILE] [--expect-source-commit HASH] [--expect-code-sha256 SHA256]");
      process.exit(0);
    } else throw new Error(`unknown argument: ${arg}`);
  }
  if (!args.runDir) throw new Error("--run-dir is required");
  return args;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function parseRoundStart(slug) {
  const match = /-updown-5m-(\d+)$/.exec(String(slug || ""));
  return match ? Number(match[1]) : null;
}

function quantile(values, q) {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * q))] : null;
}

function stats(values) {
  return {
    count: values.filter(Number.isFinite).length,
    min: quantile(values, 0),
    p50: quantile(values, 0.5),
    p90: quantile(values, 0.9),
    p95: quantile(values, 0.95),
    max: quantile(values, 1),
  };
}

function check(condition, reason) {
  return { status: condition ? "PASS" : "FAIL", reason };
}

function timestampMs(value) {
  if (value == null) return null;
  const number = Number(value);
  if (Number.isFinite(number)) return number < 100_000_000_000 ? number * 1000 : number;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function rtdsCoverage(started, exit, ticks) {
  const staleThresholdMs = Number(started.args?.rtdsStaleMs) || 10_000;
  const expectedStreams = [];
  for (const asset of started.args?.assets || []) {
    for (const windowS of started.args?.windows || []) expectedStreams.push(`${asset}:${windowS}`);
  }
  const byStream = new Map();
  for (const tick of ticks) {
    const key = `${tick.asset}:${tick.window_s}`;
    if (!byStream.has(key)) byStream.set(key, []);
    const receiveMs = Number(tick.receive_ms);
    if (Number.isFinite(receiveMs)) byStream.get(key).push(receiveMs);
  }
  const exitedAtMs = timestampMs(exit.exited_at);
  const streams = [...new Set([...expectedStreams, ...byStream.keys()])].sort().map((key) => {
    const values = (byStream.get(key) || []).sort((left, right) => left - right);
    const gaps = values.slice(1).map((value, index) => value - values[index]);
    const lastReceiveMs = values.at(-1) ?? null;
    const tailSilenceMs = lastReceiveMs == null || exitedAtMs == null ? null : exitedAtMs - lastReceiveMs;
    return {
      stream: key,
      tick_rows: values.length,
      first_receive_ms: values[0] ?? null,
      last_receive_ms: lastReceiveMs,
      max_interarrival_ms: gaps.length ? Math.max(...gaps) : null,
      tail_silence_ms: tailSilenceMs,
      complete_at_exit: values.length > 0 && tailSilenceMs != null && tailSilenceMs <= staleThresholdMs,
    };
  });
  const lastReceiveMs = ticks.reduce((latest, tick) => Number.isFinite(Number(tick.receive_ms)) ? Math.max(latest, Number(tick.receive_ms)) : latest, -Infinity);
  const tailSilenceMs = Number.isFinite(lastReceiveMs) && exitedAtMs != null ? exitedAtMs - lastReceiveMs : null;
  return {
    stale_threshold_ms: staleThresholdMs,
    exited_at_ms: exitedAtMs,
    last_tick_receive_ms: Number.isFinite(lastReceiveMs) ? lastReceiveMs : null,
    tail_silence_ms: tailSilenceMs,
    expected_stream_count: expectedStreams.length,
    observed_stream_count: byStream.size,
    streams,
    complete: streams.length > 0 && streams.every((stream) => stream.complete_at_exit),
  };
}

function selectBoundaryTick(rows, boundaryMs) {
  let before = null;
  let after = null;
  for (const row of rows) {
    const observedMs = timestampMs(row.observation_ts);
    if (observedMs == null) continue;
    if (observedMs <= boundaryMs && (!before || observedMs > timestampMs(before.observation_ts))) before = row;
    if (observedMs >= boundaryMs && (!after || observedMs < timestampMs(after.observation_ts))) after = row;
  }
  const row = before || after || null;
  const observedMs = timestampMs(row?.observation_ts);
  return { row, distance_ms: observedMs == null ? null : Math.abs(observedMs - boundaryMs) };
}

function verify(args) {
  const runDir = path.resolve(args.runDir);
  const required = ["STARTED.json", "CHECKPOINT.json", "EXIT.json", "manifest.json", "summary.json"];
  const missing = required.filter((name) => !fs.existsSync(path.join(runDir, name)));
  if (missing.length) {
    return {
      schema_version: 1,
      run_dir: runDir,
      terminal: false,
      verdict: "INCOMPLETE_CAPTURE",
      missing_files: missing,
      next_action: "Wait for EXIT.json and rerun this verifier; do not score an open run.",
    };
  }

  const started = readJson(path.join(runDir, "STARTED.json"));
  const checkpoint = readJson(path.join(runDir, "CHECKPOINT.json"));
  const exit = readJson(path.join(runDir, "EXIT.json"));
  const manifest = readJson(path.join(runDir, "manifest.json"));
  const summary = readJson(path.join(runDir, "summary.json"));
  const metadata = readJsonl(path.join(runDir, "market_metadata.jsonl"));
  const ticks = readJsonl(path.join(runDir, "twap_ticks.jsonl"));
  let bookEventCount = 0;
  forEachJsonl(path.join(runDir, "book_events.jsonl"), () => {
    bookEventCount += 1;
  });
  const boundaries = readJsonl(path.join(runDir, "boundary_observations.jsonl"));
  const settlements = readJsonl(path.join(runDir, "settlement_observations.jsonl"));

  const manifestFiles = Object.entries(manifest.files || {}).map(([name, expected]) => {
    const filePath = path.join(runDir, name);
    const present = fs.existsSync(filePath);
    return {
      name,
      present,
      sha256_match: present && sha256File(filePath) === expected.sha256,
      bytes_match: present && fs.statSync(filePath).size === expected.bytes,
      lines_match: present && fileLineCount(filePath) === expected.lines,
    };
  });
  const manifestPass = manifestFiles.every((row) => row.present && row.sha256_match && row.bytes_match && row.lines_match);

  const roundRows = metadata.map((row) => {
    const slugStart = parseRoundStart(row.slug);
    return {
      slug: row.slug,
      start_ts: row.start_ts,
      end_ts: row.end_ts,
      slug_start_ts: slugStart,
      round_aligned: slugStart != null && row.start_ts === slugStart && row.end_ts === slugStart + 300,
      twap_window_s: row.twap_window_s ?? null,
      twap_window_source: row.twap_window_source ?? null,
    };
  });
  const aligned = roundRows.filter((row) => row.round_aligned).length;
  const unresolvedWindows = roundRows.filter((row) => ![30, 60].includes(row.twap_window_s)).length;
  const exactTickRows = ticks.filter((row) => row.observation_ts != null && (row.full_accuracy_value != null || row.value_decimal != null)).length;

  const maxBoundaryDistanceMs = Number(started.args?.boundaryTickMaxDistanceMs) || 5_000;
  const metadataBySlug = new Map(metadata.map((row) => [row.slug, row]));
  const ticksByStream = new Map();
  for (const tick of ticks) {
    const key = `${tick.asset}:${tick.window_s}`;
    if (!ticksByStream.has(key)) ticksByStream.set(key, []);
    ticksByStream.get(key).push(tick);
  }
  const labelRows = settlements.map((settlement) => {
    const market = metadataBySlug.get(settlement.slug);
    const windowS = market?.twap_window_s ?? inferTwapWindow(market?.description);
    const stream = ticksByStream.get(`${market?.asset}:${windowS}`) || [];
    const start = selectBoundaryTick(stream, Number(market?.start_ts) * 1000);
    const end = selectBoundaryTick(stream, Number(market?.end_ts) * 1000);
    let diagnosticSide = "unknown";
    if (/^-?\d+$/.test(String(start.row?.full_accuracy_value ?? "")) && /^-?\d+$/.test(String(end.row?.full_accuracy_value ?? ""))) {
      diagnosticSide = BigInt(end.row.full_accuracy_value) >= BigInt(start.row.full_accuracy_value) ? "Up" : "Down";
    }
    const qualified = start.distance_ms != null && end.distance_ms != null
      && start.distance_ms <= maxBoundaryDistanceMs && end.distance_ms <= maxBoundaryDistanceMs;
    return {
      settlement,
      diagnostic_side: diagnosticSide,
      diagnostic_match: ["Up", "Down"].includes(diagnosticSide) && ["Up", "Down"].includes(settlement.outcome)
        ? diagnosticSide === settlement.outcome : null,
      qualified,
      qualified_match: qualified && ["Up", "Down"].includes(diagnosticSide) && ["Up", "Down"].includes(settlement.outcome)
        ? diagnosticSide === settlement.outcome : null,
      start_distance_ms: start.distance_ms,
      end_distance_ms: end.distance_ms,
    };
  });
  const diagnosticMatchRows = labelRows.filter((row) => typeof row.diagnostic_match === "boolean");
  const qualifiedMatchRows = labelRows.filter((row) => typeof row.qualified_match === "boolean");
  const matchCount = qualifiedMatchRows.filter((row) => row.qualified_match).length;
  const mismatchCount = qualifiedMatchRows.filter((row) => !row.qualified_match).length;
  const candidateLabel = {
    settlement_rows: settlements.length,
    scored_rows: qualifiedMatchRows.length,
    matches: matchCount,
    mismatches: mismatchCount,
    match_rate: qualifiedMatchRows.length ? matchCount / qualifiedMatchRows.length : null,
    incomplete_boundary_coverage_rows: settlements.length - qualifiedMatchRows.length,
    max_boundary_distance_ms: maxBoundaryDistanceMs,
    diagnostic_unqualified_scored_rows: diagnosticMatchRows.length,
    diagnostic_unqualified_matches: diagnosticMatchRows.filter((row) => row.diagnostic_match === true).length,
    boundary_distance_examples: labelRows.map((row) => ({ slug: row.settlement.slug, start_distance_ms: row.start_distance_ms, end_distance_ms: row.end_distance_ms, qualified: row.qualified })),
    interpretation: "public Gamma outcome label only; not execution or account truth",
  };

  const boundaryL2 = boundaries.flatMap((boundary) => Object.entries(boundary.token_books || {}).map(([tokenId, book]) => ({
    slug: boundary.slug,
    token_id: tokenId,
    has_book: Boolean(book),
    best_ask: book?.best_ask ?? null,
    best_bid: book?.best_bid ?? null,
    ask_depth_1c: book?.depth_shares_within_1c?.ask ?? null,
    ask_depth_2c: book?.depth_shares_within_2c?.ask ?? null,
    ask_depth_5c: book?.depth_shares_within_5c?.ask ?? null,
    book_age_ms: book?.book_age_ms ?? null,
  })));
  const askRows = boundaryL2.filter((row) => row.best_ask != null);
  const timing = {
    boundary_detection_lag_ms: stats(boundaries.map((row) => row.round_end_detection_lag_ms)),
    rtds: summary.rtds_timing ?? null,
    clob: summary.clob_timing ?? null,
  };

  const forbiddenAuthority = check(
    exit.terminal === true && exit.mode === "no-submit" && exit.live_orders_submitted === 0 && exit.credentials_loaded === false && Array.isArray(exit.open_runs) && exit.open_runs.length === 0,
    "EXIT must be terminal no-submit with zero orders, credentials false, and open_runs=[]",
  );
  const sourceBinding = check(
    !args.expectSourceCommit || manifest.source_commit === args.expectSourceCommit,
    `manifest source_commit=${manifest.source_commit ?? null}`,
  );
  const codeBinding = check(!args.expectCodeSha256 || args.expectCodeSha256 === manifest.code_sha256 || args.expectCodeSha256 === started.code_sha256, "code hash is not recorded in manifest/STARTED; compare the staged collector hash separately");
  const roundContract = check(metadata.length > 0 && aligned === metadata.length, `${aligned}/${metadata.length} metadata rows use slug-derived 5m round boundaries`);
  const exactContract = check(ticks.length > 0 && exactTickRows === ticks.length, `${exactTickRows}/${ticks.length} TWAP rows retain observation_ts and exact decimal/E18 value`);
  const manifestContract = check(manifestPass, "all manifest-bound files match sha256, byte count, and JSONL line count");
  const timingContract = check(summary.rtds_timing != null && summary.clob_timing != null, "summary contains RTDS and CLOB timing distributions");
  const coverage = rtdsCoverage(started, exit, ticks);
  const rtdsCoverageContract = check(coverage.complete, `${coverage.streams.filter((stream) => stream.complete_at_exit).length}/${coverage.streams.length} RTDS streams are fresh at EXIT within ${coverage.stale_threshold_ms}ms`);

  const gapEvents = summary.gap_events || [];
  const reconnects = summary.websocket_reconnects || {};
  const causalMissing = [
    "local_ready_ms and local candidate output are not present in TWAP collector capture",
    "external source event timestamp cutoff <= round_end is not present in TWAP collector capture",
    "local candidate to CLOB reprice join is not proven by public capture alone",
  ];
  const strategyGate = gapEvents.length === 0 && (reconnects.rtds || 0) === 0 && timingContract.status === "PASS" && rtdsCoverageContract.status === "PASS" && causalMissing.length === 0;
  const winnerAskPath = path.join(runDir, "winner_ask_window_audit.json");
  const winnerAskAudit = fs.existsSync(winnerAskPath) ? readJson(winnerAskPath) : null;

  return {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    run_dir: runDir,
    terminal: true,
    verdict: strategyGate ? "CONDITIONAL_RESEARCH_GO_CANDIDATE" : "CONDITIONAL_RESEARCH_INSUFFICIENT_EVIDENCE",
    contract_checks: {
      authority: forbiddenAuthority,
      source_binding: sourceBinding,
      code_binding: codeBinding,
      manifest: manifestContract,
      round_boundaries: roundContract,
      exact_twap_fields: exactContract,
      timing_fields: timingContract,
      rtds_coverage: rtdsCoverageContract,
    },
    terminal_contract: {
      exit,
      started: { started_at: started.started_at, mode: started.mode, live_orders_submitted: started.live_orders_submitted, credentials_loaded: started.credentials_loaded },
      checkpoint: { updated_at: checkpoint.updated_at, counts: checkpoint.counts, ws_states: checkpoint.ws_states, ws_reconnects: checkpoint.ws_reconnects, gap_events: checkpoint.gap_events },
      source_commit: manifest.source_commit ?? null,
    },
    counts: { metadata: metadata.length, twap_ticks: ticks.length, book_events: bookEventCount, boundaries: boundaries.length, settlements: settlements.length },
    round_alignment: { rows: roundRows.length, aligned, misaligned: roundRows.length - aligned, unresolved_windows: unresolvedWindows, examples: roundRows.slice(0, 10) },
    public_gamma_label: candidateLabel,
    rtds_coverage: coverage,
    winner_ask_mechanism_audit: winnerAskAudit == null ? null : {
      sha256: sha256File(winnerAskPath),
      counts: winnerAskAudit.counts,
      baseline_winner_best_ask: winnerAskAudit.baseline_winner_best_ask,
      aggregate_by_threshold: winnerAskAudit.aggregate_by_threshold,
      interpretation: "posthoc public winner-token ask survival; not a causal local candidate or private fill claim",
    },
    timing,
    boundary_l2: {
      token_rows: boundaryL2.length,
      rows_with_book: boundaryL2.filter((row) => row.has_book).length,
      rows_with_best_ask: askRows.length,
      best_ask: stats(askRows.map((row) => row.best_ask)),
      ask_depth_within_1c: stats(askRows.map((row) => row.ask_depth_1c)),
      ask_depth_within_2c: stats(askRows.map((row) => row.ask_depth_2c)),
      ask_depth_within_5c: stats(askRows.map((row) => row.ask_depth_5c)),
      orderable_boundary_count: boundaries.filter((row) => row.orderable_diagnostic).length,
    },
    gaps: { gap_event_count: gapEvents.length, gap_events: gapEvents, websocket_reconnects: reconnects },
    causal_cutoff_status: {
      status: "MISSING_FROM_COLLECTOR_CAPTURE",
      required_fields: ["source_event_ts", "source_receive_ms", "source_timestamp_cutoff_ms", "local_ready_ms", "local_candidate_price", "local_candidate_side", "decision_deadline_ms"],
      missing_fields: causalMissing,
      interpretation: "This capture validates public benchmark/reprice instrumentation, not local synthetic strategy economics.",
    },
    research_decision: {
      engineering_shadow: forbiddenAuthority.status === "PASS" && manifestContract.status === "PASS" && roundContract.status === "PASS" && rtdsCoverageContract.status === "PASS" ? "GO" : "NO-GO",
      latency_aggregation_strategy: strategyGate ? "CONDITIONAL_RESEARCH_GO_CANDIDATE" : "NO-GO_PENDING_CAUSAL_JOIN_OR_GAP_REPAIR",
      economics: "NO-GO",
      live: "NO-GO",
    },
    caveats: [
      "Gamma public outcomes are labels only.",
      "Public CLOB depth is visible liquidity, not queue position or fill truth.",
      "A CLOB reconnect or missing source cutoff invalidates the affected round for strategy scoring.",
      "An open RTDS socket is not evidence of complete data; per-stream tail freshness is a terminal contract.",
    ],
    manifest_file_check: manifestFiles,
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const report = verify(args);
  const outputPath = args.output ? path.resolve(args.output) : path.join(path.resolve(args.runDir), "terminal_verification.json");
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ output: outputPath, verdict: report.verdict, terminal: report.terminal }, null, 2));
  if (!report.terminal || report.verdict === "INCOMPLETE_CAPTURE") process.exitCode = 2;
}

main();
