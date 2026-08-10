#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import {
  buildManifest,
  buildSummary,
  fileLineCount,
  forEachJsonl,
  readJsonl,
  sha256File,
} from "./collect_twap_boundary_shadow.mjs";

function parseArgs(argv) {
  const args = { runDir: null, collectorPath: null };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = () => {
      if (i + 1 >= argv.length) throw new Error(`missing value for ${arg}`);
      i += 1;
      return argv[i];
    };
    if (arg === "--run-dir") args.runDir = next();
    else if (arg === "--collector-path") args.collectorPath = next();
    else if (arg === "--help") {
      console.log("Usage: finalize_twap_boundary_shadow_capture.mjs --run-dir DIR --collector-path COLLECTOR");
      process.exit(0);
    } else throw new Error(`unknown argument: ${arg}`);
  }
  if (!args.runDir || !args.collectorPath) throw new Error("--run-dir and --collector-path are required");
  return args;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const runDir = path.resolve(args.runDir);
  const collectorPath = path.resolve(args.collectorPath);
  const requiredRaw = ["STARTED.json", "CHECKPOINT.json", "market_metadata.jsonl", "twap_ticks.jsonl", "book_events.jsonl", "boundary_observations.jsonl", "settlement_observations.jsonl", "run.log"];
  for (const name of requiredRaw) if (!fs.existsSync(path.join(runDir, name))) throw new Error(`missing raw evidence: ${name}`);
  if (fs.existsSync(path.join(runDir, "EXIT.json")) || fs.existsSync(path.join(runDir, "manifest.json")) || fs.existsSync(path.join(runDir, "summary.json"))) {
    throw new Error("run already has terminal artifacts; refusing to overwrite");
  }

  const started = readJson(path.join(runDir, "STARTED.json"));
  const checkpoint = readJson(path.join(runDir, "CHECKPOINT.json"));
  const metadataRows = readJsonl(path.join(runDir, "market_metadata.jsonl"));
  const twapTicks = readJsonl(path.join(runDir, "twap_ticks.jsonl"));
  const logRows = readJsonl(path.join(runDir, "run.log"));
  const exitLog = [...logRows].reverse().find((row) => row.message === "collector_exit");
  const endedAtMs = exitLog?.ts ? Date.parse(exitLog.ts) : fs.statSync(path.join(runDir, "book_events.jsonl")).mtimeMs;
  if (!Number.isFinite(endedAtMs)) throw new Error("cannot determine collector terminal time");

  const bookIds = new Set();
  forEachJsonl(path.join(runDir, "book_events.jsonl"), (row) => {
    if (row.asset_id != null) bookIds.add(String(row.asset_id));
  });
  const state = {
    args: { ...(started.args || {}), bookEmitMinIntervalMs: started.args?.bookEmitMinIntervalMs ?? 0 },
    outDir: runDir,
    startedAtMs: started.started_at_ms,
    metadata: new Map(metadataRows.map((row) => [row.slug, row])),
    twapTicks,
    books: new Map([...bookIds].map((assetId) => [assetId, {}])),
    counts: {
      metadata: fileLineCount(path.join(runDir, "market_metadata.jsonl")),
      twap_ticks: fileLineCount(path.join(runDir, "twap_ticks.jsonl")),
      book_events: fileLineCount(path.join(runDir, "book_events.jsonl")),
      boundaries: fileLineCount(path.join(runDir, "boundary_observations.jsonl")),
      settlements: fileLineCount(path.join(runDir, "settlement_observations.jsonl")),
    },
    gapEvents: checkpoint.gap_events || [],
    wsStates: checkpoint.ws_states || { rtds: "unknown", clob: "unknown" },
    wsReconnects: checkpoint.ws_reconnects || { rtds: null, clob: null },
  };
  const reason = "duration_elapsed_recovered_after_summary_failure";
  const summary = buildSummary(state, endedAtMs, reason);
  writeJson(path.join(runDir, "summary.json"), summary);
  writeJson(path.join(runDir, "CHECKPOINT.json"), { ...summary, checkpoint: true });
  const manifest = buildManifest(state, started, endedAtMs, reason, collectorPath);
  manifest.finalization = {
    mode: "recovery_finalizer",
    source_error: "collector summary failed with Node ERR_STRING_TOO_LONG while reading book_events.jsonl; raw JSONL preserved",
    finalizer_path: path.resolve(process.argv[1]),
    finalizer_sha256: sha256File(path.resolve(process.argv[1])),
  };
  writeJson(path.join(runDir, "manifest.json"), manifest);
  writeJson(path.join(runDir, "EXIT.json"), {
    exited_at: new Date(endedAtMs).toISOString(),
    terminal: true,
    reason,
    mode: "no-submit",
    live_orders_submitted: 0,
    credentials_loaded: false,
    open_runs: [],
    manifest: path.join(runDir, "manifest.json"),
    finalization: manifest.finalization,
  });
  console.log(JSON.stringify({ run_dir: runDir, reason, counts: state.counts, summary: path.join(runDir, "summary.json"), manifest: path.join(runDir, "manifest.json"), exit: path.join(runDir, "EXIT.json") }, null, 2));
}

main();
