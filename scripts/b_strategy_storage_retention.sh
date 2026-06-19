#!/usr/bin/env bash
set -euo pipefail

STAGING_ROOT="${STAGING_ROOT:-/home/ubuntu/b_strategy_staging}"
OWNER_B_ROOT="${OWNER_B_ROOT:-/srv/pm_as_ofi/owner-lines/B}"
MANIFEST_ROOT="${MANIFEST_ROOT:-${STAGING_ROOT}/storage_cleanup_manifests}"
ORIGINAL_ARGS=("$@")
KEEP_STAGING_WORKTREES="${KEEP_STAGING_WORKTREES:-16}"
KEEP_RUNTIME_BINS="${KEEP_RUNTIME_BINS:-8}"
RAW_MIN_SIZE_MB="${RAW_MIN_SIZE_MB:-50}"
RAW_MIN_AGE_MIN="${RAW_MIN_AGE_MIN:-10}"
DRY_RUN=1
PRUNE_WORKTREES=1
QUIET=0

usage() {
  cat <<'USAGE'
Usage: b_strategy_storage_retention.sh [--dry-run|--apply] [options]

B-owned storage retention only. This script:
  - removes Rust target caches from B-owned staging worktrees;
  - removes B owner-line build cache / packet target caches;
  - zstd-compresses inactive large B raw WS JSONL files;
  - prunes old B-owned staging worktree directories, preserving newest N;
  - prunes old preserved runtime binaries, preserving newest N.

It does not touch service directories, C artifacts, secrets, or any effectful
trading path.

Options:
  --apply                         perform changes; default is --dry-run
  --dry-run                       log intended changes only
  --keep-staging-worktrees N      keep newest N B staging worktree dirs
  --keep-runtime-bins N           keep newest N runtime binaries
  --raw-min-size-mb N             compress raw files larger than N MB
  --raw-min-age-min N             compress only raw files older than N minutes
  --no-prune-worktrees            skip old worktree pruning
  --manifest-root PATH            write JSONL manifests under PATH
  --quiet                         suppress human summary
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      DRY_RUN=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --keep-staging-worktrees)
      KEEP_STAGING_WORKTREES="$2"
      shift 2
      ;;
    --keep-runtime-bins)
      KEEP_RUNTIME_BINS="$2"
      shift 2
      ;;
    --raw-min-size-mb)
      RAW_MIN_SIZE_MB="$2"
      shift 2
      ;;
    --raw-min-age-min)
      RAW_MIN_AGE_MIN="$2"
      shift 2
      ;;
    --no-prune-worktrees)
      PRUNE_WORKTREES=0
      shift
      ;;
    --manifest-root)
      MANIFEST_ROOT="$2"
      shift 2
      ;;
    --quiet)
      QUIET=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$KEEP_STAGING_WORKTREES" =~ ^[0-9]+$ ]] || [[ "$KEEP_STAGING_WORKTREES" -lt 1 ]]; then
  echo "KEEP_STAGING_WORKTREES must be a positive integer" >&2
  exit 2
fi
if ! [[ "$KEEP_RUNTIME_BINS" =~ ^[0-9]+$ ]] || [[ "$KEEP_RUNTIME_BINS" -lt 1 ]]; then
  echo "KEEP_RUNTIME_BINS must be a positive integer" >&2
  exit 2
fi
if ! [[ "$RAW_MIN_SIZE_MB" =~ ^[0-9]+$ ]] || [[ "$RAW_MIN_SIZE_MB" -lt 1 ]]; then
  echo "RAW_MIN_SIZE_MB must be a positive integer" >&2
  exit 2
fi
if ! [[ "$RAW_MIN_AGE_MIN" =~ ^[0-9]+$ ]] || [[ "$RAW_MIN_AGE_MIN" -lt 1 ]]; then
  echo "RAW_MIN_AGE_MIN must be a positive integer" >&2
  exit 2
fi

if [[ "${B_STRATEGY_STORAGE_RETENTION_LOCKED:-0}" != "1" ]]; then
  export B_STRATEGY_STORAGE_RETENTION_LOCKED=1
  if command -v flock >/dev/null 2>&1; then
    exec flock -n /tmp/b_strategy_storage_retention.lock "$0" "${ORIGINAL_ARGS[@]}"
  fi
fi

mkdir -p "$MANIFEST_ROOT"
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
MODE="dry_run"
if [[ "$DRY_RUN" -eq 0 ]]; then
  MODE="apply"
fi
MANIFEST="${MANIFEST_ROOT}/b_strategy_storage_retention_${MODE}_${RUN_TS}.jsonl"

json_escape() {
  sed 's/\\/\\\\/g; s/"/\\"/g' <<<"$1" | tr -d '\n'
}

log_event() {
  local event="$1"
  local detail="$2"
  printf '{"ts":"%s","mode":"%s","event":"%s","detail":%s}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$MODE" "$event" "$detail" >>"$MANIFEST"
}

json_path_detail() {
  local path="$1"
  local size_kb="${2:-}"
  if [[ -n "$size_kb" ]]; then
    printf '{"path":"%s","size_kb":%s}' "$(json_escape "$path")" "$size_kb"
  else
    printf '{"path":"%s"}' "$(json_escape "$path")"
  fi
}

sudo_if_needed() {
  if [[ -e "$1" && -r "$1" && -w "$1" ]]; then
    return 1
  fi
  return 0
}

du_kb() {
  local path="$1"
  if [[ -e "$path" && -r "$path" ]]; then
    du -sk "$path" 2>/dev/null | awk '{print $1}'
  else
    sudo -n du -sk "$path" 2>/dev/null | awk '{print $1}'
  fi
}

rm_path() {
  local path="$1"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    return 0
  fi
  if [[ -e "$path" && -w "$(dirname "$path")" ]]; then
    rm -rf -- "$path"
  else
    sudo -n rm -rf -- "$path"
  fi
}

compress_raw_file() {
  local file="$1"
  local orig_bytes zst_bytes detail

  if [[ -e "${file}.zst" ]]; then
    detail="$(printf '{"path":"%s","reason":"zst_exists"}' "$(json_escape "$file")")"
    log_event "skip_raw_compress" "$detail"
    return 0
  fi
  if command -v fuser >/dev/null 2>&1 && sudo -n fuser -s "$file" 2>/dev/null; then
    detail="$(printf '{"path":"%s","reason":"file_open"}' "$(json_escape "$file")")"
    log_event "skip_raw_compress" "$detail"
    return 0
  fi

  orig_bytes="$(sudo -n stat -c '%s' "$file" 2>/dev/null || stat -c '%s' "$file" 2>/dev/null || echo 0)"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    sudo -n zstd -T0 -3 --rm --quiet "$file"
    zst_bytes="$(sudo -n stat -c '%s' "${file}.zst" 2>/dev/null || echo 0)"
  else
    zst_bytes=0
  fi
  detail="$(printf '{"path":"%s","orig_bytes":%s,"zst_bytes":%s}' "$(json_escape "$file")" "$orig_bytes" "$zst_bytes")"
  log_event "compress_raw_ws_jsonl" "$detail"
}

log_event "start" "$(printf '{"staging_root":"%s","owner_b_root":"%s","keep_staging_worktrees":%s,"keep_runtime_bins":%s,"raw_min_size_mb":%s,"raw_min_age_min":%s}' "$(json_escape "$STAGING_ROOT")" "$(json_escape "$OWNER_B_ROOT")" "$KEEP_STAGING_WORKTREES" "$KEEP_RUNTIME_BINS" "$RAW_MIN_SIZE_MB" "$RAW_MIN_AGE_MIN")"

if [[ -d "$STAGING_ROOT" ]]; then
  while IFS= read -r -d '' target_dir; do
    size_kb="$(du_kb "$target_dir")"
    log_event "remove_staging_repo_target" "$(json_path_detail "$target_dir" "${size_kb:-0}")"
    rm_path "$target_dir"
  done < <(find "$STAGING_ROOT" -path "$STAGING_ROOT/*/repo/target" -type d -prune -print0 2>/dev/null)
fi

if sudo -n test -d "$OWNER_B_ROOT" 2>/dev/null; then
  if sudo -n test -d "$OWNER_B_ROOT/cache" 2>/dev/null; then
    while IFS= read -r -d '' cache_child; do
      size_kb="$(du_kb "$cache_child")"
      log_event "remove_owner_b_cache_child" "$(json_path_detail "$cache_child" "${size_kb:-0}")"
      rm_path "$cache_child"
    done < <(sudo -n find "$OWNER_B_ROOT/cache" -mindepth 1 -maxdepth 1 -print0 2>/dev/null)
  fi

  while IFS= read -r -d '' target_dir; do
    size_kb="$(du_kb "$target_dir")"
    log_event "remove_owner_b_packet_target" "$(json_path_detail "$target_dir" "${size_kb:-0}")"
    rm_path "$target_dir"
  done < <(sudo -n find "$OWNER_B_ROOT/packets" \
    \( -path "$OWNER_B_ROOT/packets/*/runtime/target" -o -path "$OWNER_B_ROOT/packets/*/target" \) \
    -type d -prune -print0 2>/dev/null || true)

  while IFS= read -r -d '' raw_file; do
    compress_raw_file "$raw_file"
  done < <(sudo -n find "$OWNER_B_ROOT/runs" \
    -type f -name raw_ws_book_snapshots.jsonl \
    -size +"${RAW_MIN_SIZE_MB}"M -mmin +"${RAW_MIN_AGE_MIN}" -print0 2>/dev/null || true)
fi

if [[ "$PRUNE_WORKTREES" -eq 1 && -d "$STAGING_ROOT" ]]; then
  mapfile -t staging_dirs < <(find "$STAGING_ROOT" -maxdepth 1 -mindepth 1 -type d \
    -name 'b_strategy_*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | awk '{ $1=""; sub(/^ /, ""); print }')
  idx=0
  for worktree_dir in "${staging_dirs[@]}"; do
    idx=$((idx + 1))
    if [[ "$idx" -le "$KEEP_STAGING_WORKTREES" ]]; then
      log_event "keep_staging_worktree" "$(printf '{"path":"%s","rank":%s}' "$(json_escape "$worktree_dir")" "$idx")"
      continue
    fi
    size_kb="$(du_kb "$worktree_dir")"
    log_event "prune_staging_worktree" "$(printf '{"path":"%s","rank":%s,"size_kb":%s}' "$(json_escape "$worktree_dir")" "$idx" "${size_kb:-0}")"
    rm_path "$worktree_dir"
  done
fi

runtime_dir="$STAGING_ROOT/toolchains/runtime_bins"
if [[ -d "$runtime_dir" ]]; then
  mapfile -t runtime_bins < <(find "$runtime_dir" -maxdepth 1 -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | awk '{ $1=""; sub(/^ /, ""); print }')
  idx=0
  for bin_path in "${runtime_bins[@]}"; do
    idx=$((idx + 1))
    if [[ "$idx" -le "$KEEP_RUNTIME_BINS" ]]; then
      log_event "keep_runtime_bin" "$(printf '{"path":"%s","rank":%s}' "$(json_escape "$bin_path")" "$idx")"
      continue
    fi
    size_kb="$(du_kb "$bin_path")"
    log_event "prune_runtime_bin" "$(printf '{"path":"%s","rank":%s,"size_kb":%s}' "$(json_escape "$bin_path")" "$idx" "${size_kb:-0}")"
    rm_path "$bin_path"
  done
fi

df_line="$(df -hT / | awk 'NR==2 {print $1" "$2" "$3" "$4" "$5" "$6" "$7}')"
log_event "finish" "$(printf '{"df_root":"%s","manifest":"%s"}' "$(json_escape "$df_line")" "$(json_escape "$MANIFEST")")"

if [[ "$QUIET" -eq 0 ]]; then
  echo "mode=$MODE"
  echo "manifest=$MANIFEST"
  echo "$df_line"
fi
