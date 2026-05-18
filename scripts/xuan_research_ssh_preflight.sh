#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ART_DIR="$ROOT/xuan_research_artifacts"
STATE_FILE="$ART_DIR/ssh_preflight_state.env"
TARGET_FILE="$ART_DIR/ssh_preflight_target.env"
LATEST_NOTE="$ART_DIR/ssh_unavailable_latest.md"
ENV_DIAG_FILE="$ART_DIR/ssh_preflight_env_diag.txt"

KEY="${XUAN_RESEARCH_SSH_KEY:-$HOME/.ssh/polymarket-Ireland.pem}"
CANONICAL="ec2-3-248-230-60.eu-west-1.compute.amazonaws.com"
DIRECT_IP="3.248.230.60"
EXPECTED_HOSTNAME="ip-172-31-41-70"
EXPECTED_PRIVATE_IP="172.31.41.70"
FORCE=0
EXTENDED_RETRY_COUNT="${XUAN_RESEARCH_SSH_EXTENDED_RETRY_COUNT:-3}"
EXTENDED_RETRY_BASE_SECS="${XUAN_RESEARCH_SSH_EXTENDED_RETRY_BASE_SECS:-15}"

if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
fi

mkdir -p "$ART_DIR"

read_state_value() {
  local key="$1"
  [[ -f "$STATE_FILE" ]] || return 0
  awk -F= -v k="$key" '$1 == k {print substr($0, index($0, "=") + 1)}' "$STATE_FILE" | tail -n 1
}

write_state() {
  local status="$1"
  local fail_count="$2"
  local next_retry_epoch="$3"
  local label="$4"
  local message="$5"
  cat > "$STATE_FILE" <<EOF
status=$status
fail_count=$fail_count
next_retry_epoch=$next_retry_epoch
last_label=$label
last_message=$message
updated_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
EOF
}

write_target() {
  local label="$1"
  local target="$2"
  local host="$3"
  local ips="$4"
  local host_key_alias="${5:-}"
  cat > "$TARGET_FILE" <<EOF
target=$target
target_label=$label
ssh_host_key_alias=$host_key_alias
verified_hostname=$host
verified_ips=$ips
verified_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
EOF
}

run_identity_check() {
  local label="$1"
  local target="$2"
  local host_alias="${3:-}"
  local out="$ART_DIR/ssh_preflight_${label}.out"
  local err="$ART_DIR/ssh_preflight_${label}.err"
  local opts=(
    -i "$KEY"
    -o IdentitiesOnly=yes
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=accept-new
  )
  if [[ -n "$host_alias" ]]; then
    opts+=(
      -o "HostKeyAlias=$host_alias"
      -o CheckHostIP=no
    )
  fi
  rm -f "$out" "$err"
  ssh "${opts[@]}" "ubuntu@$target" 'hostname; hostname -I' >"$out" 2>"$err"
}

identity_ok() {
  local label="$1"
  local out="$ART_DIR/ssh_preflight_${label}.out"
  local host ips
  host="$(sed -n '1p' "$out" | tr -d '\r')"
  ips="$(sed -n '2p' "$out" | tr -d '\r')"
  [[ "$host" == "$EXPECTED_HOSTNAME" && "$ips" == *"$EXPECTED_PRIVATE_IP"* ]]
}

emit_ok() {
  local label="$1"
  local target="$2"
  local host_key_alias="${3:-}"
  local out="$ART_DIR/ssh_preflight_${label}.out"
  local host ips
  host="$(sed -n '1p' "$out" | tr -d '\r')"
  ips="$(sed -n '2p' "$out" | tr -d '\r')"
  write_target "$label" "$target" "$host" "$ips" "$host_key_alias"
  write_state "OK" 0 0 "$label" "ssh_available"
  rm -f "$LATEST_NOTE"
  printf 'status=OK\ntarget=%s\ntarget_label=%s\nverified_hostname=%s\nverified_ips=%s\n' \
    "$target" "$label" "$host" "$ips"
}

resolve_canonical_targets() {
  python3 - "$CANONICAL" <<'PY' 2>/dev/null || true
import socket
import sys

host = sys.argv[1]
seen = set()
for item in socket.getaddrinfo(host, 22, proto=socket.IPPROTO_TCP):
    ip = item[4][0]
    if ip not in seen:
        seen.add(ip)
        print(ip)
PY
}

try_python_resolved_targets() {
  local attempt_suffix="${1:-}"
  local idx=0
  local resolved
  while IFS= read -r resolved; do
    [[ -n "$resolved" ]] || continue
    idx=$((idx + 1))
    local label="canonical_resolved_${idx}${attempt_suffix}"
    if run_identity_check "$label" "$resolved" "$CANONICAL" && identity_ok "$label"; then
      emit_ok "$label" "$resolved" "$CANONICAL"
      return 0
    fi
  done < <(resolve_canonical_targets)
  return 1
}

write_env_diag() {
  {
    printf 'updated_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'pwd=%s\n' "$(pwd)"
    printf 'user=%s\n' "$(id -un 2>/dev/null || true)"
    printf 'uid=%s\n' "$(id -u 2>/dev/null || true)"
    printf 'home=%s\n' "${HOME:-}"
    printf 'key=%s\n' "$KEY"
    if [[ -e "$KEY" ]]; then
      ls -l "$KEY"
    else
      printf 'key_missing=1\n'
    fi
    printf 'script_sha256='
    shasum -a 256 "$0" 2>/dev/null || true
    printf 'python_resolved_targets='
    resolve_canonical_targets | paste -sd, - || true
    printf '\n'
    command -v ssh || true
    ssh -V 2>&1 || true
  } >"$ENV_DIAG_FILE" 2>&1
}

now_epoch="$(date +%s)"
next_retry_epoch="$(read_state_value next_retry_epoch)"
last_status="$(read_state_value status)"
if [[ "$FORCE" != "1" && "${last_status:-}" == "UNAVAILABLE" && "${next_retry_epoch:-0}" =~ ^[0-9]+$ && "$next_retry_epoch" -gt "$now_epoch" ]]; then
  printf 'status=BACKOFF\nnext_retry_epoch=%s\n' "$next_retry_epoch"
  exit 75
fi

if run_identity_check canonical "$CANONICAL" && identity_ok canonical; then
  emit_ok canonical "$CANONICAL"
  exit 0
fi

sleep 10
if run_identity_check canonical_retry "$CANONICAL" && identity_ok canonical_retry; then
  emit_ok canonical_retry "$CANONICAL"
  exit 0
fi

if try_python_resolved_targets ""; then
  exit 0
fi

if run_identity_check direct_ip "$DIRECT_IP" && identity_ok direct_ip; then
  emit_ok direct_ip "$DIRECT_IP"
  exit 0
fi

if [[ "$EXTENDED_RETRY_COUNT" =~ ^[0-9]+$ && "$EXTENDED_RETRY_BASE_SECS" =~ ^[0-9]+$ ]]; then
  for ((attempt = 1; attempt <= EXTENDED_RETRY_COUNT; attempt++)); do
    sleep "$((attempt * EXTENDED_RETRY_BASE_SECS))"
    label="canonical_ext_${attempt}"
    if run_identity_check "$label" "$CANONICAL" && identity_ok "$label"; then
      emit_ok "$label" "$CANONICAL"
      exit 0
    fi
    if try_python_resolved_targets "_ext_${attempt}"; then
      exit 0
    fi
    label="direct_ip_ext_${attempt}"
    if run_identity_check "$label" "$DIRECT_IP" && identity_ok "$label"; then
      emit_ok "$label" "$DIRECT_IP"
      exit 0
    fi
  done
fi

prev_fail_count="$(read_state_value fail_count)"
if [[ ! "${prev_fail_count:-0}" =~ ^[0-9]+$ || "${last_status:-}" != "UNAVAILABLE" ]]; then
  prev_fail_count=0
fi
fail_count=$((prev_fail_count + 1))
case "$fail_count" in
  1) backoff_secs=900 ;;
  2) backoff_secs=1800 ;;
  *) backoff_secs=3600 ;;
esac
next_retry=$((now_epoch + backoff_secs))
write_state "UNAVAILABLE" "$fail_count" "$next_retry" "all" "ssh_unavailable"
write_env_diag

cat > "$LATEST_NOTE" <<EOF
# xuan-research remote verifier SSH unavailable

Updated UTC: $(date -u '+%Y-%m-%dT%H:%M:%SZ')

This is an infrastructure availability state, not a strategy verdict.

Canonical target: $CANONICAL
Direct-IP fallback: $DIRECT_IP
Expected identity: hostname=$EXPECTED_HOSTNAME private_ip=$EXPECTED_PRIVATE_IP

Fail count in current streak: $fail_count
Next retry epoch: $next_retry

Latest stderr files:
- $ART_DIR/ssh_preflight_canonical.err
- $ART_DIR/ssh_preflight_canonical_retry.err
- $ART_DIR/ssh_preflight_canonical_resolved_1.err
- $ART_DIR/ssh_preflight_direct_ip.err

Environment diagnostic:
- $ENV_DIAG_FILE

Automation should not create timestamped SSH-unavailable notes while this rolling note is current.
EOF

printf 'status=UNAVAILABLE\nfail_count=%s\nnext_retry_epoch=%s\nlatest_note=%s\n' \
  "$fail_count" "$next_retry" "$LATEST_NOTE"
exit 75
