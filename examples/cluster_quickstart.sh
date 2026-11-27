#!/bin/sh
# End-to-end CLI quickstart for simraft-sim and simraft-node.
# Runs deterministic chaos simulation and shell completion generation by default
# (--sim-only), and optionally boots a temporary 3-node localhost TCP cluster
# when --live-tcp is supplied.
set -eu

SCRIPT_DIR="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH='' cd -- "${SCRIPT_DIR}/.." && pwd)"

MODE="sim-only"
WORK_DIR=""
PID_1=""
PID_2=""
PID_3=""

cleanup() {
  for pid in "$PID_1" "$PID_2" "$PID_3"; do
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$PID_1" "$PID_2" "$PID_3"; do
    if [ -n "$pid" ]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [ -n "$WORK_DIR" ] && [ -d "$WORK_DIR" ]; then
    rm -rf "$WORK_DIR"
  fi
}

trap cleanup EXIT INT TERM HUP

usage() {
  cat <<'EOF'
Usage: sh examples/cluster_quickstart.sh [--sim-only | --live-tcp]

Options:
  --sim-only   Run deterministic chaos sweep and shell completions check (default)
  --live-tcp   Also boot a 3-node localhost TCP cluster and run put/get/delete
  -h, --help   Show this help message and exit
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --sim-only)
      MODE="sim-only"
      shift
      ;;
    --live-tcp)
      MODE="live-tcp"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'cluster_quickstart.sh: unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

locate_bin() {
  bin_name="$1"
  override_val="$2"
  if [ -n "$override_val" ] && [ -x "$override_val" ]; then
    printf '%s\n' "$override_val"
    return 0
  fi
  for candidate in \
    "${REPO_ROOT}/target/release/${bin_name}" \
    "${REPO_ROOT}/target/debug/${bin_name}" \
    "${REPO_ROOT}/target/release/${bin_name}.exe" \
    "${REPO_ROOT}/target/debug/${bin_name}.exe"
  do
    if [ -f "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  if command -v "$bin_name" >/dev/null 2>&1; then
    command -v "$bin_name"
    return 0
  fi
  printf 'cluster_quickstart.sh: binary %s not found; build with cargo build --bins\n' "$bin_name" >&2
  exit 1
}

SIM_BIN="$(locate_bin simraft-sim "${SIMRAFT_SIM_BIN:-}")"
NODE_BIN="$(locate_bin simraft-node "${SIMRAFT_NODE_BIN:-}")"

printf '[quickstart] running 3-seed chaos sweep via %s\n' "$SIM_BIN"
sim_json="$("$SIM_BIN" --seed 912 --seeds 3 --chaos --json)"
printf '%s\n' "$sim_json"

line_count="$(printf '%s\n' "$sim_json" | grep -c '"passed":true')"
if [ "$line_count" -ne 3 ]; then
  printf 'cluster_quickstart.sh: expected 3 passing JSON reports, got %s\n' "$line_count" >&2
  exit 1
fi

comp_out="$("$NODE_BIN" completions bash)"
case "$comp_out" in
  *simraft-node*) ;;
  *)
    printf 'cluster_quickstart.sh: unexpected bash completion output\n' >&2
    exit 1
    ;;
esac
printf '[quickstart] verified deterministic chaos sweep and bash completions\n'

if [ "$MODE" != "live-tcp" ]; then
  exit 0
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/simraft-quickstart.XXXXXX")"
mkdir -p "${WORK_DIR}/n1" "${WORK_DIR}/n2" "${WORK_DIR}/n3"

if command -v python3 >/dev/null 2>&1; then
  ports="$(python3 - <<'PY'
import socket
socks = []
for _ in range(3):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    socks.append(s)
print(" ".join(str(s.getsockname()[1]) for s in socks))
for s in socks:
    s.close()
PY
)"
  PORT_1="${ports%% *}"
  rest="${ports#* }"
  PORT_2="${rest%% *}"
  PORT_3="${rest#* }"
else
  PORT_1=17101
  PORT_2=17102
  PORT_3=17103
fi

ADDR_1="127.0.0.1:${PORT_1}"
ADDR_2="127.0.0.1:${PORT_2}"
ADDR_3="127.0.0.1:${PORT_3}"

printf '[quickstart] booting 3-node cluster at %s, %s, %s\n' "$ADDR_1" "$ADDR_2" "$ADDR_3"
"$NODE_BIN" serve --id 1 --data-dir "${WORK_DIR}/n1" --tick-ms 25 \
  --member "1@${ADDR_1}" --member "2@${ADDR_2}" --member "3@${ADDR_3}" \
  >"${WORK_DIR}/n1.log" 2>&1 &
PID_1=$!

"$NODE_BIN" serve --id 2 --data-dir "${WORK_DIR}/n2" --tick-ms 25 \
  --member "1@${ADDR_1}" --member "2@${ADDR_2}" --member "3@${ADDR_3}" \
  >"${WORK_DIR}/n2.log" 2>&1 &
PID_2=$!

"$NODE_BIN" serve --id 3 --data-dir "${WORK_DIR}/n3" --tick-ms 25 \
  --member "1@${ADDR_1}" --member "2@${ADDR_2}" --member "3@${ADDR_3}" \
  >"${WORK_DIR}/n3.log" 2>&1 &
PID_3=$!

put_res="$("$NODE_BIN" client --server "$ADDR_1" put greeting hello)"
[ "$put_res" = "ok" ] || { printf 'expected put ok, got %s\n' "$put_res" >&2; exit 1; }

get_res="$("$NODE_BIN" client --server "$ADDR_2" get greeting)"
[ "$get_res" = "hello" ] || { printf 'expected get hello, got %s\n' "$get_res" >&2; exit 1; }

del_res="$("$NODE_BIN" client --server "$ADDR_3" delete greeting)"
[ "$del_res" = "ok" ] || { printf 'expected delete ok, got %s\n' "$del_res" >&2; exit 1; }

rc=0
"$NODE_BIN" client --server "$ADDR_1" get greeting >/dev/null 2>&1 || rc=$?
if [ "$rc" -ne 1 ]; then
  printf 'expected exit code 1 for deleted key, got %s\n' "$rc" >&2
  exit 1
fi

printf '[quickstart] live 3-node TCP put/get/delete verified cleanly\n'
