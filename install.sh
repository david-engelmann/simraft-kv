#!/bin/sh
# Install prebuilt simraft-node and simraft-sim release binaries with
# mandatory SHA256SUMS verification prior to archive extraction.
set -eu

REPO="david-engelmann/simraft-kv"
DEFAULT_BASE_URL="https://github.com/${REPO}/releases"

BASE_URL="${SIMRAFT_BASE_URL:-$DEFAULT_BASE_URL}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/bin}"
VERSION="${SIMRAFT_VERSION:-}"
BIN_SELECTION="all"
CUSTOM_BASE_URL=0
TMP_DIR=""

cleanup() {
  if [ -n "${TMP_DIR:-}" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}

trap cleanup EXIT INT TERM HUP

err() {
  printf 'install.sh: error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: install.sh [OPTIONS]

Download, verify (SHA-256), and install simraft-kv release binaries.

Options:
  --version <vX.Y.Z>               Release tag to install (defaults to latest release)
  --install-dir <DIR>              Destination directory (default: $INSTALL_DIR or ~/.local/bin)
  --bin <all|simraft-node|simraft-sim>
                                   Binary selection to install (default: all)
  --base-url <URL>                 Base releases URL (default: https://github.com/david-engelmann/simraft-kv/releases)
  -h, --help                       Print this help message and exit
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -ge 2 ] || err "--version requires a value"
      VERSION="$2"
      shift 2
      ;;
    --version=*)
      VERSION="${1#*=}"
      shift
      ;;
    --install-dir)
      [ "$#" -ge 2 ] || err "--install-dir requires a value"
      INSTALL_DIR="$2"
      shift 2
      ;;
    --install-dir=*)
      INSTALL_DIR="${1#*=}"
      shift
      ;;
    --bin)
      [ "$#" -ge 2 ] || err "--bin requires a value"
      BIN_SELECTION="$2"
      shift 2
      ;;
    --bin=*)
      BIN_SELECTION="${1#*=}"
      shift
      ;;
    --base-url)
      [ "$#" -ge 2 ] || err "--base-url requires a value"
      BASE_URL="$2"
      CUSTOM_BASE_URL=1
      shift 2
      ;;
    --base-url=*)
      BASE_URL="${1#*=}"
      CUSTOM_BASE_URL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "unknown argument: $1 (pass --help for usage)"
      ;;
  esac
done

case "$BIN_SELECTION" in
  all)
    BINS="simraft-node simraft-sim"
    ;;
  simraft-node|simraft-sim)
    BINS="$BIN_SELECTION"
    ;;
  *)
    err "invalid --bin '$BIN_SELECTION' (expected 'all', 'simraft-node', or 'simraft-sim')"
    ;;
esac

[ -n "$INSTALL_DIR" ] || err "--install-dir must not be empty"
[ -n "$BASE_URL" ] || err "--base-url must not be empty"
BASE_URL="${BASE_URL%/}"

detect_target() {
  if [ -n "${SIMRAFT_TARGET:-}" ]; then
    printf '%s\n' "$SIMRAFT_TARGET"
    return 0
  fi

  uname_s="$(uname -s 2>/dev/null || printf 'unknown')"
  uname_m="$(uname -m 2>/dev/null || printf 'unknown')"

  case "$uname_s" in
    Linux)
      os_part="unknown-linux-gnu"
      ;;
    Darwin)
      os_part="apple-darwin"
      ;;
    *)
      err "unsupported operating system '$uname_s' (prebuilt archives support Linux and macOS; use cargo install on other platforms)"
      ;;
  esac

  case "$uname_m" in
    x86_64|amd64)
      arch_part="x86_64"
      ;;
    aarch64|arm64)
      arch_part="aarch64"
      ;;
    *)
      err "unsupported CPU architecture '$uname_m' (expected x86_64 or aarch64/arm64)"
      ;;
  esac

  printf '%s-%s\n' "$arch_part" "$os_part"
}

fetch_file() {
  src_url="$1"
  dest_path="$2"

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$src_url" -o "$dest_path"
    return $?
  fi

  case "$src_url" in
    file://*)
      local_src="${src_url#file://}"
      [ -f "$local_src" ] || return 1
      cp "$local_src" "$dest_path"
      return $?
      ;;
  esac

  if command -v wget >/dev/null 2>&1; then
    wget -qO "$dest_path" "$src_url"
    return $?
  fi

  err "neither curl nor wget is installed"
}

resolve_latest_tag() {
  if command -v curl >/dev/null 2>&1; then
    effective_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' "${BASE_URL}/latest" 2>/dev/null || true)"
    case "$effective_url" in
      */tag/v*)
        printf '%s\n' "${effective_url##*/tag/}"
        return 0
        ;;
    esac
    api_json="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null || true)"
  elif command -v wget >/dev/null 2>&1; then
    api_json="$(wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null || true)"
  else
    err "neither curl nor wget is installed"
  fi

  tag="$(printf '%s\n' "${api_json:-}" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
  [ -n "$tag" ] || err "failed to determine latest release tag; pass --version vX.Y.Z explicitly"
  printf '%s\n' "$tag"
}

compute_sha256() {
  file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | awk '{print tolower($1)}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file_path" | awk '{print tolower($1)}'
  else
    err "neither sha256sum nor shasum is installed; refusing unverified install"
  fi
}

lookup_expected_sha256() {
  sums_file="$1"
  archive_name="$2"
  awk -v target="$archive_name" '
    NF >= 2 {
      name = $2
      sub(/^\*/, "", name)
      sub(/^\.\//, "", name)
      if (name == target) {
        print tolower($1)
        exit
      }
    }
  ' "$sums_file"
}

validate_sha256_hex() {
  digest="$1"
  [ "${#digest}" -eq 64 ] || return 1
  case "$digest" in
    *[!0-9a-f]*)
      return 1
      ;;
  esac
  return 0
}

if [ -z "$VERSION" ]; then
  VERSION="$(resolve_latest_tag)"
fi

case "$VERSION" in
  v*)
    TAG="$VERSION"
    ;;
  *)
    TAG="v${VERSION}"
    ;;
esac

case "$TAG" in
  v*[!A-Za-z0-9._+-]*|v|*..* )
    err "invalid release tag '$TAG'"
    ;;
esac

TARGET="$(detect_target)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/simraft-install.XXXXXX")"

ASSET_BASE_URL="${BASE_URL}/download/${TAG}"
SUMS_PATH="${TMP_DIR}/SHA256SUMS"

if ! fetch_file "${ASSET_BASE_URL}/SHA256SUMS" "$SUMS_PATH" 2>/dev/null; then
  if [ "$CUSTOM_BASE_URL" -eq 1 ]; then
    case "$BASE_URL" in
      file://*)
        if fetch_file "${BASE_URL}/SHA256SUMS" "$SUMS_PATH" 2>/dev/null; then
          ASSET_BASE_URL="$BASE_URL"
        else
          err "failed to download SHA256SUMS from ${ASSET_BASE_URL}/SHA256SUMS or ${BASE_URL}/SHA256SUMS"
        fi
        ;;
      *)
        err "failed to download SHA256SUMS from ${ASSET_BASE_URL}/SHA256SUMS"
        ;;
    esac
  else
    err "failed to download SHA256SUMS from ${ASSET_BASE_URL}/SHA256SUMS"
  fi
fi

[ -s "$SUMS_PATH" ] || err "downloaded SHA256SUMS is empty"

# Download and cryptographically verify every requested archive BEFORE extracting any files.
for bin in $BINS; do
  archive="${bin}-${TAG}-${TARGET}.tar.gz"
  archive_path="${TMP_DIR}/${archive}"
  expected_sha="$(lookup_expected_sha256 "$SUMS_PATH" "$archive")"

  if [ -z "$expected_sha" ]; then
    err "archive '${archive}' is missing from SHA256SUMS"
  fi
  if ! validate_sha256_hex "$expected_sha"; then
    err "malformed SHA-256 digest in SHA256SUMS for '${archive}'"
  fi

  if ! fetch_file "${ASSET_BASE_URL}/${archive}" "$archive_path"; then
    err "failed to download ${ASSET_BASE_URL}/${archive}"
  fi

  actual_sha="$(compute_sha256 "$archive_path")"
  if ! validate_sha256_hex "$actual_sha"; then
    err "failed to compute valid SHA-256 digest for '${archive}'"
  fi

  if [ "$actual_sha" != "$expected_sha" ]; then
    err "SHA-256 checksum mismatch for '${archive}' (expected ${expected_sha}, got ${actual_sha})"
  fi
done

mkdir -p "$INSTALL_DIR"

for bin in $BINS; do
  archive="${bin}-${TAG}-${TARGET}.tar.gz"
  archive_path="${TMP_DIR}/${archive}"
  extract_dir="${TMP_DIR}/extract-${bin}"
  mkdir -p "$extract_dir"
  tar -xzf "$archive_path" -C "$extract_dir"

  extracted_bin="${extract_dir}/${bin}"
  [ -f "$extracted_bin" ] || err "expected binary '${bin}' missing at archive root of '${archive}'"

  staged_dest="${INSTALL_DIR}/.${bin}.tmp.$$"
  cp "$extracted_bin" "$staged_dest"
  chmod 0755 "$staged_dest"
  mv -f "$staged_dest" "${INSTALL_DIR}/${bin}"
  printf 'installed %s (%s, %s) -> %s/%s\n' "$bin" "$TAG" "$TARGET" "$INSTALL_DIR" "$bin"
done
