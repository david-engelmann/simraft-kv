#!/usr/bin/env bash
# Stage the SUMMARY-referenced docs/ pages into book/src/docs/ so mdBook builds
# them as real in-site pages, and rewrite cross-tree links so the published site
# and the mdbook-linkcheck gate resolve cleanly.
#
# Canonical simraft-kv docs live in docs/ as GitHub-native Markdown. Maintainer
# history and process files (DECISIONS.md, docs/ROADMAP.md, docs/README.md) stay
# out of the published book and are linked directly on GitHub.
#
# Output dir book/src/docs/ is generated and git-ignored. Run before `mdbook build book`.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
src_docs="$repo_root/book/src/docs"

rm -rf "$src_docs"
mkdir -p "$src_docs/media"

published_pages=(
  "ARCHITECTURE"
  "INVARIANTS"
  "COMPARISON"
  "FAQ"
)
for name in "${published_pages[@]}"; do
  cp "$repo_root/docs/$name.md" "$src_docs/$name.md"
done

cp "$repo_root"/docs/media/*.svg "$src_docs/media/"
cp "$repo_root"/docs/media/*.png "$src_docs/media/"
cp "$repo_root"/docs/media/*.gif "$src_docs/media/"

export GH="https://github.com/david-engelmann/simraft-kv/blob/main"
export GHTREE="https://github.com/david-engelmann/simraft-kv/tree/main"

find "$src_docs" -name '*.md' -print0 | while IFS= read -r -d '' f; do
  # 1) Repo-root files outside the book -> canonical GitHub blob URLs
  perl -pi -e 's{\]\(\.\./(README\.md|DECISIONS\.md|SECURITY\.md|CHANGELOG\.md|CONTRIBUTING\.md|CODE_OF_CONDUCT\.md|AGENTS\.md|CITATION\.cff|Cargo\.toml|pyproject\.toml|deny\.toml|install\.sh|Makefile|LICENSE-MIT|LICENSE-APACHE|rust-toolchain\.toml)(#[^)]*)?\)}{]($ENV{GH}/$1$2)}g' "$f"

  # 2) Maintainer and index pages in docs/ kept out of SUMMARY -> GitHub blob URLs
  perl -pi -e 's{\]\((?:\.\./docs/)?(ROADMAP\.md|README\.md)(#[^)]*)?\)}{]($ENV{GH}/docs/$1$2)}g' "$f"

  # 3) Directory links to top-level trees -> GitHub tree URLs
  perl -pi -e 's{\]\(\.\./(examples|fuzz|python|simraft-core|simraft-wire|simraft-kv|simraft-node|simraft-sim)/?\)}{]($ENV{GHTREE}/$1)}g' "$f"

  # 4) File links into top-level workspace directories -> GitHub blob URLs
  perl -pi -e 's{\]\(\.\./(examples|fuzz|python|simraft-core|simraft-wire|simraft-kv|simraft-node|simraft-sim|\.github)/([^)]+)\)}{]($ENV{GH}/$1/$2)}g' "$f"
done

echo "staged $(find "$src_docs" -name '*.md' | wc -l | tr -d ' ') docs pages into book/src/docs/"
