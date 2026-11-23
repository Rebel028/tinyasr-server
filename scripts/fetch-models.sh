#!/bin/sh
# Fetch the model files from tinyasr and verify them. The rev is the one the library is pinned to
# in Cargo.toml, so the weights and the code cannot drift apart; a new rev needs new sums below.
# Usage: scripts/fetch-models.sh [dir]   (default: models)
set -eu
REV=$(sed -n 's/^tinyasr = .*rev = "\([0-9a-f]\{40\}\)".*/\1/p' "$(dirname "$0")/../Cargo.toml")
[ -n "$REV" ] || { echo "no tinyasr rev in Cargo.toml" >&2; exit 1; }
DIR=${1:-models}
mkdir -p "$DIR"
cd "$DIR"
for f in en-ru-uk.tzf silero_vad.onnx; do
  curl -fsSL "https://raw.githubusercontent.com/dkaraush/tinyasr/$REV/models/$f.gz" | gunzip > "$f"
done
sha256sum -c - <<SUMS
78ab8bfdf294829dcc804ec9403283bb9ce4e69483ea59812c49ea5934b7ee69  en-ru-uk.tzf
1a153a22f4509e292a94e67d6f9b85e8deb25b4988682b7e174c65279d8788e3  silero_vad.onnx
SUMS
