#!/bin/sh
# Fetch model files from tinyasr and verify them. The rev is the one the library is pinned to in
# Cargo.toml, so the weights and the code cannot drift apart; a new rev needs new sums below.
# A file that is already there with the right checksum is not fetched again.
# Usage: scripts/fetch-models.sh [dir [file...]]
#   dir   default: models
#   file  default: en-ru-uk.tzf silero_vad.onnx tinylid.bin (what the tests need); "all": every file below.
#         A name that is not below must already be in dir (your own model); it is used unchecked.
set -eu
REV=$(sed -n 's/^tinyasr = .*rev = "\([0-9a-f]\{40\}\)".*/\1/p' "$(dirname "$0")/../Cargo.toml")
[ -n "$REV" ] || { echo "no tinyasr rev in Cargo.toml" >&2; exit 1; }
SUMS=$(cat <<SUMS
0725f1c10bd87b891e3c29f6a36ef3fa9bcdb9341279b576b53509b9008b6b72  ar.tzf
93d31b5893148bc52012611ad0075fd84d1b51eee5b32bac8fca39fa1c8afdf3  en-es-it-pt.tzf
35464d05b0f542c898a1194fb7d5abcb99f2743760abcf4dc48934e08a0583c8  en-fa.tzf
5ab914b9d8e694ebfbb97483e732b0988ba2921baf4d36ce65f75182ca8d92d1  en-km.tzf
78ab8bfdf294829dcc804ec9403283bb9ce4e69483ea59812c49ea5934b7ee69  en-ru-uk.tzf
18d414e8063fc51930fde88790432d1072043f8a23962c494371471d3cdbb4dc  fr-de.tzf
6cf71c6dff66da4e56b82cba4aa1b2042674cd3da3ba599e84e9fdf9b3533b3b  id-ms.tzf
1a153a22f4509e292a94e67d6f9b85e8deb25b4988682b7e174c65279d8788e3  silero_vad.onnx
866dcbc28e0c02df2ef06bed564d0b94ccb7c680baa835fcc3f06ab01e7c123b  tinylid.bin
1d792a998e99f902e9fb5222efcbf9c0672bcd933f7b547f796dc5e29586058a  uz.tzf
SUMS
)
DIR=${1:-models}
[ $# -eq 0 ] || shift
[ $# -gt 0 ] || set -- en-ru-uk.tzf silero_vad.onnx tinylid.bin
# shellcheck disable=SC2046 # the names have no spaces
[ "$1" != all ] || set -- $(echo "$SUMS" | cut -d' ' -f3)
mkdir -p "$DIR"
cd "$DIR"
for f; do
  sum=$(echo "$SUMS" | awk -v f="$f" '$2 == f { print $1 }')
  if [ -z "$sum" ]; then
    [ -f "$f" ] || { echo "$f: not a tinyasr model at this rev and not in $DIR" >&2; exit 1; }
    echo "$f: own file, not checked"
    continue
  fi
  if [ -f "$f" ] && echo "$sum  $f" | sha256sum -c - >/dev/null 2>&1; then
    echo "$f: present"
    continue
  fi
  curl -fsSL "https://raw.githubusercontent.com/dkaraush/tinyasr/$REV/models/$f.gz" | gunzip > "$f.part" \
    || { echo "$f: download failed (the first start with a new file needs the network)" >&2; exit 1; }
  echo "$sum  $f.part" | sha256sum -c - >/dev/null || { echo "$f: checksum mismatch" >&2; exit 1; }
  mv "$f.part" "$f"
  echo "$f: fetched"
done
