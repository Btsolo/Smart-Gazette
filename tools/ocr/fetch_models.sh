#!/bin/sh
# Figure OCR models for the app (service.PpOcr, docs/specs/figures.md), for a
# server or CI: downloads the PP-OCRv6 small models from RapidOCR's official
# host (Apache-2.0), checks their SHA-256 and adds the character list kept in
# git (tools/ocr/PP-OCRv6_rec_small.keys.txt - see tools/fetch_ocr_models.py
# for why it is a separate file).
#   sh tools/ocr/fetch_models.sh [target dir, default models/ppocr]
set -eu
DIR="${1:-models/ppocr}"
BASE="https://www.modelscope.cn/models/RapidAI/RapidOCR/resolve/v3.9.2/onnx/PP-OCRv6"
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$DIR"
fetch() {   # file, url path, sha256
  if [ ! -f "$DIR/$1" ]; then curl -fsSL "$BASE/$2" -o "$DIR/$1"; fi
  echo "$3  $DIR/$1" | sha256sum -c -
}
fetch PP-OCRv6_det_small.onnx det/PP-OCRv6_det_small.onnx 090f04abcd9d9a7498bc4ebf677e4cb9bdce1fe4197ddb7e529f1ef44e1ff94f
fetch PP-OCRv6_rec_small.onnx rec/PP-OCRv6_rec_small.onnx 6f327246b50388f3c176ae304bd95767ea6dc0c9ae92153ef8cbe210b3c14884
cp "$HERE/PP-OCRv6_rec_small.keys.txt" "$DIR/"
echo "PP-OCR models ready in $DIR"
