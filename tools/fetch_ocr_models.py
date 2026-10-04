"""
Figures OCR models for the Java app (service.PpOcr, docs/specs/figures.md).

The app reads figure text with the PP-OCRv6 small models that RapidOCR ships
(Apache-2.0, PaddleOCR). They are 31 MB, so they are not in git: this copies
them from the RapidOCR virtualenv into models/ppocr/ (gitignored) and writes
their SHA-256, so a deployment can check it has the same files.

It also writes the recognition model's character list to a UTF-8 file
(PP-OCRv6_rec_small.keys.txt): ONNX Runtime's Java binding decodes the list
stored in the model as JNI "modified UTF-8", which breaks the 4-byte
characters near its end and loses 540 entries - including the space.

Usage (with the RapidOCR virtualenv's python, which has onnxruntime):
  "%LOCALAPPDATA%/smart_gazette/paddle-venv/Scripts/python.exe" tools/fetch_ocr_models.py [rapidocr models dir]
"""
import hashlib, os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
DEFAULT = os.path.join(os.environ.get('LOCALAPPDATA', ''), 'smart_gazette', 'paddle-venv', 'Lib', 'site-packages', 'rapidocr', 'models')
FILES = ['PP-OCRv6_det_small.onnx', 'PP-OCRv6_rec_small.onnx']


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else DEFAULT
    dst = os.path.join(REPO, 'models', 'ppocr')
    os.makedirs(dst, exist_ok=True)
    lines = []
    for f in FILES:
        shutil.copyfile(os.path.join(src, f), os.path.join(dst, f))
        digest = hashlib.sha256(open(os.path.join(dst, f), 'rb').read()).hexdigest()
        lines.append('%s  %s' % (digest, f))
        print(f, digest)
    import onnxruntime
    rec = onnxruntime.InferenceSession(os.path.join(dst, FILES[1]))
    keys = rec.get_modelmeta().custom_metadata_map['character'].splitlines()
    classes = rec.get_outputs()[0].shape[-1]
    assert len(keys) + 2 == classes, (len(keys), classes)          # + CTC blank + space
    with open(os.path.join(dst, 'PP-OCRv6_rec_small.keys.txt'), 'w', encoding='utf-8', newline='\n') as f:
        f.write('\n'.join(keys))
    print('characters', len(keys), '+ blank + space =', classes, 'classes')
    open(os.path.join(dst, 'SHA256SUMS'), 'w').write('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
