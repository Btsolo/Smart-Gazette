"""
RapidOCR (PP-OCR models on ONNX Runtime) for the scan lane - fix 5.

Renders each page of a scanned gazette at 300 dpi, reads it with RapidOCR and
writes, next to the Tesseract cache:
  reports/ocr/<year>/<slug>/r0001.txt       page text in reading order
  reports/ocr/<year>/<slug>/r0001.json.gz   line boxes, text, confidence
RapidOCR returns text LINES with boxes but no reading order, so the page is
laid out here: full-width lines (headings, tables) split the page into bands;
inside a band the left column is read before the right one (the Gazette's
two-column layout - same idea as inspect_positions.js for born-digital).

Run with the RapidOCR virtualenv:
  "%LOCALAPPDATA%/smart_gazette/paddle-venv/Scripts/python.exe" tools/rapid_ocr_pages.py --workers 3 2022 2023 2024 2025
Pages already done are skipped, so the run can be stopped and resumed.
"""
import argparse, csv, gzip, json, os, subprocess, sys, tempfile, time
from multiprocessing import Pool

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')
TMP = os.path.join(tempfile.gettempdir(), 'smart_gazette_rapid')
DPI = 300
_eng = None


def engine(threads):
    global _eng
    if _eng is None:
        from rapidocr import RapidOCR
        _eng = RapidOCR(params={'Global.log_level': 'error',
                                'EngineConfig.onnxruntime.intra_op_num_threads': threads})
    return _eng


def layout(items, width):
    """items: (x0, y0, x1, y1, text) -> page text in reading order."""
    if not items:
        return ''
    hs = sorted(i[3] - i[1] for i in items)
    lh = hs[len(hs) // 2] or 30                     # median line height
    mid = width / 2.0
    full = lambda i: (i[2] - i[0]) > 0.55 * width or (i[0] < mid - 0.05 * width and i[2] > mid + 0.05 * width)
    items = sorted(items, key=lambda i: i[1])
    out, band = [], []

    def rows(lines):
        """group boxes whose vertical centres are within half a line height"""
        res, cur, yc = [], [], None
        for i in sorted(lines, key=lambda i: (i[1] + i[3]) / 2):
            c = (i[1] + i[3]) / 2
            if cur and c - yc > 0.5 * lh:
                res.append(cur); cur = []
            if not cur:
                yc = c
            cur.append(i)
        if cur:
            res.append(cur)
        return [' '.join(t[4] for t in sorted(r, key=lambda t: t[0])) for r in res]

    def flush():
        left = [i for i in band if (i[0] + i[2]) / 2 < mid]
        right = [i for i in band if (i[0] + i[2]) / 2 >= mid]
        out.extend(rows(left)); out.extend(rows(right))
        band.clear()

    for i in items:
        if full(i):
            flush()
            out.append(i[4])
        else:
            band.append(i)
    flush()
    return '\n'.join(out)


def do_page(job):
    pdf, page, outdir, threads = job
    base = os.path.join(outdir, 'r%04d' % page)
    if os.path.exists(base + '.txt'):
        return page, 0.0, True
    t0 = time.time()
    os.makedirs(TMP, exist_ok=True)
    img = os.path.join(TMP, '%s_%d_%04d' % (os.path.basename(outdir), os.getpid(), page))
    subprocess.run(['pdftoppm', '-r', str(DPI), '-gray', '-png', '-singlefile', '-f', str(page), '-l', str(page), pdf, img],
                   check=True, capture_output=True)
    try:
        from PIL import Image
        w = Image.open(img + '.png').size[0]
        r = engine(threads)(img + '.png')
    finally:
        if os.path.exists(img + '.png'):
            os.remove(img + '.png')
    items, recs = [], []
    if r.txts:
        for t, s, b in zip(r.txts, r.scores, r.boxes):
            xs = [p[0] for p in b]; ys = [p[1] for p in b]
            it = (float(min(xs)), float(min(ys)), float(max(xs)), float(max(ys)), t)
            items.append(it); recs.append({'box': it[:4], 'text': t, 'conf': round(float(s), 4)})
    open(base + '.txt', 'w', encoding='utf-8').write(layout(items, w))
    with gzip.open(base + '.json.gz', 'wt', encoding='utf-8') as f:
        json.dump({'width': w, 'lines': recs}, f)
    return page, time.time() - t0, False


def jobs(years, threads):
    for y in years:
        for r in csv.DictReader(open(os.path.join(REPO, 'reports', 'ocr', y, 'gazettes.csv'), encoding='utf-8')):
            if r['kind'] == 'born-digital':
                continue
            pdf = os.path.join(CORPUS, y, r['file'])
            outdir = os.path.join(REPO, 'reports', 'ocr', y, r['slug'])
            os.makedirs(outdir, exist_ok=True)
            for p in range(1, int(r['pages']) + 1):
                yield (pdf, p, outdir, threads)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('years', nargs='+')
    ap.add_argument('--workers', type=int, default=3)
    ap.add_argument('--threads', type=int, default=2, help='ONNX threads per worker')
    ap.add_argument('--limit', type=int, default=0, help='only the first N pages (timing test)')
    ap.add_argument('--only', default='', help='comma-separated slugs, e.g. no006,no231')
    ap.add_argument('--pages-file', default='', help='lines "year/slug/page": re-read only these pages (scan_lane gap pages)')
    a = ap.parse_args()
    js = list(jobs(a.years, a.threads))
    if a.only:
        keep = set(a.only.split(','))
        js = [j for j in js if os.path.basename(j[2]) in keep]
    if a.pages_file:
        want = set(l.strip() for l in open(a.pages_file) if l.strip())
        js = [j for j in js if '%s/%s/%d' % (os.path.basename(os.path.dirname(j[2])), os.path.basename(j[2]), j[1]) in want]
    if a.limit:
        js = js[:a.limit]
    t0, done, secs = time.time(), 0, []
    with Pool(a.workers) as pool:
        for page, s, skipped in pool.imap_unordered(do_page, js):
            done += 1
            if not skipped:
                secs.append(s)
            if done % 50 == 0 or done == len(js):
                el = time.time() - t0
                print('%d/%d pages | %.0f s elapsed | %.1f s/page per worker | %.2f pages/s overall'
                      % (done, len(js), el, sum(secs) / max(1, len(secs)), len(secs) / max(1e-9, el)), flush=True)


if __name__ == '__main__':
    main()
