"""
Second Tesseract pass for scan-lane gap pages (fix 5 step 2) - one engine only.

Tesseract's automatic page layout (psm 3) sometimes merges the Gazette's two
columns or drops a short header line, which is why notice numbers go missing
(a "gap"). RapidOCR re-reading those pages recovered most of them, but it is
a second engine, ~10x slower and another runtime to deploy. This pass tries
the same job with Tesseract: the page is cut into its two columns and each
column is read on its own as a single block of text (psm 4), left then right.

Writes reports/ocr/<year>/<slug>/s0001.txt for the pages listed in
cleaned/gap_pages.txt ("year/slug/page" per line, written by scan_lane).

Usage: python tools/tess_second_pass.py [--workers 6] [--psm 4] [--margin 0.02]
"""
import argparse, csv, os, subprocess, tempfile, time
from multiprocessing import Pool

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')
TMP = os.path.join(tempfile.gettempdir(), 'smart_gazette_tess2')
ENV = dict(os.environ, OMP_THREAD_LIMIT='1')


def pdf_of(year, slug):
    for r in csv.DictReader(open(os.path.join(REPO, 'reports', 'ocr', year, 'gazettes.csv'), encoding='utf-8')):
        if r['slug'] == slug:
            return os.path.join(CORPUS, year, r['file'])


def columns(png, margin):
    """Left and right column images, split at the widest blank vertical band
    near the middle (the gutter); falls back to the page middle."""
    from PIL import Image
    im = Image.open(png).convert('L')
    w, h = im.size
    lo, hi = int(w * 0.40), int(w * 0.60)
    px = im.load()
    step = 4
    dark = []
    for x in range(lo, hi):
        dark.append(sum(1 for y in range(int(h * 0.08), int(h * 0.95), step) if px[x, y] < 128))
    best = min(range(len(dark)), key=lambda i: (dark[i], abs(i - len(dark) // 2)))
    cut = lo + best
    pad = int(w * margin)
    left = im.crop((0, 0, min(w, cut + pad), h))
    right = im.crop((max(0, cut - pad), 0, w, h))
    return left, right


def do(job):
    year, slug, page, psm, margin = job
    out = os.path.join(REPO, 'reports', 'ocr', year, slug, 's%04d.txt' % page)
    if os.path.exists(out):
        return 0.0
    t0 = time.time()
    os.makedirs(TMP, exist_ok=True)
    base = os.path.join(TMP, '%s_%s_%d_%04d' % (year, slug, os.getpid(), page))
    subprocess.run(['pdftoppm', '-r', '300', '-gray', '-png', '-singlefile', '-f', str(page), '-l', str(page),
                    pdf_of(year, slug), base], check=True, capture_output=True)
    texts = []
    try:
        for i, col in enumerate(columns(base + '.png', margin)):
            cp = '%s_c%d.png' % (base, i)
            col.save(cp)
            r = subprocess.run(['tesseract', cp, 'stdout', '-l', 'eng', '--psm', str(psm)],
                               capture_output=True, env=ENV)
            texts.append(r.stdout.decode('utf-8', errors='replace'))
            os.remove(cp)
    finally:
        if os.path.exists(base + '.png'):
            os.remove(base + '.png')
    open(out, 'w', encoding='utf-8').write('\n'.join(texts))
    return time.time() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=6)
    ap.add_argument('--psm', type=int, default=4)
    ap.add_argument('--margin', type=float, default=0.02)
    ap.add_argument('--pages-file', default=os.path.join(REPO, 'cleaned', 'gap_pages.txt'))
    a = ap.parse_args()
    jobs = []
    for line in open(a.pages_file):
        if line.strip():
            y, slug, p = line.strip().split('/')
            jobs.append((y, slug, int(p), a.psm, a.margin))
    t0 = time.time()
    with Pool(a.workers) as pool:
        secs = [s for s in pool.imap_unordered(do, jobs) if s]
    print('%d pages in %.0f s (%.1f s/page per worker)' % (len(jobs), time.time() - t0, sum(secs) / max(1, len(secs))))


if __name__ == '__main__':
    main()
