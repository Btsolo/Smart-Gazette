"""
Measurement (scan tables): how many scanned pages carry tables, are they
ruled, and can the ruling lines be found in the page image?

Per scanned page (Tesseract word boxes, reports/ocr/<year>/<slug>/pNNNN.tsv.gz,
300 dpi):
  - table lines: OCR lines whose words fall into >= 3 groups separated by a
    gap wider than 2x the line height (a prose line has one group per page
    column); a table page has >= 5 such lines
  - ruling lines (table pages and a sample of prose pages as control): the
    page rendered at 100 dpi in grey; a horizontal rule is a pixel row with a
    dark run >= 15% of the page width, a vertical rule a pixel column with a
    dark run >= 8% of the page height; a lattice = >= 3 vertical and >= 3
    horizontal rules

Usage: python tools/scan_table_probe.py 2022 2023 2024 2025
"""
import collections, csv, gzip, os, random, subprocess, sys, tempfile

import numpy as np
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')
TMP = tempfile.gettempdir()


def words(tsv):
    out = []
    with gzip.open(tsv, 'rt', encoding='utf-8', errors='replace') as f:
        next(f)
        for ln in f:
            c = ln.rstrip('\n').split('\t')
            if len(c) < 12 or c[0] != '5' or not c[11].strip():
                continue
            out.append({'line': (c[2], c[3], c[4]), 'x': int(c[6]), 'y': int(c[7]), 'w': int(c[8]), 'h': int(c[9]),
                        'conf': float(c[10]), 'text': c[11]})
    return out


def table_lines(ws):
    lines = collections.defaultdict(list)
    for w in ws:
        lines[w['line']].append(w)
    n = 0
    for ln in lines.values():
        ln.sort(key=lambda w: w['x'])
        h = sorted(w['h'] for w in ln)[len(ln) // 2]
        groups = 1 + sum(1 for a, b in zip(ln, ln[1:]) if b['x'] - (a['x'] + a['w']) > 2 * h)
        n += groups >= 3
    return n, len(lines)


def runs(a):
    """longest run of True in each row of a 2-D bool array"""
    best = np.zeros(a.shape[0], dtype=int)
    cur = np.zeros(a.shape[0], dtype=int)
    for j in range(a.shape[1]):
        cur = np.where(a[:, j], cur + 1, 0)
        best = np.maximum(best, cur)
    return best


def rules(pdf, page):
    base = os.path.join(TMP, 'stp_%d_%d' % (os.getpid(), page))
    subprocess.run(['pdftoppm', '-r', '100', '-gray', '-png', '-singlefile', '-f', str(page), '-l', str(page), pdf, base],
                   capture_output=True)
    try:
        img = np.asarray(Image.open(base + '.png').convert('L'))
    finally:
        if os.path.exists(base + '.png'):
            os.remove(base + '.png')
    dark = img < 110
    H, W = dark.shape
    hr = runs(dark) >= 0.15 * W
    vr = runs(dark.T) >= 0.08 * H
    # count distinct lines (adjacent pixel rows are one line)
    count = lambda m: int(np.sum(m[1:] & ~m[:-1]) + (1 if m[0] else 0))
    return count(hr), count(vr)


def main(years):
    random.seed(4)
    stats = collections.defaultdict(collections.Counter)
    for y in years:
        rows = list(csv.DictReader(open(os.path.join(REPO, 'reports', 'ocr', y, 'gazettes.csv'), encoding='utf-8')))
        pdfs = {}
        for f in os.listdir(os.path.join(CORPUS, y)):
            import re
            m = re.search(r'No\s*(\d+)(\s*\(1\))?\.pdf$', f)
            if m:
                pdfs.setdefault('no%03d' % int(m.group(1)) + ('_b' if m.group(2) else ''), os.path.join(CORPUS, y, f))
        for r in rows:
            if r['kind'] in ('born-digital', 'mixed'):
                continue
            d = os.path.join(REPO, 'reports', 'ocr', y, r['slug'])
            pdf = pdfs.get(r['slug'])
            for p in range(1, int(r['pages']) + 1):
                tsv = os.path.join(d, 'p%04d.tsv.gz' % p)
                if not os.path.exists(tsv):
                    continue
                tl, nl = table_lines(words(tsv))
                s = stats[y]
                s['pages'] += 1
                kind = 'table' if tl >= 5 else 'prose'
                s[kind + ' pages'] += 1
                s['table lines'] += tl
                if pdf and (kind == 'table' or random.random() < 0.08):
                    h, v = rules(pdf, p)
                    lat = h >= 3 and v >= 3
                    s[kind + ' pages checked for rules'] += 1
                    s[kind + ' pages with a lattice'] += lat
                    if kind == 'table':
                        print('\t'.join(map(str, [y, r['slug'], p, tl, nl, h, v, int(lat)])))
                        sys.stdout.flush()
    for y in years:
        print('==', y, dict(stats[y]))
    print('== all', dict(sum(stats.values(), collections.Counter())))


if __name__ == '__main__':
    main(sys.argv[1:])
