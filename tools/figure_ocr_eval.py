"""
Figures: RapidOCR vs Tesseract on the figure crops (docs/specs/figures.md).

The figures that carry text - maps, charts, table images, forms - are read
by Tesseract today (figures.ocr, --psm 11). RapidOCR (PP-OCR on ONNX) read
table numbers far better on scans (lesson 38) but is ~3x slower; a gazette
has only a few figures, so speed matters little here.

There is no answer key for a map, so three referees:
  - real words: tokens of >= 3 letters found in the corpus vocabulary
    (pdftotext layer of 2022-2026) - a misread word is not in it
  - well-formed numbers: digit groups with commas in the right places
    ("25,911,449"), against misgrouped ones ("25,91,1449")
  - map titles: "BLOCK <n>" read on the map whose number the notice also
    prints ("Nairobi Block 95") - the notice text is an independent witness
plus a side-by-side sample for reading.

Usage:
  1. "%LOCALAPPDATA%/smart_gazette/paddle-venv/Scripts/python.exe" tools/figure_ocr_eval.py --read
     (RapidOCR on every text-bearing figure -> reports/corpus/figure_rapid/<year>_<slug>_<id>.json)
  2. python tools/figure_ocr_eval.py        (scores -> reports/corpus/figure_ocr_eval.txt)
"""
import collections, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
INDEX = os.path.join(REPO, 'reports', 'corpus', 'figures_index.json')
CACHE = os.path.join(REPO, 'reports', 'corpus', 'figure_rapid')
CACHE_JAVA = os.path.join(REPO, 'reports', 'corpus', 'figure_rapid_java')   # Java library (RapidJava harness)
KINDS = {'map', 'chart', 'table_image', 'form', 'prescribed_image', 'photo', 'page_scan', 'stamp_seal', 'logo', 'other'}
_eng = None


def cache_path(r, cache=CACHE):
    return os.path.join(cache, '%s_%s_%s.json' % (r['year'], r['slug'], r['id'].replace('.', '_')))


def reading_order(lines):
    """RapidOCR boxes [x0, y0, x1, y1, text, score] -> text: lines whose vertical
    centres are within half the median line height form one row, read left to right"""
    if not lines:
        return ''
    hs = sorted(l[3] - l[1] for l in lines)
    half = max(1, hs[len(hs) // 2]) / 2.0
    rows, cur, yc = [], [], None
    for l in sorted(lines, key=lambda l: (l[1] + l[3]) / 2):
        c = (l[1] + l[3]) / 2
        if cur and c - yc > half:
            rows.append(cur); cur = []
        if not cur:
            yc = c
        cur.append(l)
    rows.append(cur)
    return '\n'.join(' '.join(l[4] for l in sorted(r, key=lambda l: l[0])) for r in rows)


def rapid_read(job):
    """RapidOCR lines of one crop, top to bottom then left to right"""
    global _eng
    r, threads = job
    out = cache_path(r)
    if os.path.exists(out):
        return
    if _eng is None:
        from rapidocr import RapidOCR
        _eng = RapidOCR(params={'Global.log_level': 'error',
                                'EngineConfig.onnxruntime.intra_op_num_threads': threads})
    import time
    t = time.time()
    res = _eng(os.path.join(REPO, r['file']))
    lines = []
    if res.txts:
        for txt, score, box in zip(res.txts, res.scores, res.boxes):
            xs, ys = [p[0] for p in box], [p[1] for p in box]
            lines.append([round(min(xs)), round(min(ys)), round(max(xs)), round(max(ys)), txt, round(float(score), 3)])
    lines.sort(key=lambda l: (l[1] // 20, l[0]))
    json.dump({'lines': lines, 'seconds': round(time.time() - t, 2)}, open(out, 'w', encoding='utf-8'))


# ------------------------------------------------------------- referees
WORD = re.compile(r'[A-Za-z]{3,}')
NUM = re.compile(r'(?<![\d,])\d[\d,]*\d(?:\.\d+)?(?![\d,])')
GOOD_NUM = re.compile(r'\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?')
BLOCK = re.compile(r'\bBLOCK\s*(\d{1,4})\b', re.I)


def words(text, V):
    ws = [w.lower() for w in WORD.findall(text)]
    return len(ws), sum(1 for w in ws if w in V)


def numbers(text):
    ns = [n for n in NUM.findall(text) if ',' in n]
    return len(ns), sum(1 for n in ns if GOOD_NUM.fullmatch(n))


def block_titles(text, notice_blocks):
    got = {b.lstrip('0') for b in BLOCK.findall(text)}
    return len(got & notice_blocks)


def main():
    sys.path.insert(0, HERE)
    import vocab_repair as VR, gazette_clean as G, ruled_downstream as D
    V = VR.load_corpus_vocab()
    ix = [r for r in json.load(open(INDEX, encoding='utf-8')) if r['kind'] in KINDS]
    notice_blocks = {}
    tot = collections.defaultdict(collections.Counter)
    secs = collections.Counter()
    sample = []
    for r in ix:
        p = cache_path(r)
        if not os.path.exists(p):
            continue
        rap = json.load(open(p, encoding='utf-8'))
        rtext = reading_order(rap['lines'])
        ttext = r['ocr_text']
        pj = cache_path(r, CACHE_JAVA)
        jap = json.load(open(pj, encoding='utf-8')) if os.path.exists(pj) else None
        jtext = reading_order(jap['lines']) if jap else None
        key = (r['year'], r['slug'], r['notice'])
        if r['kind'] == 'map' and key not in notice_blocks:
            nb = set()
            if r['notice']:
                raw = open(os.path.join(REPO, 'raw', r['year'], r['slug'] + '.fig.txt'), encoding='utf-8', errors='replace').read()
                n = D.notices(G.apply_ascending_lock(VR.repair(G.clean(raw), V))).get(int(r['notice']), '')
                nb = {b.lstrip('0') for b in BLOCK.findall(re.sub(r'\[\[FIGURE:[\d.]+\]\]', ' ', n))}
            notice_blocks[key] = nb
        k = r['kind']
        engines = [('tesseract', ttext), ('rapidocr', rtext)] + ([('rapid-java', jtext)] if jap else [])
        for eng, text in engines:
            a, b = words(text, V)
            c, d = numbers(text)
            s = tot[(k, eng)]
            s['figures'] += 1
            s['words'] += a
            s['real words'] += b
            s['comma numbers'] += c
            s['well-formed'] += d
            if k == 'map':
                s['map titles matching the notice'] += block_titles(text, notice_blocks[key])
                s['maps with a matching title'] += block_titles(text, notice_blocks[key]) > 0
        secs[(k, 'rapidocr')] += rap['seconds']
        if jap:
            secs[(k, 'rapid-java')] += jap['seconds']
        sample.append((r, ttext, rtext, jtext))
    out = []
    for k in sorted({k for k, _ in tot}):
        out.append('== %s  (seconds in all: RapidOCR Python %.0f, Java %.0f)' % (k, secs[(k, 'rapidocr')], secs[(k, 'rapid-java')]))
        for eng in ('tesseract', 'rapidocr', 'rapid-java'):
            s = tot[(k, eng)]
            if not s:
                continue
            out.append('  %-9s figures %4d | real words %6d of %6d (%5.1f%%) | well-formed numbers %5d of %5d (%5.1f%%)%s' % (
                eng, s['figures'], s['real words'], s['words'], 100.0 * s['real words'] / max(1, s['words']),
                s['well-formed'], s['comma numbers'], 100.0 * s['well-formed'] / max(1, s['comma numbers']),
                (' | maps with a title matching the notice %d' % s['maps with a matching title']) if k == 'map' else ''))
    out.append('\n== side by side (first 160 characters)')
    seen = collections.Counter()
    for r, t, ra, ja in sample:
        if seen[r['kind']] >= 4:
            continue
        seen[r['kind']] += 1
        out.append('-- %s %s %s %s notice %s' % (r['kind'], r['year'], r['slug'], r['id'], r['notice']))
        out.append('   tesseract: ' + re.sub(r'\s+', ' ', t)[:160])
        out.append('   rapidocr : ' + re.sub(r'\s+', ' ', ra)[:160])
        if ja is not None:
            out.append('   rapid-jv : ' + re.sub(r'\s+', ' ', ja)[:160])
    report = '\n'.join(out)
    open(os.path.join(REPO, 'reports', 'corpus', 'figure_ocr_eval.txt'), 'w', encoding='utf-8').write(report + '\n')
    print(report)


if __name__ == '__main__':
    if '--read' in sys.argv:
        from multiprocessing import Pool
        os.makedirs(CACHE, exist_ok=True)
        jobs = [(r, 2) for r in json.load(open(INDEX, encoding='utf-8'))
                if r['kind'] in KINDS and (r['bbox'][2] - r['bbox'][0]) * (r['bbox'][3] - r['bbox'][1]) >= 0.25 * 72 * 72]
        print(len(jobs), 'figures to read', flush=True)
        with Pool(3) as pool:
            for i, _ in enumerate(pool.imap_unordered(rapid_read, jobs)):
                if i % 25 == 0:
                    print(i, flush=True)
    else:
        main()
