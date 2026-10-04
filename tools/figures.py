"""
Figures (docs/specs/figures.md): every image a gazette prints - captured,
linked to its notice, classified, cleaned (scanned kinds), kept.

Input per gazette: the extractor's text with markers (raw/<y>/<slug>.fig.txt)
and its figure list (.fig.json: page, bbox in points, image size in pixels).

  figures_of(pdf, text, figs, out_dir) -> [record]
      record: id, page, bbox, notice (number or None), in_table, kind,
              ocr_text, file (original crop), clean_file (or None)

Kinds, in order: page_scan, coat_of_arms, table_symbol, form, chart, table_image, map,
prescribed_image, stamp_seal, signature, logo, photo, mark, other.

Usage: python tools/figures.py 2022 2023 2024 2025 2026   (writes figures/ and reports/corpus/figures_index.json)
"""
import collections, json, os, re, subprocess, sys, tempfile

import numpy as np
from PIL import Image, ImageFilter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TMP = tempfile.gettempdir()
DPI = 200
MARK = re.compile(r'\[\[FIGURE:(\d+)\.(\d+)\]\]')
HDR = re.compile(r'GAZETTE NOTICE NO\. (\d+)')

# on the figure itself (OCR)
MAP_WORDS = re.compile(r'\b(?:map|registry\s+index|index\s+series|block|scale|sheet|coordinates?|UTM|Arc\s*1960|survey|plan\s+no|R\.?I\.?M)\b', re.I)
# in the notice text: only explicit words (a long report says 'block' or 'scale' for other reasons)
MAP_NOTICE = re.compile(r'\b(?:maps?|index\s+series|registry\s+index|index\s+map|coordinates|UTM|Arc\s*1960|concession\s+(?:areas?|blocks?|maps?)|coal\s+blocks?|petroleum\s+blocks?)\b', re.I)
CHART_WORDS = re.compile(r'\b(?:figure\s+\d|fig\.\s*\d|chart|graph|trend|performance|revenue|growth|percent|financial\s+year|billion|actual|target|%)', re.I)
PRESCRIBED_WORDS = re.compile(r'\b(?:images?\s+set\s+out|pictorial|health\s+warning|graphic\s+warning|prescribed\s+images?)\b', re.I)
STAMP_WORDS = re.compile(r'\b(?:received|library|council|registrar|certified|seal|stamp|approved|law\s+reporting|commissioner\s+for\s+oaths|date)\b', re.I)
SIGN_CONTEXT = re.compile(r'\b(?:Dated\s+the|Signed|Chairperson|Registrar|Secretary|Cabinet\s+Secretary|Governor)\b', re.I)


# ------------------------------------------------------------- notice link
def notice_of_markers(text):
    """marker id -> (notice number, in_table, line text): the notice is the last
    header above the marker in the cleaned reading order"""
    out = {}
    num = None
    for line in text.split('\n'):
        m = re.match(r'\s*GAZETTE\s+NOTICE\s+NO\.?\s*(\d+)', line, re.I)
        if m:
            num = m.group(1)
        for mm in MARK.finditer(line):
            out['%s.%s' % (mm.group(1), mm.group(2))] = (num, ' | ' in line or line.rstrip().endswith(' |'), line.strip())
    return out


# ------------------------------------------------------------- image work
def crop(pdf, f, path):
    k = DPI / 72.0
    x, y = int(f['x0'] * k), int(f['y0'] * k)
    w, h = max(1, int((f['x1'] - f['x0']) * k)), max(1, int((f['y1'] - f['y0']) * k))
    base = path[:-4]
    subprocess.run(['pdftoppm', '-r', str(DPI), '-png', '-singlefile', '-f', str(f['page']), '-l', str(f['page']),
                    '-x', str(x), '-y', str(y), '-W', str(w), '-H', str(h), pdf, base], capture_output=True)
    return os.path.exists(path)


def ocr(path):
    r = subprocess.run(['tesseract', path, '-', '-l', 'eng', '--psm', '11'], capture_output=True, text=True,
                       env=dict(os.environ, OMP_THREAD_LIMIT='1'), encoding='utf-8', errors='replace')
    return re.sub(r'\s+', ' ', r.stdout).strip()


def confident_words(path):
    """words of >= 4 letters Tesseract reads with confidence >= 80"""
    r = subprocess.run(['tesseract', path, '-', '-l', 'eng', '--psm', '11', 'tsv'], capture_output=True, text=True,
                       env=dict(os.environ, OMP_THREAD_LIMIT='1'), encoding='utf-8', errors='replace')
    n = 0
    for line in r.stdout.splitlines()[1:]:
        c = line.split('\t')
        if len(c) == 12 and c[10] not in ('', '-1') and float(c[10]) >= 80 and re.fullmatch(r'[A-Za-z]{4,}', c[11]):
            n += 1
    return n


def upside_down(path):
    """a map scanned upside down (2025 No 163 p61): Tesseract's own orientation
    detector fails on maps (confidence 0.14), so read the image both ways up and
    turn it only when the turned reading clearly wins"""
    turned = path[:-4] + '_r180.tmp.png'
    Image.open(path).rotate(180).save(turned)
    try:
        up, down = confident_words(path), confident_words(turned)
    finally:
        os.remove(turned)
    return down >= 3 and down > 2 * up


def colourful(img):
    a = np.asarray(img.convert('RGB')).astype(np.int16)
    return float(np.mean(np.max(a, axis=2) - np.min(a, axis=2)))


def paper_tone(img):
    """how far the background is from white (brown / grey paper): 0 = white"""
    g = np.asarray(img.convert('L'))
    return float(255 - np.percentile(g, 90))


def clean(src, dst, rotate=0):
    """local cleanup, no generative step: divide by a blurred copy (removes
    paper tone, stains, uneven light), stretch contrast, sharpen; small scans
    2x. Every output pixel is a monotone function of its neighbourhood."""
    im = Image.open(src).convert('RGB')
    g = np.asarray(im.convert('L')).astype(np.float32)
    bg = np.asarray(im.convert('L').filter(ImageFilter.GaussianBlur(radius=max(8, im.width // 60)))).astype(np.float32)
    flat = np.clip(g / np.maximum(bg, 1) * 255, 0, 255)
    lo, hi = np.percentile(flat, 1), np.percentile(flat, 70)
    out = Image.fromarray(np.clip((flat - lo) / max(1, hi - lo) * 255, 0, 255).astype(np.uint8))
    if out.width < 1600:
        out = out.resize((out.width * 2, out.height * 2), Image.LANCZOS)
    out = out.filter(ImageFilter.UnsharpMask(radius=1.5, percent=120, threshold=2))
    (out.rotate(rotate) if rotate else out).save(dst)    # the original crop is never turned


def ahash(img):
    a = np.asarray(img.convert('L').resize((8, 8), Image.BILINEAR))
    return ''.join('1' if v > a.mean() else '0' for v in a.flatten())


# ------------------------------------------------------------- classify
def classify(f, ctx, ocr_text, img, repeats, page_area, notice_text):
    w_in, h_in = (f['x1'] - f['x0']) / 72.0, (f['y1'] - f['y0']) / 72.0
    area = w_in * h_in
    num, in_table, line = ctx
    words = len(re.findall(r'[A-Za-z]{3,}', ocr_text))
    if (f['x1'] - f['x0']) * (f['y1'] - f['y0']) >= 0.85 * page_area:
        return 'page_scan' if not MAP_WORDS.search(ocr_text + ' ' + (notice_text or '')) else 'map'
    if f['page'] == 1 and f['y0'] < 300 and 1.5 <= w_in <= 3.5 and num is None:
        return 'coat_of_arms'
    if in_table:
        return 'table_symbol'
    # a prescribed form printed as an image (2026 No 75: EPRA Form 3 certificate)
    if re.match(r'\W*FORM\s*\d', ocr_text):
        return 'form'
    # a page-sized map: checked before the chart / table tests, because a map's
    # OCR is full of parcel numbers (2025 No 163, 2022 No 43 block maps)
    if area >= 30 and (MAP_WORDS.search(ocr_text) or (notice_text and MAP_NOTICE.search(notice_text))):
        return 'map'
    # a chart shows its numbers and labels; checked before the notice-level map test
    if area >= 4 and CHART_WORDS.search(ocr_text) and len(re.findall(r'\d', ocr_text)) >= 8:
        return 'chart'
    # a picture of a table (financial statements printed as images, 2023 No 154):
    # its OCR text is the content. It prints comma-grouped amounts ("142,390"),
    # even a one-row strip (p37: 6.6 x 0.6 in). Not a bare digit count: a county
    # map's block numbers 1-24 are 43 digits (2026 No 75 p27, PP-OCR text)
    amounts = len(re.findall(r'\b\d{1,3}(?:,\d{3})+\b', ocr_text))
    if area >= 2 and words >= 2 and amounts >= 2:
        return 'table_image'
    # a report that cites "Figure n" (KRA annual reports): its images are charts,
    # even where its tables happen to say "concession" or "block"
    if area >= 4 and notice_text and re.search(r'\b(?:Figure|Fig\.)\s*\d', notice_text):
        return 'chart'
    # a map: map words on the figure itself, or explicit map / coordinate words
    # in its notice (a location map beside a coordinate table is ~8 sq in)
    if (area >= 12 and MAP_WORDS.search(ocr_text)) or (area >= 4 and notice_text and MAP_NOTICE.search(notice_text)):
        return 'map'
    if notice_text and PRESCRIBED_WORDS.search(notice_text):
        return 'prescribed_image'
    if STAMP_WORDS.search(ocr_text) and area < 12:
        return 'stamp_seal'
    if area < 0.25:
        return 'mark'
    if area < 3 and w_in > 1.6 * h_in and notice_text and SIGN_CONTEXT.search(line or ''):
        return 'signature'
    if area < 4 and repeats >= 2:
        return 'logo'
    if area >= 2 and words < 15:
        return 'photo'
    if area < 4:
        return 'logo'
    return 'other'


SCANNED_KINDS = {'map', 'page_scan', 'stamp_seal', 'photo'}


def figures_of(pdf, text, figs, out_dir, notices=None):
    os.makedirs(out_dir, exist_ok=True)
    ctx = notice_of_markers(text)
    page_area = 595 * 842
    recs, hashes = [], collections.Counter()
    for f in figs:
        path = os.path.join(out_dir, 'f%s.png' % f['id'].replace('.', '_'))
        if not os.path.exists(path):
            crop(pdf, f, path)
        if not os.path.exists(path):
            continue
        img = Image.open(path)
        # the same image placed several times (a logo, a party symbol) has the same
        # pixel size in the PDF - deterministic in Python and Java, unlike a resampled hash
        f['_hash'] = 'x'.join(str(int(v)) for v in (f.get('px') or [0, 0]))
        hashes[f['_hash']] += 1
    for f in figs:
        path = os.path.join(out_dir, 'f%s.png' % f['id'].replace('.', '_'))
        if not os.path.exists(path):
            continue
        img = Image.open(path)
        c = ctx.get(f['id'], (None, False, ''))
        ocr_text = ocr(path) if (f['x1'] - f['x0']) * (f['y1'] - f['y0']) >= 0.25 * 72 * 72 else ''
        notice_text = (notices or {}).get(c[0])
        kind = classify(f, c, ocr_text, img, hashes[f.get('_hash')], page_area, notice_text)
        clean_file, rotate = None, 0
        if kind in ('map', 'page_scan') and upside_down(path):
            rotate = 180
        if kind in SCANNED_KINDS and (paper_tone(img) > 25 or rotate):
            clean_file = path[:-4] + '_clean.png'
            if not os.path.exists(clean_file):
                clean(path, clean_file, rotate)
        recs.append({'id': f['id'], 'page': f['page'], 'bbox': [round(f['x0'], 1), round(f['y0'], 1), round(f['x1'], 1), round(f['y1'], 1)],
                     'px': f.get('px'), 'notice': c[0], 'in_table': c[1], 'kind': kind, 'ocr_text': ocr_text,
                     'colour': round(colourful(img), 1), 'paper_tone': round(paper_tone(img), 1),
                     'repeats': hashes[f.get('_hash')], 'rotate': rotate, 'file': os.path.relpath(path, REPO),
                     'clean_file': os.path.relpath(clean_file, REPO) if clean_file else None})
    return recs


_V = None


def corpus_job(job):
    """one gazette of the corpus run (module level: Windows workers import it)"""
    global _V
    try:
        sys.path.insert(0, HERE)
        import gazette_clean as G, vocab_repair as VR
        if _V is None:
            _V = VR.load_corpus_vocab()
        y, slug, pdf = job
        d = os.path.join(REPO, 'raw', y)
        figs = json.load(open(os.path.join(d, slug + '.fig.json'), encoding='utf-8'))
        if not figs:
            return y, slug, []
        raw = open(os.path.join(d, slug + '.fig.txt'), encoding='utf-8', errors='replace').read()
        text = G.apply_ascending_lock(VR.repair(G.clean(raw), _V))
        notices = {m.group(1): n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text) for m in [HDR.match(n)] if m}
        return y, slug, figures_of(pdf, text, figs, os.path.join(REPO, 'figures', y, slug), notices)
    except Exception as e:                           # one bad gazette must not stop the corpus run
        print('FAILED', job[0], job[1], repr(e)[:200], flush=True)
        return job[0], job[1], []


def reclassify(ocr_from=None):
    """re-run the rules on the OCR text the index already holds (a rule change
    needs no new crops or OCR); a figure that becomes a scanned kind gets its
    orientation check and cleaned copy now.
    ocr_from: a folder of PP-OCR line caches (figure_ocr_eval.py: the Java
    service.PpOcr writes reports/corpus/figure_rapid_java) - their text, in
    reading order, replaces the stored OCR text first, as the app reads figures"""
    sys.path.insert(0, HERE)
    import gazette_clean as G, vocab_repair as VR, figure_ocr_eval as FE
    V = VR.load_corpus_vocab()
    path = os.path.join(REPO, 'reports', 'corpus', 'figures_index.json')
    index = json.load(open(path, encoding='utf-8'))
    changes = collections.Counter()
    by_gazette = collections.defaultdict(list)
    for r in index:
        by_gazette[(r['year'], r['slug'])].append(r)
    for (y, slug), recs in by_gazette.items():
        d = os.path.join(REPO, 'raw', y)
        exact = {f['id']: f for f in json.load(open(os.path.join(d, slug + '.fig.json'), encoding='utf-8'))}
        raw = open(os.path.join(d, slug + '.fig.txt'), encoding='utf-8', errors='replace').read()
        text = G.apply_ascending_lock(VR.repair(G.clean(raw), V))
        notices = {m.group(1): n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text) for m in [HDR.match(n)] if m}
        ctx = notice_of_markers(text)
        for r in recs:
            if ocr_from:
                cached = FE.cache_path(r, ocr_from)
                if os.path.exists(cached):
                    r['ocr_text'] = FE.reading_order(json.load(open(cached, encoding='utf-8'))['lines'])
                    r['ocr_engine'] = 'pp-ocr'
            c = ctx.get(r['id'], (None, False, ''))
            kind = classify(exact[r['id']], c, r['ocr_text'], None, r['repeats'], 595 * 842, notices.get(c[0]))
            if kind == r['kind']:
                continue
            changes['%s -> %s' % (r['kind'], kind)] += 1
            r['kind'] = kind
            src = os.path.join(REPO, r['file'])
            if kind in ('map', 'page_scan') and not r.get('rotate') and upside_down(src):
                r['rotate'] = 180
            if kind in SCANNED_KINDS and (r['paper_tone'] > 25 or r.get('rotate')) and not r['clean_file']:
                dst = src[:-4] + '_clean.png'
                clean(src, dst, r.get('rotate') or 0)
                r['clean_file'] = os.path.relpath(dst, REPO)
    json.dump(index, open(path, 'w', encoding='utf-8'), indent=0)
    print('changed', sum(changes.values()), dict(changes))
    print('figures', len(index), dict(collections.Counter(r['kind'] for r in index).most_common()))


if __name__ == '__main__' and '--reclassify' in sys.argv:
    # python tools/figures.py --reclassify [--ocr-from reports/corpus/figure_rapid_java]
    reclassify(sys.argv[sys.argv.index('--ocr-from') + 1] if '--ocr-from' in sys.argv else None)
elif __name__ == '__main__':
    import concurrent.futures as cf
    sys.path.insert(0, HERE)
    import ruled_eval as RE
    jobs = []
    for y in sys.argv[1:]:
        ps = RE.pdfs(y)
        for f in sorted(os.listdir(os.path.join(REPO, 'raw', y))):
            if f.endswith('.fig.json'):
                s = f.split('.')[0]
                if s in ps:
                    jobs.append((y, s, ps[s]))
    index = []
    with cf.ProcessPoolExecutor(4) as ex:
        for y, slug, recs in ex.map(corpus_job, jobs):
            for r in recs:
                r['year'], r['slug'] = y, slug
            index += recs
    json.dump(index, open(os.path.join(REPO, 'reports', 'corpus', 'figures_index.json'), 'w', encoding='utf-8'), indent=0)
    kinds = collections.Counter(r['kind'] for r in index)
    print('figures', len(index), dict(kinds.most_common()))
    print('linked to a notice', sum(1 for r in index if r['notice']), '| cleaned copies', sum(1 for r in index if r['clean_file']))
