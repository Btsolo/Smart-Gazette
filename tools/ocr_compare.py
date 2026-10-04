"""
OCR engine comparison - measurement only. Tesseract vs PaddleOCR vs RapidOCR.

Run with the PaddleOCR virtualenv (it has paddleocr, rapidocr, onnxruntime,
psutil, Pillow):
  "%LOCALAPPDATA%/smart_gazette/paddle-venv/Scripts/python.exe" tools/ocr_compare.py

Page sets
  A  born-digital pages rendered at 300 dpi           reference = text layer
  B  the same pages degraded like a scan (150 dpi,
     JPEG q40, noise, 0.6 deg tilt)                   reference = text layer
  C  real scans (Foxit, OmniPage, PaperStream ...)    reference = OmniPage layer
                                                      where usable, else engine
                                                      agreement + headers found
Metrics: word F1 and number recall vs the reference, table rows complete
(pdftotext -layout rows, same rule as extractor_eval.py), GAZETTE NOTICE
headers found, seconds per page, peak memory, CPU use.
Writes reports/eval/ocr_compare.json and .txt
"""
import csv, gc, glob, json, os, random, re, statistics, subprocess, sys, tempfile, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import extractor_eval as E      # words(), nums(), f1(), recall(), layout_rows(), row_found(), index(), TOK
import psutil
from PIL import Image, ImageFilter
import numpy as np

CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')
WORK = os.path.join(tempfile.gettempdir(), 'smart_gazette_ocrcmp')
os.makedirs(WORK, exist_ok=True)
HDR = re.compile(r'G\s?A\s?Z\s?E\s?T\s?T?\s?E\s+N\s?[O0]\s?T\s?[I1l]\s?C\s?E\s+N\s?[O0o]\s*[.,:;]?\s*([0-9OoIl|SB]{2,6})', re.I)


def read(p):
    b = open(p, 'rb').read()
    return b.decode('utf-8', errors='replace').replace('\r\n', '\n')


def pdf_of(y, slug):
    summ = json.load(open(os.path.join(REPO, 'reports', 'ocr', y, slug, 'summary.json'), encoding='utf-8'))['summary']
    return os.path.join(CORPUS, y, summ['file'])


def render(pdf, page, dpi, out):
    if not os.path.exists(out + '.png'):
        subprocess.run(['pdftoppm', '-r', str(dpi), '-gray', '-png', '-singlefile', '-f', str(page), '-l', str(page), pdf, out],
                       capture_output=True)
    return out + '.png'


def degrade(src, out):
    if os.path.exists(out):
        return out
    rnd = random.Random(7)
    im = Image.open(src).convert('L').rotate(0.6, expand=False, fillcolor=255)
    a = np.asarray(im).astype(np.float32)
    a += np.random.default_rng(7).normal(0, 12, a.shape)
    im = Image.fromarray(np.clip(a, 0, 255).astype(np.uint8)).filter(ImageFilter.GaussianBlur(0.6))
    im.save(out, 'JPEG', quality=40)
    return out


# --- page selection ------------------------------------------------------------
def pick_pages():
    rnd = random.Random(42)
    A = []
    # sized so ~40 pages per engine finish in a few hours on a laptop CPU
    want = {'probate cause list': 1, 'land schedule': 2, 'financial statement': 1, 'exchequer': 2, 'electoral': 1,
            'tariff schedule': 1}
    for y in ('2022', '2023', '2024', '2026'):
        for r in csv.DictReader(open(os.path.join(REPO, 'reports', 'corpus', y, 'tables.csv'), encoding='utf-8')):
            k = r['kind']
            if want.get(k, 0) > 0 and int(r['rows']) >= 12 and os.path.exists(os.path.join(REPO, 'raw', y, r['slug'] + '.layout.txt')):
                s = json.load(open(os.path.join(REPO, 'reports', 'ocr', y, r['slug'], 'summary.json'), encoding='utf-8'))['summary']
                if s['kind'] == 'born-digital':
                    A.append(('table', k, y, r['slug'], int(r['page']))); want[k] -= 1
    text = []
    for y in ('2023', '2024', '2026'):
        for sj in sorted(glob.glob(os.path.join(REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            if s['kind'] == 'born-digital' and s['pages'] >= 8:
                text.append((y, s['slug'], s['pages']))
    for y, slug, n in rnd.sample(text, 8):
        A.append(('text', 'prose', y, slug, rnd.randint(2, n - 1)))
    C = [('scan', 'Foxit', '2022', 'no137', 5), ('scan', 'Foxit results table', '2022', 'no169', 4),
         ('scan', 'OmniPage', '2022', 'no073', 10), ('scan', 'OmniPage', '2024', 'no077', 20),
         ('scan', 'Law Reporting', '2023', 'no181', 20), ('scan', 'PaperStream 150dpi', '2024', 'no095', 8),
         ('scan', 'PaperStream (lesson 15)', '2025', 'no186', 2), ('scan', 'conversion list (mixed)', '2022', 'no043', 20)]
    return A, C


# --- engines -------------------------------------------------------------------
class Tess:
    name = 'tesseract'

    def run(self, img):
        base = os.path.join(WORK, 'tess_out')
        subprocess.run(['tesseract', img, base, '-l', 'eng', 'tsv'], capture_output=True)
        lines = {}
        conf = []
        for r in csv.DictReader(open(base + '.tsv', encoding='utf-8', errors='replace'), delimiter='\t', quoting=csv.QUOTE_NONE):
            if r.get('level') == '5' and (r.get('text') or '').strip():
                k = (r['block_num'], r['par_num'], r['line_num'])
                lines.setdefault(k, []).append((int(r['left']), int(r['top']), r['text']))
                try: conf.append(float(r['conf']))
                except ValueError: pass
        out = []
        for ws in lines.values():
            ws.sort()
            out.append((ws[0][1], ws[0][0], ' '.join(w[2] for w in ws)))
        return out, (statistics.mean(conf) / 100 if conf else None)

    def child_pids(self):
        return True     # tesseract runs as a child process


class Paddle:
    name = 'paddleocr'

    def __init__(self):
        from paddleocr import PaddleOCR
        # enable_mkldnn=False: paddlepaddle 3.3.1's oneDNN path crashes on this
        # CPU (ConvertPirAttribute2RuntimeAttribute). Mobile models: the
        # default server detector took >10 min/page on CPU without oneDNN.
        self.ocr = PaddleOCR(lang='en', use_doc_orientation_classify=False, use_doc_unwarping=False,
                             use_textline_orientation=False, enable_mkldnn=False,
                             text_detection_model_name='PP-OCRv5_mobile_det',
                             text_recognition_model_name='en_PP-OCRv5_mobile_rec')

    def run(self, img):
        res = self.ocr.predict(img)
        out, conf = [], []
        for r in res:
            d = r.json['res'] if hasattr(r, 'json') else r
            for t, s, b in zip(d['rec_texts'], d['rec_scores'], d.get('rec_boxes') or d.get('rec_polys')):
                b = np.asarray(b).reshape(-1)
                x, y = (float(b[0]), float(b[1])) if len(b) == 4 else (float(min(b[0::2])), float(min(b[1::2])))
                out.append((y, x, t)); conf.append(float(s))
        return out, (statistics.mean(conf) if conf else None)

    def child_pids(self):
        return False


class Rapid:
    name = 'rapidocr'

    def __init__(self):
        from rapidocr import RapidOCR
        self.eng = RapidOCR()

    def run(self, img):
        r = self.eng(img)
        out, conf = [], []
        if r.txts:
            for t, s, b in zip(r.txts, r.scores, r.boxes):
                b = np.asarray(b)
                out.append((float(b[:, 1].min()), float(b[:, 0].min()), t)); conf.append(float(s))
        return out, (statistics.mean(conf) if conf else None)

    def child_pids(self):
        return False


def to_lines(items):
    """Group (y, x, text) boxes into printed lines: same y within 0.6 line height."""
    items = sorted(items)
    if not items:
        return []
    hs = sorted(abs(b[0] - a[0]) for a, b in zip(items, items[1:]) if abs(b[0] - a[0]) > 3)
    lh = hs[len(hs) // 4] if hs else 30
    lines, cur, y0 = [], [], None
    for y, x, t in items:
        if y0 is not None and y - y0 > 0.6 * lh:
            lines.append(cur); cur = []
        if not cur: y0 = y
        cur.append((x, t))
    lines.append(cur)
    return [' '.join(t for x, t in sorted(l)) for l in lines]


def measure(engine, img):
    """Run once, sampling RSS and CPU of this process (and children for tesseract)."""
    proc = psutil.Process()
    peak = [proc.memory_info().rss]
    cpu = []
    stop = threading.Event()

    def sample():
        proc.cpu_percent(None)
        while not stop.is_set():
            rss = proc.memory_info().rss
            c = proc.cpu_percent(None)
            for ch in proc.children(recursive=True):
                try:
                    rss += ch.memory_info().rss; c += ch.cpu_percent(None)
                except psutil.Error:
                    pass
            peak[0] = max(peak[0], rss); cpu.append(c)
            time.sleep(0.1)
    th = threading.Thread(target=sample); th.start()
    t0 = time.time()
    items, conf = engine.run(img)
    dt = time.time() - t0
    stop.set(); th.join()
    return items, conf, dt, peak[0] / 2 ** 20, (statistics.mean(cpu) if cpu else 0)


def score(text, ref_text, rows):
    w = E.f1(E.words(ref_text), E.words(text))
    n = E.recall(E.nums(ref_text), E.nums(text))
    rc = None
    if rows:
        toks = E.TOK.findall(text.lower()); ix = E.index(toks)
        rc = sum(1 for r in rows if E.row_found(r, toks, ix) >= 0.9) / len(rows)
    return w, n, rc


def main():
    A, C = pick_pages()
    engines = [Tess()]
    # Native PaddleOCR is opt-in: on this Windows CPU paddlepaddle 3.3.1 crashes
    # with oneDNN and needs >15 min/page without it. RapidOCR runs the same
    # PP-OCR models through ONNX Runtime.
    for cls in ((Paddle, Rapid) if os.environ.get('OCRCMP_PADDLE') == '1' else (Rapid,)):
        try:
            t0 = time.time(); e = cls(); e.load_s = time.time() - t0; engines.append(e)
        except Exception as ex:
            print('could not load', cls.__name__, ex)
    res = []
    for setname, pages in (('A', A), ('B', A), ('C', C)):
        for kind, sub, y, slug, p in pages:
            pdf = pdf_of(y, slug)
            img = render(pdf, p, 300 if setname != 'B' else 150, os.path.join(WORK, '%s_%s_%d_%s' % (y, slug, p, setname)))
            if setname == 'B':
                img = degrade(img, img.replace('.png', '_deg.jpg'))
            ref = rows = None
            ptt = read(os.path.join(REPO, 'raw', y, slug + '.pdftotext.txt')).split('\f')
            ref_text = ptt[p - 1] if p - 1 < len(ptt) else ''
            if setname in ('A', 'B'):
                ref = ref_text
                if kind == 'table':
                    lay = read(os.path.join(REPO, 'raw', y, slug + '.layout.txt')).split('\f')
                    rows = E.layout_rows(lay[p - 1]) if p - 1 < len(lay) else None
            elif sub.startswith('OmniPage'):
                ref = ref_text                      # good hidden OCR layer (agreement ~0.98)
            for e in engines:
                items, conf, dt, mem, cpu = measure(e, img)
                text = '\n'.join(to_lines(items))
                w = n = rc = None
                if ref:
                    w, n, rc = score(text, ref, rows)
                hdrs = sorted(set(m.group(1) for m in HDR.finditer(text)))
                res.append({'set': setname, 'kind': kind, 'sub': sub, 'page': '%s/%s/p%d' % (y, slug, p), 'engine': e.name,
                            'word_f1': w, 'num_recall': n, 'rows_complete': rc, 'headers': len(hdrs), 'conf': conf,
                            'sec': round(dt, 2), 'peak_mb': round(mem), 'cpu_pct': round(cpu), 'text': text[:4000]})
                print('%s %-22s %-14s %-10s w=%s n=%s rows=%s hdr=%d %.1fs %dMB cpu%d' % (
                    setname, sub[:22], res[-1]['page'], e.name, w and round(w, 3), n and round(n, 3), rc and round(rc, 2),
                    len(hdrs), dt, mem, cpu), flush=True)
    # engine agreement on real scans without a reference
    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    json.dump({'results': res, 'load_seconds': {e.name: round(getattr(e, 'load_s', 0), 1) for e in engines}},
              open(os.path.join(REPO, 'reports', 'eval', 'ocr_compare.json'), 'w', encoding='utf-8'), indent=1)
    summarise(res, engines)


def summarise(res, engines):
    out = []
    names = [e.name for e in engines]
    for setname, title in (('A', 'A. clean born-digital (300 dpi)'), ('B', 'B. degraded like a scan (150 dpi, JPEG, noise, tilt)'),
                           ('C', 'C. real scans')):
        out.append('== ' + title)
        for kind in sorted(set(r['kind'] for r in res if r['set'] == setname)):
            for n in names:
                rs = [r for r in res if r['set'] == setname and r['kind'] == kind and r['engine'] == n]
                if not rs: continue
                m = lambda k: (statistics.mean([r[k] for r in rs if r[k] is not None]) if any(r[k] is not None for r in rs) else None)
                fmt = lambda v, p=3: '-' if v is None else ('%.' + str(p) + 'f') % v
                out.append('  %-6s %-10s pages %2d | words F1 %s | numbers %s | rows complete %s | headers %d | %s s/page | peak %s MB | cpu %s%%' % (
                    kind, n, len(rs), fmt(m('word_f1')), fmt(m('num_recall')), fmt(m('rows_complete'), 2),
                    sum(r['headers'] for r in rs), fmt(m('sec'), 1), fmt(m('peak_mb'), 0), fmt(m('cpu_pct'), 0)))
    # agreement between engines on real scans (word F1 between their outputs)
    out.append('== C. engine agreement on real scans (word F1 between outputs)')
    for page in sorted(set(r['page'] for r in res if r['set'] == 'C')):
        t = {r['engine']: r for r in res if r['set'] == 'C' and r['page'] == page}
        pairs = []
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                if a in t and b in t:
                    pairs.append('%s~%s %.3f' % (a[:5], b[:5], E.f1(E.words(t[a]['text']), E.words(t[b]['text'])) or 0))
        out.append('  %-18s %-24s %s | headers %s' % (page, t[names[0]]['sub'][:24], ' '.join(pairs),
                                                    ' '.join('%s:%d' % (k[:5], v['headers']) for k, v in t.items())))
    txt = '\n'.join(out)
    open(os.path.join(REPO, 'reports', 'eval', 'ocr_compare.txt'), 'w', encoding='utf-8').write(txt + '\n')
    print('\n' + txt)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--summarise':
        d = json.load(open(os.path.join(REPO, 'reports', 'eval', 'ocr_compare.json'), encoding='utf-8'))
        class N:  # noqa
            def __init__(s, n): s.name = n
        summarise(d['results'], [N(n) for n in ('tesseract', 'paddleocr', 'rapidocr')])
    else:
        main()
