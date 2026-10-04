"""
Extractor evaluation - measurement only. How good is each extractor, and can
Tesseract be relied on where pdf-inspector fails (scans, tables)?

Extractors compared
  inspector  pdf-inspector extractText -> gazette_clean (what the pipeline reads)
  pdftotext  Xpdf text layer (the embedded text: our reference on born-digital)
  layout     pdftotext -layout (keeps printed rows on one line: table reference)
  tesseract  OCR of the rendered page (reports/ocr/<year>/)

Sections
  A. word / number accuracy vs the text layer, text pages vs table pages
  B. same notices, two extractors: segment Tesseract text by its own headers,
     run the real categoriser + templates, compare with the pipeline
  C. scans: what a Tesseract lane would deliver (notices, templates)
  D. tables: are rows complete and in order? (reference: pdftotext -layout)

Usage: python tools/extractor_eval.py 2022 2023 2024 2026
Writes reports/eval/<year>.json and prints a summary.
"""
import csv, glob, gzip, json, os, re, subprocess, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import corpus_report as R
import ocr_check as OC

REPO = R.REPO
WORD = re.compile(r'[a-z]{3,}')
NUM = re.compile(r'\d[\d,./-]*\d|\d')


def words(t): return Counter(WORD.findall(t.lower()))


def nums(t):
    """Digit runs of 3+ (thousands commas removed). Dashes and slashes split
    runs, so '81926–80100' and '81926-80100' compare equal."""
    return Counter(re.findall(r'\d{3,}', re.sub(r'(?<=\d),(?=\d{3})', '', t)))


def recall(ref, got):
    n = sum(ref.values())
    return (sum((ref & got).values()) / n) if n else None


def f1(a, b):
    na, nb = sum(a.values()), sum(b.values())
    return (2 * sum((a & b).values()) / (na + nb)) if na + nb else None


def mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 4) if xs else None


# --- Tesseract notice segmentation (its own headers, not the pdf-inspector cleaner)
def ocr_notices(pages):
    """Split OCR text at every GAZETTE NOTICE NO. header (any case, anywhere on
    a line), keep numbers in the main block(s), first occurrence wins."""
    text = '\n'.join(pages)
    hits = []
    for m in OC.HDR_OCR.finditer(text):
        s = m.group(1).translate(OC.OCR_DIGITS)
        if s.isdigit():
            hits.append((m.start(), m.end(), int(s)))
    keep = OC.main_block([h[2] for h in hits])
    hits = [h for h in hits if h[2] in keep]
    out, seen = {}, set()
    for i, (a, b, n) in enumerate(hits):
        end = hits[i + 1][0] if i + 1 < len(hits) else len(text)
        if n in seen:
            continue
        seen.add(n)
        out[n] = 'GAZETTE NOTICE NO. %d\n' % n + text[b:end].strip()
    return out


def template_path(n):
    try:
        return R.PL.route_notice(n)[3]
    except Exception:
        return 'error'


# --- tables: row completeness against pdftotext -layout ----------------------
TOK = re.compile(r'[a-z0-9]+')


def column_segments(page_text):
    """pdftotext -layout prints both columns of a two-column page on one line.
    Find the gutter (a character column between 35% and 65% of the width that
    is blank on almost every line) and split lines there; full-width tables
    cross the gutter, so none is found and lines stay whole."""
    lines = page_text.split('\n')
    width = max((len(l) for l in lines), default=0)
    busy = [l for l in lines if l.strip()]
    if width < 60 or len(busy) < 10:
        return lines
    occ = [0] * width
    for l in busy:
        for x, ch in enumerate(l):
            if ch != ' ':
                occ[x] += 1
    lo, hi = int(width * 0.35), int(width * 0.65)
    x = min(range(lo, hi), key=lambda i: occ[i])
    if occ[x] > 0.05 * len(busy):
        return lines
    return [l[:x] for l in lines] + [l[x:] for l in lines]


def layout_rows(page_text):
    rows = []
    for ln in column_segments(page_text):
        cells = [c for c in re.split(r'\s{2,}', ln.strip()) if c]
        if len(cells) >= 3 and any(re.fullmatch(r'[\d,./()%-]+', c) for c in cells):
            t = TOK.findall(ln.lower())
            if len(t) >= 3:
                rows.append(t)
    return rows


def index(tokens):
    ix = defaultdict(list)
    for i, t in enumerate(tokens):
        ix[t].append(i)
    return ix


def row_found(row, tokens, ix):
    """Best in-order match of the row's tokens inside a window of the stream.
    Returns the matched share (1.0 = every cell token present, in order)."""
    anchors = [t for t in row if len(t) >= 3 and t in ix] or [t for t in row if t in ix]
    if not anchors:
        return 0.0
    a = min(anchors, key=lambda t: len(ix[t]))
    best, L = 0, len(row)
    for pos in ix[a][:60]:
        lo = max(0, pos - 3 * L)
        j, got = lo, 0
        for t in row:
            k = j
            while k < min(len(tokens), lo + 6 * L + 10) and tokens[k] != t:
                k += 1
            if k < min(len(tokens), lo + 6 * L + 10):
                got += 1; j = k + 1
        best = max(best, got)
        if best == L:
            break
    return best / L


def ensure_layout(y, slug, pdf):
    path = os.path.join(REPO, 'raw', y, slug + '.layout.txt')
    if not os.path.exists(path):
        subprocess.run(['pdftotext', '-layout', '-enc', 'UTF-8', pdf, path], capture_output=True)
    return R.read_text(path) if os.path.exists(path) else ''


def main(years):
    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    for y in years:
        tabs = defaultdict(set)
        tpath = os.path.join(REPO, 'reports', 'corpus', y, 'tables.csv')
        if os.path.exists(tpath):
            for r in csv.DictReader(open(tpath, encoding='utf-8')):
                tabs[r['slug']].add(int(r['page']))
        A = defaultdict(list)          # metric -> values
        B = Counter()                  # same-notice template outcomes
        B_cat = Counter()
        B_notices = Counter()
        C = []                         # scans
        D = defaultdict(list)
        D_kind = defaultdict(lambda: defaultdict(list))
        kinds = {}
        if os.path.exists(tpath):
            for r in csv.DictReader(open(tpath, encoding='utf-8')):
                kinds[(r['slug'], int(r['page']))] = r['kind']
        examples = defaultdict(list)

        for sj in sorted(glob.glob(os.path.join(REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            slug = s['slug']
            ocr_pages = [open(p, encoding='utf-8', errors='replace').read()
                         for p in sorted(glob.glob(os.path.join(REPO, 'reports', 'ocr', y, slug, 'p*.txt')))]
            if s['kind'] in ('scanned', 'scan+ocr-layer'):
                on = ocr_notices(ocr_pages)
                ptt_s = R.read_text(os.path.join(REPO, 'raw', y, slug + '.pdftotext.txt')).split('\f')
                pn = ocr_notices(ptt_s)          # the scan's hidden OCR layer, via pdftotext
                paths, ppaths = Counter(), Counter()
                cats = Counter()
                for num, n in on.items():
                    c = R.canon(R.CEN.categorise(n)); cats[c] += 1
                    if c in ('Court_Legal', 'Land_Property', 'Corrigenda'):
                        paths[template_path(n)] += 1
                for num, n in pn.items():
                    if R.canon(R.CEN.categorise(n)) in ('Court_Legal', 'Land_Property', 'Corrigenda'):
                        ppaths[template_path(n)] += 1
                C.append({'slug': slug, 'pages': s['pages'], 'producer': s['producer'][:20], 'printed_T': s['notices_T'],
                          'ocr_lane_notices': len(on), 'layer_notices_pdftotext': len(pn), 'layer_agreement': s['mean_agreement'],
                          'ocr_conf': s['mean_ocr_conf'], 'template_ok': paths['template'],
                          'template_eligible': sum(paths.values()), 'layer_template_ok': ppaths['template'],
                          'layer_template_eligible': sum(ppaths.values()), 'categories': dict(cats.most_common(5))})
                continue

            ptt_pages = R.read_text(os.path.join(REPO, 'raw', y, slug + '.pdftotext.txt')).split('\f')
            raw = R.read_text(os.path.join(REPO, 'raw', y, slug + '.inspector.txt'))
            cleaned = R.G.apply_ascending_lock(R.G.clean(raw))

            # A. accuracy vs the text layer
            for p, ocr in enumerate(ocr_pages, 1):
                ref = ptt_pages[p - 1] if p - 1 < len(ptt_pages) else ''
                if len(WORD.findall(ref.lower())) < 30:
                    continue
                kind = 'table' if p in tabs[slug] else 'text'
                A['ocr_word_f1_' + kind].append(f1(words(ref), words(ocr)))
                A['ocr_num_recall_' + kind].append(recall(nums(ref), nums(ocr)))
            ref_all = '\n'.join(ptt_pages)
            A['inspector_word_recall'].append(recall(words(ref_all), words(cleaned)))
            A['inspector_num_recall'].append(recall(nums(ref_all), nums(cleaned)))
            A['ocr_word_recall_gazette'].append(recall(words(ref_all), words('\n'.join(ocr_pages))))
            A['ocr_num_recall_gazette'].append(recall(nums(ref_all), nums('\n'.join(ocr_pages))))

            # B. same notices through three extractors (categorised on the pipeline text)
            ins = {int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1)): n
                   for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', cleaned) if n.startswith('GAZETTE NOTICE NO.')}
            on = ocr_notices(ocr_pages)
            pn = ocr_notices(ptt_pages)                    # pdftotext text, same splitter
            B_notices['inspector'] += len(ins); B_notices['tesseract'] += len(on); B_notices['pdftotext'] += len(pn)
            common = set(ins) & set(on) & set(pn)
            B_notices['all three'] += len(common)
            for num in common:
                ci = R.CEN.categorise(ins[num])
                B_cat['tesseract same category' if R.CEN.categorise(on[num]) == ci else 'tesseract different category'] += 1
                B_cat['pdftotext same category' if R.CEN.categorise(pn[num]) == ci else 'pdftotext different category'] += 1
                if ci not in ('court_legal', 'land_property', 'Corrigenda'):
                    continue
                ok = {k: template_path(v[num]) == 'template' for k, v in (('inspector', ins), ('tesseract', on), ('pdftotext', pn))}
                cat = R.canon(ci)
                B[(cat, 'eligible')] += 1
                for k, v in ok.items():
                    B[(cat, k + ' ok')] += v
                B[(cat, 'any ok')] += any(ok.values())
                if ok['tesseract'] and not ok['inspector'] and len(examples['tess_not_insp']) < 6:
                    examples['tess_not_insp'].append('%s/%d' % (slug, num))
                if ok['pdftotext'] and not ok['inspector'] and len(examples['ptt_not_insp']) < 6:
                    examples['ptt_not_insp'].append('%s/%d' % (slug, num))
                if ok['inspector'] and not ok['pdftotext'] and len(examples['insp_not_ptt']) < 6:
                    examples['insp_not_ptt'].append('%s/%d' % (slug, num))

            # D. tables
            if tabs[slug]:
                lay_pages = ensure_layout(y, slug, os.path.join(R.REPO, '..', 'Testing', 'Material', 'Atricle raw material',
                                                                'M5', y, s['file'])).split('\f')
                ctoks = TOK.findall(cleaned.lower()); cix = index(ctoks)
                for p in sorted(tabs[slug]):
                    if p - 1 >= len(lay_pages):
                        continue
                    rows = layout_rows(lay_pages[p - 1])
                    if len(rows) < 3:
                        continue
                    otoks = TOK.findall(ocr_pages[p - 1].lower()); oix = index(otoks)
                    lt = [TOK.findall(l.lower()) for l in ocr_pages[p - 1].split('\n')]
                    for row in rows:
                        so = row_found(row, otoks, oix)
                        si = row_found(row, ctoks, cix)
                        # same printed line in OCR: all tokens on one OCR line, in order
                        one_line = any(all(t in set(l) for t in row) for l in lt if len(l) >= len(row) * 0.6)
                        D['ocr_row_complete'].append(so >= 0.9)
                        D['ocr_row_one_line'].append(one_line)
                        D['inspector_row_complete'].append(si >= 0.9)
                        D['ocr_token_share'].append(so)
                        D['inspector_token_share'].append(si)
                        k = kinds.get((slug, p), 'other')
                        D_kind[k]['ocr'].append(so >= 0.9)
                        D_kind[k]['ins'].append(si >= 0.9)
                        D_kind[k]['one_line'].append(one_line)

        res = {
            'A': {k: mean(v) for k, v in A.items()},
            'B': {'%s | %s' % k: v for k, v in sorted(B.items())}, 'B_notices': dict(B_notices), 'B_category': dict(B_cat),
            'B_examples': dict(examples),
            'C': C,
            'D': {k: round(sum(v) / len(v), 4) if v else None for k, v in D.items()}, 'D_rows': len(D['ocr_row_complete']),
            'D_kind': {k: {m: (round(sum(x) / len(x), 3), len(x)) for m, x in v.items()} for k, v in D_kind.items()},
        }
        json.dump(res, open(os.path.join(REPO, 'reports', 'eval', '%s.json' % y), 'w', encoding='utf-8'), indent=1)
        print('== %s' % y)
        print('A', json.dumps(res['A']))
        print('B notices', res['B_notices'], 'category agreement', res['B_category'])
        for k, v in res['B'].items():
            print('   B', k, v)
        print('   B examples', res['B_examples'])
        tc = sum(c['ocr_lane_notices'] for c in C); tp = sum(c['printed_T'] for c in C)
        te = sum(c['template_eligible'] for c in C); to = sum(c['template_ok'] for c in C)
        print('C scans %d: tesseract-lane notices %d of %d printed; templates %d/%d' % (len(C), tc, tp, to, te))
        for c in C:
            print('   C %s %-20s %3dp conf %s | tesseract %d notices, templates %d/%d | text layer (agree %s) %d notices, templates %d/%d' % (
                c['slug'], c['producer'], c['pages'], c['ocr_conf'], c['ocr_lane_notices'], c['template_ok'],
                c['template_eligible'], c['layer_agreement'], c['layer_notices_pdftotext'], c['layer_template_ok'],
                c['layer_template_eligible']))
        print('D rows %d' % res['D_rows'], json.dumps(res['D']))
        for k, v in sorted(res['D_kind'].items(), key=lambda kv: -kv[1]['ocr'][1]):
            print('   D %-24s rows %4d | complete: tesseract %.2f  inspector %.2f | tesseract same line %.2f' % (
                k, v['ocr'][1], v['ocr'][0], v['ins'][0], v['one_line'][0]))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2023'])
