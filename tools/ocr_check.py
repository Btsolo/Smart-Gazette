"""
Tesseract OCR witness — measurement only (see CLAUDE.md, "Tesseract").

pdf-inspector and pdftotext both read the PDF's text layer; Tesseract reads
the rendered page image. This script OCRs gazettes and compares the three:

  1. Three-way notice check  - notice numbers from pdf-inspector (through the
     real cleaner + ascending lock), pdftotext and Tesseract. Two-against-one
     says which extractor is wrong. A disagreement is a flag, not a verdict.
  2. Page agreement          - word overlap between the text layer and OCR per
     page. Low agreement = broken text layer, hidden text, or a scan.
  3. OCR usability           - mean Tesseract word confidence per page.

Word boxes are kept (pNNNN.tsv.gz) for a later table inventory; nothing here
builds a table lane or touches the pipeline.

Usage:
  python tools/ocr_check.py --year 2023                 # whole year folder
  python tools/ocr_check.py --year 2023 --only 268 99   # just these issue numbers
  options: --workers 8  --force (redo analysis; OCR pages are always cached)

Speed (measured on this laptop, i7-8650U): one page per worker, 8 workers,
OMP_THREAD_LIMIT=1 -> ~2.2 s/page. Tesseract's own threading was slower.
Page images go to the local temp dir (not OneDrive) and are deleted once read.

Output (gitignored):
  raw/<year>/<slug>.inspector.txt, raw/<year>/<slug>.pdftotext.txt
  reports/ocr/<year>/<slug>/pNNNN.txt, pNNNN.tsv.gz, summary.json
  reports/ocr/<year>/gazettes.csv, pages.csv, notice_disagreements.csv
"""
import argparse, csv, gzip, json, os, re, subprocess, sys, tempfile, time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gazette_clean import clean, apply_ascending_lock

REPO   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS = r'C:\Users\User\OneDrive\Documents\Projects claude\Smart Gazette\Testing\Material\Atricle raw material\M5'
TMP    = os.path.join(tempfile.gettempdir(), 'smart_gazette_ocr')
DPI    = 300
LOW_AGREEMENT = 0.70     # page flagged as text layer vs OCR disagreement
EMPTY_LAYER   = 20       # fewer text-layer words than this = no text layer

OCR_ENV = dict(os.environ, OMP_THREAD_LIMIT='1')


def run(cmd, timeout=600, env=None):
    r = subprocess.run(cmd, capture_output=True, timeout=timeout, env=env, cwd=REPO)
    return r.returncode, r.stdout.decode('utf-8', errors='replace').replace('\r\n', '\n'), r.stderr.decode('utf-8', errors='replace')


# --- notice headers ---------------------------------------------------------
# Same rule as the cleaner: real headers are small caps (uppercase); cross-
# references are mixed case ("Gazette Notice No. 719 of 2026"), so matching is
# case-sensitive on GAZETTE NOTICE and rejects a trailing "of <year>".
#
# Unlike pdf-inspector, pdftotext and Tesseract do NOT give reliable reading
# order on two-column pages (5482 can come before 5481, and OCR can merge both
# columns onto one line). So no line anchor and no ascending-order (LIS) pass:
# instead keep the main block of numbers and drop far-away outliers.
HDR_TEXT = re.compile(r'G\s?AZET\s?T?E\s+N\s?OTICE\s+N\s?O\s*\.\s*(\d[\d ]{0,6}\d|\d)(?!\s*of\s+\d{4})')
HDR_OCR  = re.compile(r'G\s?A\s?Z\s?E\s?T\s?T?\s?E\s+N\s?[O0]\s?T\s?[I1l]\s?C\s?E\s+N\s?[O0o]\s*[.,:;]?\s*'
                      r'([0-9OoIl|SB]{1,6})\b(?!\s*of\s+\d{4})')
OCR_DIGITS = str.maketrans({'O': '0', 'o': '0', 'I': '1', 'l': '1', '|': '1', 'S': '5', 'B': '8'})
CLUSTER_GAP = 100        # a jump larger than this between sorted numbers splits blocks


def main_block(values):
    """Runs of sorted distinct numbers split at gaps > CLUSTER_GAP. Keep the
    largest run plus any run of >= 3 numbers: stray cross-references come one
    or two at a time, but the source itself can jump (2026 No 137 goes
    12299 -> 13000 and carries on to 13012)."""
    vs = sorted(set(values))
    if not vs:
        return set()
    blocks, cur = [], [vs[0]]
    for v in vs[1:]:
        if v - cur[-1] > CLUSTER_GAP:
            blocks.append(cur); cur = []
        cur.append(v)
    blocks.append(cur)
    big = max(blocks, key=len)
    return set(v for b in blocks if b is big or len(b) >= 3 for v in b)


def headers_by_page(pages, pattern, fix=None):
    """{number: first page} for candidates in the main block, and candidate count."""
    cands = []
    for p, txt in enumerate(pages, 1):
        for m in pattern.finditer(txt):
            s = m.group(1)
            s = re.sub(r'\s', '', s.translate(fix) if fix else s)
            if s.isdigit():
                cands.append((p, int(s)))
    kept = main_block([v for _, v in cands])
    page_of = {}
    for p, v in cands:
        if v in kept:
            page_of.setdefault(v, p)
    return page_of, len(set(v for _, v in cands))


def one_char_per_line(raw):
    """pdf-inspector output with ~1 character per line (seen on scans). clean()
    never flushes its join buffer on it and goes quadratic, so callers skip it."""
    ne = [l for l in raw.split('\n') if l.strip()]
    return len(ne) > 20000 and sum(len(l.strip()) for l in ne) / len(ne) < 3


def inspector_headers(raw):
    if one_char_per_line(raw):
        return []
    res = apply_ascending_lock(clean(raw))
    return [int(x) for x in re.findall(r'^GAZETTE NOTICE NO\. (\d+)', res, re.M)]


# --- per-page OCR -------------------------------------------------------------
def ocr_page(pdf, page, outdir):
    base = os.path.join(outdir, 'p%04d' % page)
    if os.path.exists(base + '.txt') and os.path.exists(base + '.tsv.gz'):
        return page, 0.0, True
    t0 = time.time()
    img = os.path.join(TMP, '%s_%d_%04d' % (os.path.basename(outdir), os.getpid(), page))
    rc, _, err = run(['pdftoppm', '-r', str(DPI), '-gray', '-png', '-singlefile',
                      '-f', str(page), '-l', str(page), pdf, img])
    if rc != 0:
        raise RuntimeError('pdftoppm p%d: %s' % (page, err.strip()[:200]))
    try:
        rc, _, err = run(['tesseract', img + '.png', base, '-l', 'eng', 'txt', 'tsv'], env=OCR_ENV)
        if rc != 0:
            raise RuntimeError('tesseract p%d: %s' % (page, err.strip()[:200]))
    finally:
        if os.path.exists(img + '.png'):
            os.remove(img + '.png')
    with open(base + '.tsv', 'rb') as f, gzip.open(base + '.tsv.gz', 'wb') as g:
        g.write(f.read())
    os.remove(base + '.tsv')
    return page, time.time() - t0, False


def ocr_confidence(outdir, page):
    confs = []
    with gzip.open(os.path.join(outdir, 'p%04d.tsv.gz' % page), 'rt', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE):
            try:
                c = float(row['conf'])
            except (TypeError, ValueError, KeyError):
                continue
            if c >= 0 and (row.get('text') or '').strip():
                confs.append(c)
    return (sum(confs) / len(confs)) if confs else 0.0


# --- per-gazette preparation (text layer) -------------------------------------
def pdf_info(pdf):
    _, out, _ = run(['pdfinfo', pdf])
    info = dict(l.split(':', 1) for l in out.splitlines() if ':' in l)
    return int(info.get('Pages', '0').strip() or 0), info.get('Producer', '').strip()


def prepare(g):
    """Text-layer extraction for one gazette (cached in raw/<year>/)."""
    raw_dir = os.path.join(REPO, 'raw', g['year'])
    os.makedirs(raw_dir, exist_ok=True)
    ins = os.path.join(raw_dir, g['slug'] + '.inspector.txt')
    ptt = os.path.join(raw_dir, g['slug'] + '.pdftotext.txt')
    if not os.path.exists(ins):
        rc, out, err = run(['node', os.path.join('tools', 'inspect.js'), g['pdf']])
        if rc != 0:
            out = ''
            g['inspector_error'] = err.strip().splitlines()[-1][:200] if err.strip() else 'exit %d' % rc
        open(ins, 'w', encoding='utf-8', newline='').write(out)   # keep node's "\n" as-is
    if not os.path.exists(ptt):
        run(['pdftotext', '-enc', 'UTF-8', g['pdf'], ptt])
    # Pages carrying a full-page raster image are scans, even when a hidden OCR
    # text layer sits on top (e.g. 2023 No 181, No 239: Law Reporting copies).
    # Judge by printed size (pixels / ppi), not pixels: 2024 No 95 is scanned
    # at 150 ppi (1182 px wide) - still a whole A4 page.
    _, out, _ = run(['pdfimages', '-list', g['pdf']])
    g['scan_pages'] = set()
    for ln in out.splitlines()[2:]:
        f = ln.split()
        try:
            page, w, h, xppi, yppi = int(f[0]), int(f[3]), int(f[4]), float(f[12]), float(f[13])
        except (IndexError, ValueError):
            continue
        if xppi > 0 and yppi > 0 and w / xppi >= 6.5 and h / yppi >= 9.0:     # >= ~A4 page, in inches
            g['scan_pages'].add(page)
    return g


# --- analysis ---------------------------------------------------------------
WORD = re.compile(r'[a-z]{3,}')


def agreement(a, b):
    ca, cb = Counter(WORD.findall(a.lower())), Counter(WORD.findall(b.lower()))
    na, nb = sum(ca.values()), sum(cb.values())
    if na + nb == 0:
        return 1.0, na, nb
    return 2 * sum((ca & cb).values()) / (na + nb), na, nb


def analyse(g, outdir):
    raw_dir = os.path.join(REPO, 'raw', g['year'])
    ins_raw = open(os.path.join(raw_dir, g['slug'] + '.inspector.txt'), encoding='utf-8', errors='replace').read()
    ptt_raw = open(os.path.join(raw_dir, g['slug'] + '.pdftotext.txt'), encoding='utf-8', errors='replace').read()
    tl_pages = ptt_raw.split('\f')[:g['pages']]
    tl_pages += [''] * (g['pages'] - len(tl_pages))
    ocr_pages = [open(os.path.join(outdir, 'p%04d.txt' % p), encoding='utf-8', errors='replace').read()
                 for p in range(1, g['pages'] + 1)]

    # 1. three-way notice check
    I = set(inspector_headers(ins_raw))
    P_page, P_cands = headers_by_page(tl_pages, HDR_TEXT)
    T_page, T_cands = headers_by_page(ocr_pages, HDR_OCR, OCR_DIGITS)
    P, T = set(P_page), set(T_page)
    disagreements = []
    for n in sorted(I | P | T):
        who = ''.join(k for k, s in (('I', I), ('P', P), ('T', T)) if n in s)
        if who != 'IPT':
            disagreements.append({'notice': n, 'found_by': who,
                                  'page': P_page.get(n) or T_page.get(n) or ''})

    # 2 + 3. page agreement and OCR confidence
    pages = []
    for p in range(1, g['pages'] + 1):
        ag, n_tl, n_ocr = agreement(tl_pages[p - 1], ocr_pages[p - 1])
        pages.append({'page': p, 'tl_words': n_tl, 'ocr_words': n_ocr,
                      'agreement': round(ag, 3), 'ocr_conf': round(ocr_confidence(outdir, p), 1)})
    empty = sum(1 for r in pages if r['tl_words'] < EMPTY_LAYER and r['ocr_words'] >= EMPTY_LAYER)
    low = [r['page'] for r in pages if r['tl_words'] >= EMPTY_LAYER and r['agreement'] < LOW_AGREEMENT]
    text_pages = [r for r in pages if r['ocr_words'] >= EMPTY_LAYER]
    scan_frac = len(g.get('scan_pages', ())) / max(1, g['pages'])
    if empty >= 0.5 * max(1, len(text_pages)):
        kind = 'scanned'
    elif scan_frac >= 0.5:
        kind = 'scan+ocr-layer'          # image pages with a hidden text layer
    elif empty:
        kind = 'mixed'
    else:
        kind = 'born-digital'

    summary = {
        'year': g['year'], 'issue': g['issue'], 'slug': g['slug'], 'file': os.path.basename(g['pdf']),
        'producer': g['producer'], 'pages': g['pages'], 'kind': kind,
        'scan_image_pages': len(g.get('scan_pages', ())),
        'notices_I': len(I), 'notices_P': len(P), 'notices_T': len(T),
        'cands_P': P_cands, 'cands_T': T_cands,
        'all_three': len(I & P & T), 'disagreements': len(disagreements),
        'I_min': min(I) if I else None, 'I_max': max(I) if I else None,
        'P_min': min(P) if P else None, 'P_max': max(P) if P else None,
        'mean_agreement': round(sum(r['agreement'] for r in text_pages) / len(text_pages), 3) if text_pages else None,
        'mean_ocr_conf': round(sum(r['ocr_conf'] for r in text_pages) / len(text_pages), 1) if text_pages else None,
        'no_text_layer_pages': empty, 'low_agreement_pages': low,
        'ocr_seconds': round(g.get('ocr_seconds', 0.0), 1),
        'inspector_error': g.get('inspector_error', ''),
    }
    json.dump({'summary': summary, 'pages': pages, 'disagreements': disagreements},
              open(os.path.join(outdir, 'summary.json'), 'w', encoding='utf-8'), indent=1)
    return summary


# --- driver -----------------------------------------------------------------
def discover(year, only):
    folder = os.path.join(CORPUS, year)
    gz, seen = [], set()
    for f in sorted(os.listdir(folder)):
        if not f.lower().endswith('.pdf'):
            continue
        m = re.search(r'No\.?\s*(\d+)', f, re.I)      # 2025 has "CXXVIINO 46.pdf"
        issue = int(m.group(1)) if m else None
        if only and issue not in only:
            continue
        slug = 'no%03d' % issue if issue is not None else re.sub(r'\W+', '_', f[:-4])
        while slug in seen:
            slug += '_b'
        seen.add(slug)
        gz.append({'year': year, 'issue': issue, 'slug': slug, 'pdf': os.path.join(folder, f)})
    return sorted(gz, key=lambda g: (g['issue'] is None, g['issue'] or 0))


def fmt(v):
    return '-' if v is None else v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', required=True)
    ap.add_argument('--only', type=int, nargs='*', help='issue numbers to process')
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--force', action='store_true', help='redo analysis even if summary.json exists')
    a = ap.parse_args()

    os.makedirs(TMP, exist_ok=True)
    rep = os.path.join(REPO, 'reports', 'ocr', a.year)
    os.makedirs(rep, exist_ok=True)
    gazettes = discover(a.year, set(a.only or []))
    t_start = time.time()

    with ThreadPoolExecutor(a.workers) as pool:
        for g, (pages, producer) in zip(gazettes, pool.map(lambda g: pdf_info(g['pdf']), gazettes)):
            g['pages'], g['producer'] = pages, producer
        total_pages = sum(g['pages'] for g in gazettes)
        print('year %s: %d gazettes, %d pages, %d workers' % (a.year, len(gazettes), total_pages, a.workers), flush=True)

        results, todo = {}, []
        for g in gazettes:
            g['outdir'] = os.path.join(rep, g['slug'])
            os.makedirs(g['outdir'], exist_ok=True)
            sj = os.path.join(g['outdir'], 'summary.json')
            if os.path.exists(sj) and not a.force:
                results[g['slug']] = json.load(open(sj, encoding='utf-8'))['summary']
            else:
                todo.append(g)

        # every page of every pending gazette goes into one pool, so small
        # gazettes don't leave workers idle; a gazette is analysed when its
        # last page and its text-layer extraction are both done
        futs, remaining, prep_done = {}, {}, set()
        for g in todo:
            g['ocr_seconds'], remaining[g['slug']] = 0.0, g['pages']
            futs[pool.submit(prepare, g)] = ('prep', g)
            for p in range(1, g['pages'] + 1):
                futs[pool.submit(ocr_page, g['pdf'], p, g['outdir'])] = ('page', g)

        done_pages, failures = 0, []
        for f in as_completed(futs):
            kind, g = futs[f]
            try:
                r = f.result()
            except Exception as e:
                failures.append('%s: %s' % (g['slug'], e))
                r = None
            if kind == 'prep':
                prep_done.add(g['slug'])
            else:
                remaining[g['slug']] -= 1
                done_pages += 1
                if r:
                    g['ocr_seconds'] += r[1]
            if remaining[g['slug']] == 0 and g['slug'] in prep_done and g['slug'] not in results:
                try:
                    s = analyse(g, g['outdir'])
                    results[g['slug']] = s
                    print('%-7s %3dp %-12s I/P/T %4s/%4s/%4s  disagree %3d  agree %s  conf %s  [%d/%d pages, %.0fs]' % (
                        g['slug'], s['pages'], s['kind'], s['notices_I'], s['notices_P'], s['notices_T'],
                        s['disagreements'], fmt(s['mean_agreement']), fmt(s['mean_ocr_conf']),
                        done_pages, total_pages, time.time() - t_start), flush=True)
                except Exception as e:
                    failures.append('%s analyse: %s' % (g['slug'], e))

    write_year_reports(a.year, rep, gazettes, results)
    elapsed = time.time() - t_start
    new_pages = sum(g['pages'] for g in todo)
    print('done in %.0fs (%s)' % (elapsed, '%.2f s/page over %d new pages' % (elapsed / new_pages, new_pages) if new_pages else 'all cached'))
    for fl in failures:
        print('FAILED', fl)


def write_year_reports(year, rep, gazettes, results):
    rows = [results[g['slug']] for g in gazettes if g['slug'] in results]
    if not rows:
        return
    cols = [k for k in rows[0] if k != 'low_agreement_pages'] + ['low_agreement_pages']
    with open(os.path.join(rep, 'gazettes.csv'), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, cols); w.writeheader()
        for r in rows:
            w.writerow(dict(r, low_agreement_pages=' '.join(map(str, r['low_agreement_pages']))))

    with open(os.path.join(rep, 'pages.csv'), 'w', newline='', encoding='utf-8') as fp, \
         open(os.path.join(rep, 'notice_disagreements.csv'), 'w', newline='', encoding='utf-8') as fd:
        wp = csv.DictWriter(fp, ['slug', 'page', 'tl_words', 'ocr_words', 'agreement', 'ocr_conf']); wp.writeheader()
        wd = csv.DictWriter(fd, ['slug', 'notice', 'found_by', 'page']); wd.writeheader()
        for r in rows:
            d = json.load(open(os.path.join(rep, r['slug'], 'summary.json'), encoding='utf-8'))
            for p in d['pages']:
                wp.writerow(dict(p, slug=r['slug']))
            for x in d['disagreements']:
                wd.writerow(dict(x, slug=r['slug']))

    # Lesson 14: numbers increment across every issue of the year. Check each
    # issue starts above where the previous one ended (per text-layer source).
    order = []
    for src in ('I', 'P'):
        prev = None
        for r in rows:
            lo, hi = r[src + '_min'], r[src + '_max']
            if lo is None:
                continue
            if prev and lo <= prev[1]:
                order.append('%s: %s starts at %d but %s ended at %d' % (src, r['slug'], lo, prev[0], prev[1]))
            prev = (r['slug'], hi)
    open(os.path.join(rep, 'ascending_across_issues.txt'), 'w', encoding='utf-8').write(
        '\n'.join(order) if order else 'OK: every issue starts above the previous one\n')

    tot = lambda k: sum(r[k] for r in rows)
    kinds = Counter(r['kind'] for r in rows)
    print('\n== %s totals: %d gazettes, %d pages  (%s)' % (year, len(rows), tot('pages'),
          ', '.join('%s %d' % kv for kv in kinds.items())))
    print('notices  I %d  P %d  T %d  | all three agree %d | disagreements %d' % (
        tot('notices_I'), tot('notices_P'), tot('notices_T'), tot('all_three'), tot('disagreements')))
    print('pages with no text layer %d | low-agreement pages %d' % (
        tot('no_text_layer_pages'), sum(len(r['low_agreement_pages']) for r in rows)))
    print('ascending across issues: %s' % ('OK' if not order else '%d breaks (see ascending_across_issues.txt)' % len(order)))
    print('reports: %s' % os.path.relpath(rep, REPO))


if __name__ == '__main__':
    main()
