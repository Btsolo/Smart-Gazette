"""
Held-out check of whole issues - the corpus checks, run on any PDF.

For each PDF (e.g. a new gazette the system never saw while it was built):
  1. scan check (pdfimages) and the cover header (gazette_header)
  2. notices: cleaned positions text vs the printed numbering (consecutive
     slots) and vs pdftotext's headers (an independent second witness)
  3. words and numbers: how many of pdftotext's words / numbers survive in
     the cleaned text (bag-of-words recall)
  4. routing: Python categoriser, Java pre-filter port (AI triage calls),
     templates per category, and the AI extraction calls that remain
  5. tables: rows kept as rows with cells, checked against Tesseract word
     boxes of the table pages (the page image is the referee)
Writes reports/issues/<name>.md and prints it.

Usage: python tools/issue_report.py "path/to/Kenya Gazette ... No 166.pdf" [...]
"""
import collections, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import gazette_clean as G
import vocab_repair as VR
import corpus_report as R
import category_census as C
import ocr_check as OC
import extractor_eval as E
import table_eval as TE
import table_extract as T
import scan_lane as S
import gazette_header as GH

WORD = re.compile(r'[a-z]{3,}')
NUM = re.compile(r'\d[\d,./-]*\d|\d')


def run(cmd):
    return subprocess.run(cmd, capture_output=True, cwd=REPO).stdout


def notices(text):
    return {int(m.group(1)): n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text)
            for m in [re.match(r'GAZETTE NOTICE NO\. (\d+)', n)] if m}


def recall(ref, got):
    rc, gc = collections.Counter(ref), collections.Counter(got)
    tot = sum(rc.values())
    return 100.0 * sum(min(v, gc[k]) for k, v in rc.items()) / max(1, tot), tot


def report(pdf, V):
    name = re.sub(r'[^A-Za-z0-9]+', '_', os.path.splitext(os.path.basename(pdf))[0]).strip('_')
    work = os.path.join(REPO, 'raw', 'heldout')
    os.makedirs(work, exist_ok=True)
    pos_p = os.path.join(work, name + '.positions.txt')
    ptt_p = os.path.join(work, name + '.pdftotext.txt')
    if not os.path.exists(pos_p):
        open(pos_p, 'wb').write(run(['node', os.path.join('tools', 'inspect_positions.js'), pdf]))
    if not os.path.exists(ptt_p):
        subprocess.run(['pdftotext', '-enc', 'UTF-8', pdf, ptt_p], capture_output=True)
    raw = R.read_text(pos_p)
    ptt = open(ptt_p, encoding='utf-8', errors='replace').read()
    out = ['# Held-out check: %s' % os.path.basename(pdf), '']

    # 1. scan check + header
    pages = len(ptt.split('\f'))
    lst = run(['pdfimages', '-list', pdf]).decode('utf-8', 'replace').splitlines()[2:]
    scan_pages = set()
    for ln in lst:
        f = ln.split()
        try:
            if float(f[12]) > 0 and int(f[3]) / float(f[12]) >= 6.5 and int(f[4]) / float(f[13]) >= 9.0:
                scan_pages.add(int(f[0]))
        except (IndexError, ValueError):
            pass
    cleaned = G.apply_ascending_lock(VR.repair(G.clean(raw), V))
    hdr = GH.parse(cleaned)
    out += ['## 1. Lane and cover', '',
            '- pages %d, pages carrying a page-sized image %d -> %s' % (pages, len(scan_pages),
                                                                        'SCAN LANE' if len(scan_pages) >= pages / 2 else 'text layer'),
            '- cover: %s' % (hdr if hdr else 'NOT READ'), '']

    # 2. notices
    ns = notices(cleaned)
    nums = sorted(ns)
    slots = S.slots(nums)
    missing = sorted(set(range(nums[0], nums[-1] + 1)) - set(nums)) if nums else []
    ptt_hdr, _ = OC.headers_by_page(ptt.split('\f'), OC.HDR_TEXT)
    both = set(ptt_hdr) & set(nums)
    out += ['## 2. Notices', '',
            '| witness | notices |', '|---|---|',
            '| printed numbering (first..last, consecutive) | %d (%s-%s) |' % (slots, nums[0] if nums else '-', nums[-1] if nums else '-'),
            '| positions extractor + cleaner (the app) | **%d** |' % len(nums),
            '| pdftotext headers | %d |' % len(ptt_hdr),
            '| found by both | %d |' % len(both), '',
            '- missing from the numbering: %s' % (missing[:20] or 'none'),
            '- only pdftotext: %s | only the app: %s' % (sorted(set(ptt_hdr) - set(nums))[:10] or 'none',
                                                        sorted(set(nums) - set(ptt_hdr))[:10] or 'none'), '']

    # 3. words and numbers
    wr, wt = recall(WORD.findall(ptt.lower()), WORD.findall(cleaned.lower()))
    nr, nt = recall(NUM.findall(ptt), NUM.findall(cleaned))
    out += ['## 3. Words and numbers (pdftotext as reference)', '',
            '- words (a-z, 3+ letters): %.1f%% of %d kept' % (wr, wt),
            '- numbers: %.1f%% of %d kept' % (nr, nt), '']

    # 4. routing and templates
    rows = []
    k = collections.Counter()
    per = collections.defaultdict(lambda: [0, 0])
    for num, n in ns.items():
        py = C.categorise(n)
        jv = R.java_prefilter(re.sub(r'\s+', '', n.lower()), n)
        ok = bool(C.template_for(py, n))
        per[R.canon(py)][0] += 1
        per[R.canon(py)][1] += ok
        k['AI triage call (Java)'] += jv is None
        k['template'] += ok
        oversized = len(n) > 14000 and sum(len(t['rows']) for t in T.tables(n)) >= 5
        k['table record (oversized, no template)'] += (not ok) and oversized
        k['AI extraction call'] += (not ok) and not oversized
    out += ['## 4. Routing and templates', '',
            '| category | notices | by template |', '|---|---|---|']
    for cat, (a, b) in sorted(per.items(), key=lambda kv: -kv[1][0]):
        out.append('| %s | %d | %d |' % (cat, a, b))
    out += ['', '- %s' % ', '.join('%s: %d' % kv for kv in k.items()),
            '- (batched land notices also need fewer calls: one call per five notices)', '']

    # 5. tables vs the page image
    page_texts = raw.split('\f')
    tpages = [i + 1 for i, p in enumerate(page_texts) if sum(1 for l in p.split('\n') if ' | ' in l) >= 3]
    odir = os.path.join(REPO, 'reports', 'ocr', 'heldout', name)
    os.makedirs(odir, exist_ok=True)
    for p in tpages:
        OC.ocr_page(pdf, p, odir)
    toks = [E.TOK.findall(l.lower()) for l in cleaned.split('\n')]
    lines = cleaned.split('\n')
    ix = collections.defaultdict(list)
    for i, ts in enumerate(toks):
        for t in set(ts):
            ix[t].append(i)
    kept = cells = total = 0
    for p in tpages:
        for row, nc in TE.tesseract_rows('heldout', name, p):
            total += 1
            li = TE.kept(row, toks, ix, 0.8)
            if li >= 0:
                kept += 1
                cells += ' | ' in lines[li]
    tabs = sum(len(T.tables(n)) for n in ns.values())
    out += ['## 5. Tables (Tesseract word boxes of the table pages as referee)', '',
            '- table pages %d, tables extracted %d' % (len(tpages), tabs),
            '- table rows on those pages %d: kept as a row %.1f%%, with cells %.1f%%' % (
                total, 100.0 * kept / max(1, total), 100.0 * cells / max(1, total)), '']
    md = '\n'.join(out)
    os.makedirs(os.path.join(REPO, 'reports', 'issues'), exist_ok=True)
    open(os.path.join(REPO, 'reports', 'issues', name + '.md'), 'w', encoding='utf-8').write(md)
    return md


if __name__ == '__main__':
    V = VR.load_corpus_vocab()
    for pdf in sys.argv[1:]:
        print(report(pdf, V))
        print()
