"""
Pages without a text layer inside a born-digital issue (docs/specs/mixed-pages.md).

The extractor's text has one page per PDF page (form feed between pages). A
page whose text holds fewer than 5 words of >= 3 letters is read by OCR - the
scan lane's per-page path: Tesseract word boxes and the page image ->
scan_tables.page_text (tables, no fake cells) -> scan_lane fix_headers +
ocr_fix - and its text replaces the empty page.

  textless_pages(raw)       -> [page numbers, 1-based]
  fill(raw, ocr_page)       -> (text, pages filled)   ocr_page(page) -> text or None
  ocr_page_lab(pdf, year, slug, vocab)  the lab's ocr_page (OCR cache in reports/ocr)

Java: ScanLaneService.fillTextlessPages (keep in sync).

Usage: python tools/mixed_pages.py   (the 4 issues of the analysis, before / after)
"""
import os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WORD = re.compile(r'[A-Za-z]{3,}')
FIG = re.compile(r'\[\[FIGURE:\d+\.\d+\]\]')


def textless_pages(raw):
    # figure markers are not words (a map page holds only its marker)
    return [i + 1 for i, p in enumerate(raw.split('\f')) if len(WORD.findall(FIG.sub('', p))) < 5]


def fill(raw, ocr_page):
    """the extractor's text with every textless page replaced by its OCR text
    (a page keeps the "\\n" that ends it before the next form feed)"""
    pages = raw.split('\f')
    done = []
    for n in textless_pages(raw):
        text = ocr_page(n)
        if text is None or not text.strip():
            continue
        # the page's figure markers stay (the figures are captured separately)
        marks = FIG.findall(pages[n - 1])
        body = ('\n'.join(marks) + '\n' if marks else '') + text.strip('\n')
        pages[n - 1] = body + ('\n' if n < len(pages) else '')
        done.append(n)
    return '\f'.join(pages), done


def ocr_page_lab(pdf, year, slug, vocab):
    sys.path.insert(0, HERE)
    import ocr_check as OC
    import scan_tables as ST
    import scan_lane as SL
    out = os.path.join(REPO, 'reports', 'ocr', year, slug)
    os.makedirs(out, exist_ok=True)

    def ocr(page):
        OC.ocr_page(pdf, page, out)
        R = ST.image_rules(ST.render(pdf, page), 2, 150)
        text = ST.page_text(ST.read_words(os.path.join(out, 'p%04d.tsv.gz' % page)), R)
        return SL.ocr_fix(SL.fix_headers(text), vocab)
    return ocr


if __name__ == '__main__':
    import subprocess, time
    sys.path.insert(0, HERE)
    import vocab_repair as VR, gazette_clean as G, category_census as C
    V = VR.load_corpus_vocab()
    corpus = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')
    for y, n in [('2022', 43), ('2023', 99), ('2025', 163), ('2025', 190), ('2025', 73)]:
        pdf = os.path.join(corpus, y, [f for f in os.listdir(os.path.join(corpus, y)) if re.search(r'No\s*%d\.pdf$' % n, f)][0])
        raw = subprocess.run(['node', os.path.join(HERE, 'inspect_positions.js'), pdf], capture_output=True).stdout.decode('utf-8')
        t0 = time.time()
        text, done = fill(raw, ocr_page_lab(pdf, y, 'no%03d' % n, V))
        dt = time.time() - t0
        before = re.split(r'(?=GAZETTE NOTICE NO\. \d+)', G.apply_ascending_lock(VR.repair(G.clean(raw), V)))
        after = re.split(r'(?=GAZETTE NOTICE NO\. \d+)', G.apply_ascending_lock(VR.repair(G.clean(text), V)))
        nb = [p for p in before if p.startswith('GAZETTE NOTICE NO.')]
        na = [p for p in after if p.startswith('GAZETTE NOTICE NO.')]
        added = sum(len(WORD.findall(text.split('\f')[p - 1])) for p in done)
        print('%s No %d: pages %d | textless %d | OCR filled %d (%d words, %.0fs) | notices %d -> %d | categories %s -> %s' % (
            y, n, len(raw.split('\f')), len(textless_pages(raw)), len(done), added, dt, len(nb), len(na),
            sorted(set(C.categorise(p) for p in nb)), sorted(set(C.categorise(p) for p in na))))
