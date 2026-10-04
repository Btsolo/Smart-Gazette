"""
Scan lane (fix 5): turns a scanned gazette into the same notice text the
born-digital lane produces, so the categoriser and templates work unchanged.

Why a separate lane: 21 of 287 gazettes (2022-2025) are scans and one more is
mixed. Their text layer is missing (Foxit, PFU) or a hidden OCR guess
(OmniPage), so the positions extractor finds 0-1 notices in each - about
3,500 notices the system cannot see today.

Steps
  1. page text from an OCR engine (Tesseract cache in reports/ocr/<year>/<slug>/,
     or RapidOCR - see ocr_pages_rapid)
  2. OCR header repair: "GAZETTE N0TICE No, 8O6O" -> "GAZETTE NOTICE NO. 8060";
     "CAUSE No." -> "CAUSE NO."; O/l/I read inside numbers of a header
  3. the normal cleaner + vocabulary repair + ascending lock (pipeline.process)
  4. every notice is marked source=ocr so the app can flag it as lower confidence

Usage: python tools/scan_lane.py [--engine tesseract|rapidocr] 2022 2023 2024 2025
Writes cleaned/<year>/<slug>.scan.<engine>.txt and reports/eval/scan_lane_<engine>.json
"""
import argparse, csv, glob, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import gazette_clean as G
import vocab_repair as VR
import ocr_check as OC

# Header as OCR prints it: letters misread as digits and back, spaces inside
# the word, "No" / "N0" / "NO," / "NO:" - anything a reader would accept.
RE_HDR = re.compile(
    r'G\s?A\s?Z\s?E\s?T\s?T?\s?E\s+N\s?[O0o]\s?T\s?[I1l|]\s?C\s?E\s+N\s?[O0o]\s*[.,:;]?\s*'
    r'(?P<num>[0-9OoIl|SB](?:[0-9OoIl|SB ]{0,6}[0-9OoIl|SB])?)\b(?!\s*of\s+\d{4})',
    re.I)
RE_CAUSE = re.compile(r'\bC\s?A\s?U\s?S\s?E\s+N\s?[O0o]\s*[.,:;]?\s*(?=[A-Z]?\s*[0-9OoIl])', re.I)


# Headers Tesseract read but garbled beyond RE_HDR (2022 No 231: "GAZETTR
# NOTICE NO", "GAZETTE NOTICENO", "GAZETTE Nomice No", "GAZETTE NoTICB NO").
# A header stands alone on its line: letters close to GAZETTENOTICENO (edit
# distance <= 3) followed by the number and nothing else. No digits before
# the number: "Gazette Notice No. 123 of 2024" is a cross-reference.
RE_HDR_LINE = re.compile(r'^(?P<w>[A-Za-z .,]{10,26}?)[\s.,:;]*(?P<num>[0-9][0-9OoIlSB]{1,5})\s*[\'.,]?\s*$')
HDR_LETTERS = 'GAZETTENOTICENO'


def _edits(a, b):
    """Levenshtein distance (short strings only)."""
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def fuzzy_header(line):
    """'GAZETTR NOTICE NO 13318' -> 'GAZETTE NOTICE NO. 13318', else None."""
    m = RE_HDR_LINE.match(line)
    if not m:
        return None
    letters = re.sub(r'[^A-Za-z]', '', m.group('w')).upper()
    if not letters.startswith('GA') or _edits(letters, HDR_LETTERS) > 3:
        return None
    num = m.group('num').translate(OC.OCR_DIGITS)
    return 'GAZETTE NOTICE NO. ' + str(int(num)) if num.isdigit() else None


def fix_headers(text):
    """Canonical headers, so the cleaner splits OCR text like born-digital text.
    Only headers at the start of a line are rewritten: a cross-reference
    ("see Gazette Notice No. 123 of 2024") is lower case mid-sentence and is
    rejected by the (?!of YEAR) guard and the cleaner's own rules."""
    def hdr(m):
        num = re.sub(r'\s', '', m.group('num').translate(OC.OCR_DIGITS))
        if not num.isdigit():
            return m.group(0)
        return 'GAZETTE NOTICE NO. ' + str(int(num))
    out = []
    for line in text.split('\n'):
        s = line.lstrip()
        if s[:1] in 'Gg' and RE_HDR.match(s):
            line = RE_HDR.sub(hdr, s, count=1)
        elif s[:1] in 'Gg':
            line = fuzzy_header(s.rstrip()) or line
        out.append(line)
    text = '\n'.join(out)
    return RE_CAUSE.sub('CAUSE NO. ', text)


# --- OCR word correction (scan lane only) -----------------------------------
# A word is corrected only when (1) it is rare in the Gazette dictionary built
# from five years of born-digital text, and (2) ONE known OCR confusion turns
# it into a common Gazette word, clearly more common than any other candidate.
# Measured failures this fixes: "tor a grant", "Setters"/"l�tters" (letters),
# "tbe" (the), "arid" (and). Real rare words (names) have no such neighbour.
CONFUSIONS = [('t', 'f'), ('f', 't'), ('s', 'l'), ('l', 's'), ('i', 'l'), ('l', 'i'), ('rn', 'm'),
              ('m', 'rn'), ('cl', 'd'), ('li', 'h'), ('b', 'h'), ('h', 'b'), ('ri', 'n'), ('c', 'e'),
              ('e', 'c'), ('1', 'l'), ('0', 'o'), ('5', 's'), ('u', 'n'), ('n', 'u'),
              ('ii', 'u'), ('vv', 'w')]
# a<->o was dropped: in review it mostly changed names (Saita->Soita, Mola->Molo)
COMMON = 200        # a correction target must be at least this frequent
COMMON_CAP = 2000   # ... and this frequent when the word is capitalised (names!)
SEEN_MAX = 3        # a lowercase word seen more often than this is real; capitalised: never seen
MARGIN = 10         # the target must be this many times more frequent than the runner-up
RE_WORD = re.compile(r'(?<![A-Za-z�])[A-Za-z�][A-Za-z0-9�]{2,}(?![A-Za-z�])')


def _candidates(w):
    w = w.lower()
    if '�' in w:                        # unreadable glyph: any letter
        i = w.index('�')
        for c in 'abcdefghijklmnopqrstuvwxyz':
            yield w[:i] + c + w[i + 1:]
        return
    for a, b in CONFUSIONS:
        start = w.find(a)
        while start >= 0:
            yield w[:start] + b + w[start + len(a):]
            start = w.find(a, start + 1)


def correct_word(w, V):
    """Review of the first run (787 corrections) showed real names and words
    being changed (Shaw->Show, Mboga->Mbogo, laid->said), so: a word seen in
    five years of born-digital text is real; a capitalised word (usually a
    name) only becomes a very common word (Keriya->Kenya, Bemard->Bernard);
    a 3-letter word only becomes a short function word (tor->for, ori->on)."""
    cap = w[0].isupper()
    seen = V.get(w.lower(), 0)
    if '�' not in w and seen > (0 if cap else SEEN_MAX):
        return w
    cands = sorted(((V.get(c, 0), c) for c in set(_candidates(w))), reverse=True)
    if len(w) <= 3:
        cands = [x for x in cands if x[1] in VR.SHORT]
    if not cands or cands[0][0] < (COMMON_CAP if cap else COMMON):
        return w
    if len(cands) > 1 and cands[0][0] < MARGIN * cands[1][0]:
        return w
    best = cands[0][1]
    if w.isupper():
        return best.upper()
    if w[0].isupper():
        return best[0].upper() + best[1:]
    return best


def ocr_fix(text, V):
    """Character- and word-level OCR repair, before the normal cleaner."""
    # "CAUSE NO. E139 oF 2022", "E39 0F2025,. + By": the template needs
    # "OF <year> By" (1,600 blocks failed on "oF"; ~600 more on specks)
    text = re.sub(r'(CAUSE NO\.\s*[A-Z]?\s*\d+\s*)[oO0][fF]\s*(\d{4})[\s.,;:+|\'`i]*?(?=\s*\(?By\b)',
                  r'\1OF \2 ', text)
    # speckles read as a quote before a word: "'By", "estate 'of" - not O'Brien
    text = re.sub(r"(?:(?<=\s)|^)['‘’`](?=[A-Za-z])", '', text, flags=re.M)
    # specks inside a sentence: "for a: grant'", "who. died", "deceased's. last"
    text = re.sub(r"(?<=\b[a-z]{1})[:;'](?=\s+[a-z])|(?<=[a-z]{2})[:;'](?=\s+[a-z])", '', text)
    text = re.sub(r"\b(who|the|deceased's|a|of|for)\.(?=\s+[a-z])", r'\1', text)
    # digits: "! 5th" / "| 1th" (1 read as ! or |), "Sth" (5), "733~-20300" (speck)
    text = re.sub(r'[!|]\s?(?=\d)', '1', text)
    text = re.sub(r'\bS(?=th\b)', '5', text)
    text = re.sub(r'(?<=\d)~(?=[-\d])', '', text)
    # "aliasBikeri": the joiner of an OCR line glued the next name on
    text = re.sub(r'\balias(?=[A-Z])', 'alias ', text)
    return RE_WORD.sub(lambda m: correct_word(m.group(0), V), text)


def ocr_pages_tesseract(year, slug):
    d = os.path.join(REPO, 'reports', 'ocr', year, slug)
    # pNNNN.txt only (pNNNN.tab.txt is the tables engine's page text)
    files = sorted(f for f in glob.glob(os.path.join(d, 'p*.txt')) if re.match(r'p\d{4}\.txt$', os.path.basename(f)))
    return [open(f, encoding='utf-8', errors='replace').read() for f in files]


def ocr_pages_tables(year, slug):
    """Tesseract's word boxes with the tables rebuilt (scan_tables.page_text,
    written by tools/scan_tables_eval.py as pNNNN.tab.txt)"""
    d = os.path.join(REPO, 'reports', 'ocr', year, slug)
    files = sorted(f for f in glob.glob(os.path.join(d, 'p*.tab.txt')) if re.match(r'p\d{4}\.tab\.txt$', os.path.basename(f)))
    return [open(f, encoding='utf-8', errors='replace').read() for f in files]


def ocr_pages_rapid(year, slug):
    d = os.path.join(REPO, 'reports', 'ocr', year, slug)
    files = sorted(glob.glob(os.path.join(d, 'r*.txt')))
    return [open(f, encoding='utf-8', errors='replace').read() for f in files]


ENGINES = {'tesseract': ocr_pages_tesseract, 'rapidocr': ocr_pages_rapid, 'tables': ocr_pages_tables}


def second_page(year, slug, page, src='r'):
    """Second reading of one page: src 'r' = RapidOCR, 's' = Tesseract column
    pass (tess_second_pass.py). Accents removed (RapidOCR reads "Ochieng" as
    "Öchieng"); None if that page was not read."""
    import unicodedata
    p = os.path.join(REPO, 'reports', 'ocr', year, slug, '%s%04d.txt' % (src, page))
    if not os.path.exists(p):
        return None
    t = unicodedata.normalize('NFKD', open(p, encoding='utf-8', errors='replace').read())
    return ''.join(c for c in t if not unicodedata.combining(c))


def drop_outliers(text):
    """A header whose number is far from the issue's run (a cross-reference or
    a misread: 963 in an issue of 9618-9771, "1623382") is not a notice: it is
    lowered to plain text so it stays inside the notice it was printed in."""
    nums = [int(m.group(1)) for m in re.finditer(r'GAZETTE NOTICE NO\. (\d+)', text)]
    keep = OC.main_block(nums) if len(nums) >= 3 else set(nums)
    return re.sub(r'GAZETTE NOTICE NO\. (\d+)',
                  lambda m: m.group(0) if int(m.group(1)) in keep else 'Gazette Notice No. ' + m.group(1), text)


def scan_text(pages, vocab):
    """OCR pages -> cleaned notice text (same steps as pipeline.process)."""
    raw = '\n\n'.join(ocr_fix(fix_headers(p), vocab) for p in pages)
    return drop_outliers(G.apply_ascending_lock(VR.repair(G.clean(raw), vocab)))


def notice_map(text):
    out = {}
    for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text):
        m = re.match(r'GAZETTE NOTICE NO\. (\d+)', n)
        if m:
            out[int(m.group(1))] = n
    return out


def gap_windows(pages):
    """Gaps in the consecutive notice numbering of an issue, with the pages
    between the notices either side of each gap: [(a, b, first_page, last_page)].
    Notice numbers inside one issue run consecutively, so a gap is a header
    the OCR engine missed (its notice is glued to the one before)."""
    page_of, _ = OC.headers_by_page([fix_headers(p) for p in pages], RE_HDR, OC.OCR_DIGITS)
    nums = sorted(page_of)
    return [(a, b, page_of[a], page_of[b]) for a, b in zip(nums, nums[1:]) if 1 < b - a <= 30]


def two_witness_text(year, slug, vocab, src='r'):
    """Tesseract text, with each numbering gap re-read by RapidOCR: if RapidOCR
    finds notices inside the gap, its notices from the gap's first notice up
    to (not including) the next one replace Tesseract's. Returns (text, stats)."""
    pages = ocr_pages_tesseract(year, slug)
    base = notice_map(scan_text(pages, vocab))
    stats = {'gaps': 0, 'gaps_read': 0, 'recovered': 0}
    for a, b, pa, pb in gap_windows(pages):
        stats['gaps'] += 1
        rp = [second_page(year, slug, p, src) for p in range(pa, pb + 1)]
        if any(t is None for t in rp):
            continue
        stats['gaps_read'] += 1
        win = notice_map(scan_text(rp, vocab))
        inside = [n for n in win if a < n < b]
        if not inside or a not in base:
            continue
        for n in [a] + inside:
            if n in win:
                if n not in base:
                    stats['recovered'] += 1
                base[n] = win[n]
    text = ''.join(base[n] for n in sorted(base))
    return text, stats


def scans(years):
    for y in years:
        p = os.path.join(REPO, 'reports', 'ocr', y, 'gazettes.csv')
        for r in csv.DictReader(open(p, encoding='utf-8')):
            if r['kind'] != 'born-digital':
                yield y, r


def slots(nums):
    """Consecutive notice-number slots of an issue: the run from the first to
    the last number, with jumps > 30 (a numbering break) left out. Notice
    numbers inside an issue are consecutive, so this is an independent count
    of how many notices were printed - not one engine's own header count."""
    ns = sorted(set(nums))
    if not ns:
        return 0
    return len(ns) + sum(b - a - 1 for a, b in zip(ns, ns[1:]) if b - a <= 30)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('years', nargs='+')
    ap.add_argument('--engine', default='tesseract', choices=sorted(ENGINES) + ['two', 'two-tess'])
    a = ap.parse_args()
    import category_census as CEN
    import probate_template as P
    V = VR.load_corpus_vocab()
    rows = []
    for y, r in scans(a.years):
        pages = ocr_pages_tesseract(y, r['slug']) if a.engine.startswith('two') else ENGINES[a.engine](y, r['slug'])
        if not pages:
            continue
        st = {}
        if a.engine.startswith('two'):
            text, st = two_witness_text(y, r['slug'], V, 's' if a.engine == 'two-tess' else 'r')
        else:
            text = scan_text(pages, V)
        out_dir = os.path.join(REPO, 'cleaned', y)
        os.makedirs(out_dir, exist_ok=True)
        open(os.path.join(out_dir, '%s.scan.%s.txt' % (r['slug'], a.engine)), 'w', encoding='utf-8').write(text)
        found = notice_map(text)
        cats, tmpl = {}, {}
        blocks = blocks_ok = 0
        for num, n in found.items():
            c = CEN.categorise(n)
            cats[c] = cats.get(c, 0) + 1
            ok = CEN.template_for(c, n)
            if c in ('court_legal', 'land_property', 'Corrigenda'):
                t = tmpl.setdefault(c, [0, 0]); t[0] += 1; t[1] += bool(ok)
            if c == 'court_legal':
                bl = P.split_causes(n)
                blocks += len(bl); blocks_ok += sum(1 for b in bl if P.extract(b, n))
        sl = slots(found)
        rows.append({'year': y, 'slug': r['slug'], 'producer': r['producer'], 'pages': len(pages),
                     'slots': sl, 'found': len(found), 'two_witness': st,
                     'cats': cats, 'templates': tmpl, 'probate_blocks': [blocks, blocks_ok]})
        print('%s %-6s %-22s pages %3d | notices %3d of %3d slots (%5.1f%%) %s| probate blocks %d/%d | land %s'
              % (y, r['slug'], r['producer'][:22], len(pages), len(found), sl, 100.0 * len(found) / max(1, sl),
                 ('| gaps %(gaps)d read %(gaps_read)d recovered %(recovered)d ' % st) if st else '',
                 blocks_ok, blocks, tmpl.get('land_property')))
    S_ = sum(r['slots'] for r in rows); F = sum(r['found'] for r in rows)
    pb = [sum(r['probate_blocks'][i] for r in rows) for i in (0, 1)]
    pn = [sum(r['templates'].get('court_legal', [0, 0])[i] for r in rows) for i in (0, 1)]
    ln = [sum(r['templates'].get('land_property', [0, 0])[i] for r in rows) for i in (0, 1)]
    print('\nTOTAL gazettes %d | notices %d of %d consecutive slots (%.1f%%)' % (len(rows), F, S_, 100.0 * F / max(1, S_)))
    print('probate notices templated %d/%d (%.1f%%), blocks %d/%d (%.1f%%) | land %d/%d (%.1f%%)'
          % (pn[1], pn[0], 100.0 * pn[1] / max(1, pn[0]), pb[1], pb[0], 100.0 * pb[1] / max(1, pb[0]),
             ln[1], ln[0], 100.0 * ln[1] / max(1, ln[0])))
    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    json.dump(rows, open(os.path.join(REPO, 'reports', 'eval', 'scan_lane_%s.json' % a.engine), 'w'), indent=1)


if __name__ == '__main__':
    main()
