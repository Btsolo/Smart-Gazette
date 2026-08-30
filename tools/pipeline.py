"""
Smart Gazette extraction pipeline, end to end.

    PDF text  ->  classify document  ->  clean  ->  segment  ->  route per notice
                                                                  |
                                    template extract + generate --+-- AI fallback

Every stage has an explicit fallback, and every notice is accounted for: the
run report states exactly how many notices were handled locally, how many need
an AI call, and why.

Document lanes
--------------
BORN_DIGITAL   standard path (all Word-produced gazettes)
SCANNED        poor embedded OCR layer (Fujitsu ScanSnap issues) -> OCR lane
ALT_LAYOUT     probate compilations with no GAZETTE NOTICE NO. headers
Table-heavy pages are flagged for a future structured-table lane.
"""
import re, os, sys, json, importlib.util
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
def _load(n):
    s = importlib.util.spec_from_file_location(n, os.path.join(HERE, n + '.py'))
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m

G  = _load('gazette_clean')
P  = _load('probate_template')
L  = _load('land_template')
C  = _load('corrigenda_template')
GEN= _load('generate')
CEN= _load('category_census')

# ---------------------------------------------------------------- lane 0
def classify_document(raw):
    """Decide which processing lane a document needs, before any cleaning."""
    sample = raw[:400000]
    glued   = len(re.findall(r'[A-Za-z]{24,}', sample))
    headers = len(re.findall(r'G\s*A\s*Z\s*E\s*T\s*T?\s*E\s*N\s*O\s*T\s*I\s*C\s*E\s*N\s*O', raw))
    causes  = len(re.findall(r'C\s*A\s*U\s*S\s*E\s*N\s*O', raw, re.I))

    # A probate compilation introduces each notice with a bare margin number
    # instead of a header, so cause blocks vastly outnumber headers.
    if causes > 10 and headers < max(causes / 20.0, 3):
        return 'ALT_LAYOUT', ('%d cause blocks but only %d notice headers - '
                              'bare-margin-number layout' % (causes, headers))
    # A scanned issue with a poor OCR layer runs words together.
    if glued > 20:
        return 'SCANNED', 'run-together text indicates a poor embedded OCR layer (%d long tokens)' % glued
    return 'BORN_DIGITAL', 'standard Word-produced gazette'

def flag_table_pages(text):
    """Notices whose content is predominantly tabular. Routed to a structured
    table lane when one exists; until then they go to the AI path intact."""
    hits = []
    for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text):
        if not n.startswith('GAZETTE NOTICE NO.'):
            continue
        if re.search(r'STATEMENT OF|SCHEDULE OF|UNCLAIMED|EXCHEQUER ISSUES|BALANCE SHEET', n, re.I) \
           and len(re.findall(r'\d[\d,\.]{3,}', n)) > 25:
            hits.append(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1))
    return hits

# ---------------------------------------------------------------- routing
def route_notice(notice):
    """Return (category, records, articles, path) for one notice.
    path is 'template' when handled locally, 'ai' when it must be escalated."""
    cat = CEN.categorise(notice)

    if cat == 'court_legal':
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', notice) if b.startswith('CAUSE NO.')]
        if not blocks:
            return cat, [], [], 'ai'
        recs = [P.extract(b, notice) for b in blocks]
        if not all(recs):
            return cat, [r for r in recs if r], [], 'ai'   # partial -> AI handles the rest
        arts = [GEN.generate(cat, r) for r in recs]
        return (cat, recs, arts, 'template') if all(arts) else (cat, recs, [], 'ai')

    if cat == 'land_property':
        r = L.extract(notice)
        if not r: return cat, [], [], 'ai'
        a = GEN.generate(cat, r)
        return (cat, [r], [a], 'template') if a else (cat, [r], [], 'ai')

    if cat == 'Corrigenda':
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', notice) if b.startswith('CAUSE NO.')]
        r = C.extract(blocks[0]) if blocks else None
        if not r: return cat, [], [], 'ai'
        a = GEN.generate(cat, r)
        return (cat, [r], [a], 'template') if a else (cat, [r], [], 'ai')

    return cat, [], [], 'ai'          # no template for this category yet

# ---------------------------------------------------------------- driver
def process(raw_path):
    data = open(raw_path, 'rb').read()
    try:    raw = data.decode('utf-16')
    except  UnicodeError: raw = data.decode('utf-8', errors='replace')

    lane, why = classify_document(raw)
    cleaned = G.apply_ascending_lock(G.clean(raw))
    notices = [n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', cleaned)
               if n.startswith('GAZETTE NOTICE NO.')]

    rep = {'file': os.path.basename(raw_path), 'lane': lane, 'why': why,
           'notices': len(notices), 'tables': flag_table_pages(cleaned),
           'by_cat': Counter(), 'template': 0, 'ai': 0,
           'calls_saved': 0, 'calls_needed': 0, 'articles': [],
           'land_records': [], 'digest': None}

    if lane != 'BORN_DIGITAL':
        rep['ai'] = len(notices)                  # whole doc escalates
        rep['calls_needed'] = max(len(notices), 1) * 2
        return rep

    for n in notices:
        cat, recs, arts, path = route_notice(n)
        rep['by_cat'][cat] += 1
        units = max(len(recs), 1)
        if path == 'template':
            rep['template'] += 1
            if cat == 'land_property':
                rep['land_records'].extend(recs)
            rep['calls_saved'] += units * 2       # extraction + generation
            rep['articles'].extend(arts)
        else:
            rep['ai'] += 1
            rep['calls_needed'] += units * 2

    # One digest above the individual land notices, when there are enough of
    # them to be worth grouping (mirrors processBatchedGroup).
    rep['digest'] = GEN.generate_group('land_property', rep['land_records'])
    return rep

if __name__ == '__main__':
    import glob
    files = sys.argv[1:] or sorted(glob.glob('/mnt/user-data/uploads/raw*.txt'))
    seen, tot = set(), []
    print('%-20s %-13s %7s %9s %6s %8s %8s' %
          ('file', 'lane', 'notices', 'template', 'ai', 'saved', 'needed'))
    print('-' * 80)
    for f in files:
        base = re.sub(r'Retest.*$', '', os.path.basename(f)[:-4])
        if base in seen: continue
        seen.add(base)
        r = process(f)
        tot.append(r)
        print('%-20s %-13s %7d %9d %6d %8d %8d' %
              (r['file'][:20], r['lane'], r['notices'], r['template'], r['ai'],
               r['calls_saved'], r['calls_needed']))
    print('-' * 80)
    N  = sum(r['notices'] for r in tot); T = sum(r['template'] for r in tot)
    A  = sum(r['ai'] for r in tot)
    S  = sum(r['calls_saved'] for r in tot); Nd = sum(r['calls_needed'] for r in tot)
    print('%-20s %-13s %7d %9d %6d %8d %8d' % ('TOTAL', '', N, T, A, S, Nd))
    print('\nnotices handled without AI : %d / %d  (%.1f%%)' % (T, N, 100*T/max(N,1)))
    print('AI calls avoided           : %d / %d  (%.1f%%)' % (S, S+Nd, 100*S/max(S+Nd,1)))
    tb = sum(len(r['tables']) for r in tot)
    lanes = Counter(r['lane'] for r in tot)
    print('document lanes             : %s' % dict(lanes))
    print('table-heavy notices flagged: %d (routed to AI until a table lane exists)' % tb)
    dg = [r for r in tot if r.get('digest')]
    if dg:
        gm = sum(r['digest']['grouped_count'] for r in dg)
        print('grouped land digests       : %d, covering %d individual notices' % (len(dg), gm))
