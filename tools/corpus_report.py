"""
Year-level corpus report - measurement only (CLAUDE.md, "What I want from the
corpus run"). Nothing here changes the pipeline, cleaner, templates or Java.

Reads what tools/ocr_check.py already produced for a year:
  raw/<year>/<slug>.inspector.txt   pdf-inspector text (what the pipeline reads)
  raw/<year>/<slug>.pdftotext.txt   pdftotext text layer
  reports/ocr/<year>/<slug>/        Tesseract text + word boxes per page

and runs the real cleaner, lock, categoriser and templates over every notice.

Sections (files in reports/corpus/<year>/):
  gazettes.csv        per gazette: issue type, lane, notices, lock kept/demoted,
                      categories, template coverage, visibility
  notices.csv         one row per notice: categories (python / java / truth),
                      template path, failure cause, structure flags, heading
  failures.txt        template rejections grouped by cause, largest first
  schema_fill.txt     per schema field: how often templates fill it
  keywords.txt        precision of every candidate + Java pre-filter key, and
                      proposed new keys (>=95% precision)
  misc_clusters.txt   Miscellaneous / unrouted notices clustered by heading
  structure.txt       nested / compound notice shapes (multi-cause, multi-
                      parcel, cross-references, merged notices, schedules)
  scenarios.txt       layout / text anomalies, mapped to lessons 1-17 or NEW
  tables.csv/.txt     table pages from Tesseract word boxes, kind, columns,
                      rows, and how intact their rows survive cleaning
  visibility.csv      how much of the printed page the system can see, per stage
  samples.md          before/after content per issue type

Usage: python tools/corpus_report.py --year 2023
"""
import argparse, csv, glob, gzip, importlib.util, json, os, re, sys, time
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SCHEMAS = os.path.join(REPO, 'src', 'main', 'resources', 'schemas', 'field')


def _load(n):
    s = importlib.util.spec_from_file_location(n, os.path.join(HERE, n + '.py'))
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m


PL = _load('pipeline')                  # brings G, P, L, C, GEN, CEN with it
G, P, L, C, CEN = PL.G, PL.P, PL.L, PL.C, PL.CEN
KP = _load('keyword_probability')       # TRUTH, CANDIDATES, truth_of
NAMES = _load('names')
OC = _load('ocr_check')                 # header regexes for OCR text

# Canonical category names (schema file = name.lower() + '.json')
CANON = {'court_legal': 'Court_Legal', 'land_property': 'Land_Property', 'tenders': 'Tenders',
         'licensing': 'Licensing', 'company_registrations': 'Company_Registrations',
         'public_service_hr': 'Public_Service_HR', 'appointments': 'Appointments',
         'miscellaneous': 'Miscellaneous', 'Legislation': 'Legislation',
         'Corrigenda': 'Corrigenda', 'Change_of_Name': 'Change_of_Name',
         'County_Government': 'County_Government', 'Elections': 'Elections',
         'Uncollected_Goods': 'Uncollected_Goods', 'Environment': 'Environment',
         'Utility_Tariffs': 'Utility_Tariffs'}
canon = lambda c: CANON.get(c, c) if c else None

# Exact port of GazetteService.keywordPreFilter (Java). Keep in sync.
JAVA_RULES = [
    ('Corrigenda',            ['corrigendum', 'corrigenda']),
    ('Change_of_Name',        ['changeofname', 'byadeedpoll', 'deedpoll']),
    ('Court_Legal',           ['probateandadministration', 'intestate', 'takenoticethatanapplication',
                               'grantofprobate', 'intheestateof']),
    ('Land_Property',         ['landregistrationact', 'landregistrar', 'registeredasproprietor',
                               'titledeed', 'hasbeenlost', 'provisionalcertificate', 'certificateoflease']),
    ('Company_Registrations', ['companiesact', 'insolvencyact', 'struckoff']),
    ('Elections',             ['electionsact']),
    ('Legislation',           ['statutoryinstruments', 'bill,20']),
    ('Licensing',             ['miningact']),
    ('Appointments',          ['reappointment']),
]


def java_prefilter(sq, notice=None):
    """notice = the unsquashed text, for the heading rules (land title heading
    since fix 4, county heading since fix 6); without it only the keys run."""
    if notice is not None:
        if any(k in sq for k in JAVA_RULES[0][1]): return 'Corrigenda'
        if any(k in sq for k in JAVA_RULES[1][1]): return 'Change_of_Name'
        h = CEN.heading_category(notice)
        if h: return h
        hs = re.sub(r'\s+', '', notice[:300].lower())
        if any(k in hs for k in ('thelandregistrationact', 'thelandtitleact', 'thelandtitlesact', 'thelandact'))                 and 'probateandadministration' not in hs:
            return 'Land_Property'
    for cat, keys in JAVA_RULES:
        if any(k in sq for k in keys):
            return cat
    return None


def read_text(path):
    """UTF-16 only when a BOM says so. (pipeline.py tries utf-16 first, which
    silently garbles most UTF-8 files - see the report.)"""
    b = open(path, 'rb').read()
    if b[:2] in (b'\xff\xfe', b'\xfe\xff'):
        t = b.decode('utf-16')
    else:
        t = b.decode('utf-8', errors='replace')
    # pdf-inspector (and so the Java service) emits "\n". Files saved on
    # Windows in text mode carry "\r\n", which changes how the cleaner sees
    # line ends - normalise so we measure what production sees.
    return t.replace('\r\n', '\n')


WORDS = re.compile(r'[a-z]{3,}')
toks = lambda s: WORDS.findall(s.lower())


# --- per-notice analysis ------------------------------------------------------
def text_cause(t):
    """Text-level reason behind a grammar miss: the cleaner/extractor damage
    (or source quirk) that most likely broke the regex."""
    if re.search(r'amend\s*the\b.{0,120}printed\s+as', t, re.I | re.S):
        return 'corrigendum text glued into this notice (un-numbered CORRIGENDA section)'
    if re.search(r'knownas|titleNos?\.|known\s+a\s+s\b|kno\s+wn\s+as|sit\s+uate|undertitle|title\s+N\s*o\s+s?\.', t):
        return 'land anchor phrase glued/split ("knownas", "titleNo.", "known a s") - rigid \\s+ in template'
    if re.search(r'\b(?:19|20)\s+\d{2}\b(?!\s*(?:hectare|acre))|\b20\d\s\d\b', t): return 'split year ("20 21", "202 2")'
    if re.search(r'\b[Aa]\s(?:dministration|dvocates?|pplication|ppointment)', t): return 'drop-cap split ("a dministration")'
    if re.search(r'who\s+died\s+(?:at\s+[^,\n]+?\s*)?in\s+(?:19|20)\d\d', t, re.I): return 'grammar variant: died "in <year>" (no full date)'
    if re.search(r'\d\s*(?:st|nd|rd|th)[A-Z]', t): return 'ordinal glued to month ("7 thJanuary")'
    if re.search(r'OF\s+By\b', t): return 'year deleted (dateline rule ate the date line before it)'
    if re.search(r'who\s+died\s*(?:at\s+[^,]+,?\s*)?on\s*\.?\s*$|who\s+died\s*(?:at\s+[^,]+,?\s*)?on\s*\.', t, re.I):
        return 'date of death missing (dateline rule deleted it, or absent in source)'
    if re.search(r'Messrs\.?\s+of\b', t): return 'text missing in source ("Messrs. of")'
    if re.search(r'\b[A-Z]\s[a-z]{4,}', t): return 'drop-cap split ("A dministration")'
    if re.search(r'lastwill|writtenwill|bothof|allof|landsituate|thosepiece|undertitle', t): return 'glued words ("lastwill", "bothof")'
    if re.search(r'REGISTRA\s+TION|REGISTRAT\s+ION|REGISTRATI\s+ON|REGIST\s+RATION', t): return 'split word in Act heading ("REGISTRAT ION")'
    if re.search(r'\bof\s+that\s+piece|\bthat\s+piece\s+of', t) and not re.search(r'of\s*all\s*th', t): return 'grammar variant: "of that piece" (no "all")'
    if re.search(r'title\s+Nos?\.?\s*\n?\s*[A-Z]\.\s', t): return 'parcel id with abbreviation ("E. Bukusu/N. Sangalo")'
    if re.search(r'N\s*o\s+s\.', t): return 'split "Nos." (multi-parcel)'
    return 'other'


def diagnose(cat, notice):
    """Why a template rejected a notice: the first grammar piece that failed,
    plus the most likely text-level cause."""
    if cat == 'court_legal':
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', notice) if b.startswith('CAUSE NO.')]
        if not blocks:
            return 'probate: no CAUSE NO. block in notice'
        for b in blocks:
            m = P.RE_CAUSE.search(b)
            if not m: return 'probate: cause header (case_reference) not parsed | ' + text_cause(b[:80])
            a = P.RE_ACTION.search(m.group('body'))
            if not a: return 'probate: action clause (notice_subtype) not matched | ' + text_cause(b)
            if not P.RE_DEATH.search(a.group('tail')): return 'probate: death clause (date_of_death) not matched | ' + text_cause(a.group('tail'))
            if not P.extract(b, notice): return 'probate: required field empty (deceased/petitioners) | ' + text_cause(b)
        return 'probate: article generation failed'
    if cat == 'land_property':
        if not L.RE_ACT.search(notice):
            if not re.search(r'L\s*A\s*N\s*D\s*R\s*E\s*G\s*I\s*S\s*T\s*R\s*A\s*T\s*I\s*O\s*N|LAND\s*TITLES?\s*ACT', notice[:300]):
                return 'land: not a title notice (Land Act / planning / other), no template applies'
            return 'land: Act heading not matched by template | ' + text_cause(notice[:200])
        if not L.RE_PROP.search(notice):
            return 'land: proprietor clause (parties) not matched | ' + text_cause(notice)
        lr = L.RE_LR.search(notice) or L.RE_LR2.search(notice)
        if not lr: return 'land: parcel_id not found | ' + text_cause(notice)
        pid = re.sub(r'\s+', ' ', lr.group('lr')).strip(' ,.')
        if len(pid) < 4 or not re.search(r'\d', pid): return 'land: parcel_id unusable (fragment) | ' + text_cause(notice)
        return 'land: parties empty after name cleaning'
    if cat == 'Corrigenda':
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', notice) if b.startswith('CAUSE NO.')]
        if not blocks: return 'corrigenda: no CAUSE NO. (non-probate correction)'
        if not C.RE_CAUSE.search(blocks[0]): return 'corrigenda: cause reference not parsed'
        if not C.RE_AMEND.search(blocks[0]): return 'corrigenda: amend/printed as/to read clause not matched'
        return 'corrigenda: article generation failed'
    return 'no template for category'


ACT_LINE = re.compile(r'^(?:THE\s+)?[A-Z][A-Z0-9 ,\'’()\.\-&/]{3,120}?\b(?:ACT|CONSTITUTION OF KENYA|RULES|REGULATIONS)\b.*$')
IS_UPPER = lambda s: len(re.findall(r'[A-Z]', s)) >= 0.8 * max(1, len(re.findall(r'[A-Za-z]', s)))


RE_ACT_NAME = re.compile(r"\bTHE\s+(?!CONSTITUTION)[A-Z][A-Z\s,\-'’()&]*?\s*A\s?CT\b")
RE_CITATION = re.compile(r'^\s*,?\s*(?:\d{4}\s*)?(?:\((?:No|Cap|L\.N)[^)]*\)\s*)?')
STOP = re.compile(r'^(?:WHEREAS|IN|IT|NOTICE|TAKE|PURSUANT|PUBLIC|NOTIFICATION)$')


def heading(notice):
    """(Act, subject) from the heading block. The joiner often runs the heading
    into the body on one line, so read tokens: after the Act citation, the
    subject is the run of fully-uppercase tokens before the first mixed-case
    word. With no uppercase subject, fall back to the first body words."""
    head = re.sub(r'\s+', ' ', notice.split('\n', 1)[1] if '\n' in notice else '')[:500]
    head = re.sub(r'THE CONSTITUTION OF KENYA[, ]*(?:\d{4})?', ' ', head).strip()
    m = RE_ACT_NAME.search(head[:250])
    act = re.sub(r'\s+', ' ', m.group(0)) if m else ''
    rest = RE_CITATION.sub('', head[m.end():] if m else head)
    subj = []
    for tok in rest.split():
        if re.search(r'[a-z]', tok) or (subj and STOP.match(tok) and len(subj) >= 2):
            break
        subj.append(tok)
    subj = ' '.join(subj)
    if len(subj.split()) < 2:
        subj = '~ ' + ' '.join(rest.split()[:7])       # no uppercase subject line
    norm = lambda s: re.sub(r'\s+', ' ', re.sub(r'\d+', '#', s)).strip(' .,')[:90]
    return norm(act), norm(subj)


XREF = re.compile(r'Gazette\s+Notice\s+No[s]?\.?\s*\d+', re.I)


def structure(notice, cat):
    sq = re.sub(r'\s+', '', notice.lower())
    body = notice.split('\n', 1)[1] if '\n' in notice else ''
    f = {
        'cause_blocks': len(re.findall(r'CAUSE NO\.', notice)),
        'whereas': len(re.findall(r'\bWHEREAS\b', notice)),
        'title_nos': 1 if re.search(r'title\s+Nos\.', notice, re.I) else 0,
        'xrefs': len(XREF.findall(body)),
        'demoted_hdr': len(re.findall(r'^Gazette Notice No\. \d+\s*$', notice, re.M)),
        'dated': len(re.findall(r'\bDated\s+the\s+\d', notice)),
        'acts': len(set(re.findall(r'\bTHE\s+[A-Z][A-Z ,]{3,60}?\s+ACT\b', notice))),
        'numbered': len(re.findall(r'^\s*(?:\d{1,3}\.|\([a-z]\)|\([ivx]+\))\s', notice, re.M)),
        'schedule': 1 if re.search(r'\bSCHEDULE\b', notice) else 0,
        # embedded sections: annexes, tables, parts, appendices inside one notice
        'sections': len(re.findall(r'\b(?:ANNEX(?:URE)?\s*[\dIVX]*|APPENDIX\s*[\dIVX]*|TABLE\s*\d+|Table\s+\d+|PART\s+[IVX\d]+)\b', notice)),
        'num_density': round(len(re.findall(r'\b\d[\d,\.]{2,}\b', notice)) / max(1, len(notice.split())), 3),
    }
    shapes = []
    if cat in ('court_legal', 'Corrigenda') and f['cause_blocks'] > 1: shapes.append('multi-cause')
    if cat == 'land_property' and (f['whereas'] > 1 or f['title_nos']): shapes.append('multi-parcel')
    if f['xrefs']:
        kind = ('revokes' if 'revok' in sq else 'amends' if ('amend' in sq or 'corrigend' in sq)
                else 'extends' if 'extend' in sq else 'cites')
        shapes.append('xref-' + kind)
    if f['dated'] > 1: shapes.append('multi-signature')
    if f['demoted_hdr']: shapes.append('demoted-header-inside')
    if f['acts'] > 2: shapes.append('multi-act')
    if f['schedule']: shapes.append('schedule')
    if f['sections'] >= 2: shapes.append('embedded-sections')
    if f['numbered'] >= 5: shapes.append('numbered-list')
    if f['num_density'] > 0.15 and len(notice.split()) > 60: shapes.append('number-dense')
    f['shapes'] = shapes
    return f


FILL = lambda v: v not in (None, '', [], {})


# --- tables from Tesseract word boxes -----------------------------------------
def page_lines(tsv_gz, size=None):
    lines, width, height = defaultdict(list), 2480, 3508
    with gzip.open(tsv_gz, 'rt', encoding='utf-8', errors='replace') as f:
        for r in csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE):
            try:
                lvl = int(r['level'])
            except (ValueError, TypeError):
                continue
            if lvl == 1:
                width, height = int(r['width']), int(r['height'])
                if size is not None:
                    size['h'] = height
            if lvl == 5 and (r.get('text') or '').strip():
                lines[(r['block_num'], r['par_num'], r['line_num'])].append(
                    (int(r['left']), int(r['width']), int(r['top']), r['text']))
    out = []
    for ws in lines.values():
        ws.sort()
        out.append(ws)
    out.sort(key=lambda ws: ws[0][2])
    return out, width


def cells(ws, gap):
    cs, cur = [], [ws[0]]
    for w in ws[1:]:
        if w[0] - (cur[-1][0] + cur[-1][1]) > gap:
            cs.append(cur); cur = []
        cur.append(w)
    cs.append(cur)
    return cs


NUMERIC = re.compile(r'^[\d,\.\-/()%]+$')


RUNNING = re.compile(r'THE\s+KENYA\s+GAZETTE|^\d{1,2}(?:st|nd|rd|th)\s+\w+,?\s+\d{4}$|^\[?\d{1,5}\]?$')


def page_layout(tsv_gz):
    """Positions needed to follow a table across pages: notice headers (number,
    top), uppercase caption lines, page height."""
    size = {}
    lines, _ = page_lines(tsv_gz, size)
    hdrs, caps = [], []
    for ws in lines:
        text = ' '.join(w[3] for w in ws)
        for m in OC.HDR_OCR.finditer(text):
            s = m.group(1).translate(OC.OCR_DIGITS)
            if s.isdigit():
                hdrs.append((int(s), ws[0][2]))
        t = text.strip(' |')
        letters = re.findall(r'[A-Za-z]', t)
        if len(t.split()) >= 3 and letters and sum(1 for ch in letters if ch.isupper()) >= 0.85 * len(letters) \
                and not RUNNING.search(t) and not OC.HDR_OCR.search(t):
            caps.append(re.sub(r'\W+', ' ', t).strip()[:80])
    return {'h': size.get('h', 3508), 'hdrs': hdrs, 'caps': caps}


def table_page(tsv_gz):
    lines, width = page_lines(tsv_gz)
    gap = 0.025 * width
    rows = []
    for ws in lines:
        cs = cells(ws, gap)
        if len(cs) >= 3 and any(NUMERIC.match(''.join(w[3] for w in c)) for c in cs):
            rows.append(cs)
    if len(rows) < 8:
        return None
    first_top = min(r[0][0][2] for r in rows)
    last_top = max(r[0][0][2] for r in rows)
    lefts = sorted(c[0][0] for r in rows for c in r)
    clusters, cur = [], [lefts[0]]
    for x in lefts[1:]:
        if x - cur[-1] > 40:
            clusters.append(cur); cur = []
        cur.append(x)
    clusters.append(cur)
    cols = sum(1 for c in clusters if len(c) >= 0.3 * len(rows))
    text_rows = [' '.join(w[3] for c in r for w in c) for r in rows]
    return {'rows': len(rows), 'cols': max(cols, 3), 'row_texts': text_rows, 'row_cells': rows,
            'first_top': first_top, 'last_top': last_top, 'row_tops': [r[0][0][2] for r in rows]}


MIN_ROWS = 3        # fewer table-like lines than this is a stray (signature, date)


def table_runs(slug, pages, hdr_index):
    """Group table rows into tables that continue across page breaks.

    Each notice header on a page cuts the page's rows into segments: rows above
    the first header can continue the previous page's table; rows below the
    last header start the new notice's table. A table continues onto the next
    page when its last segment reaches the lower part of the page and the next
    page opens with >= MIN_ROWS rows before any header."""
    runs, cur = [], None

    def close():
        nonlocal cur
        if cur: runs.append(cur)
        cur = None

    for p in sorted(pages):
        tp, lay = pages[p]['table'], pages[p]['layout']
        if not tp:
            close(); continue
        tops = sorted(tp['row_tops'])
        hdrs = sorted(lay['hdrs'], key=lambda x: x[1])
        cuts = [t for _, t in hdrs]
        above = [t for t in tops if not cuts or t < cuts[0]]
        # 1. continuation of the previous page's table
        if cur and p == cur['pages'][-1] + 1 and cur['reaches_bottom'] and len(above) >= MIN_ROWS:
            cur['pages'].append(p); cur['rows'] += len(above)
            cur['cols'].append(tp['cols'])
            cur['reaches_bottom'] = not cuts and tops[-1] > 0.70 * lay['h']
            if cuts:
                cur['next_mid'] = hdrs[0][0]
                close()
        else:
            close()
        # 2. a table starting on this page, under the last header (or with no header)
        start = cuts[-1] if cuts else -1
        below = [t for t in tops if t > start]
        if cur is None and len(below) >= MIN_ROWS and (cuts or not above or len(above) < MIN_ROWS
                                                          or not runs or runs[-1]['pages'][-1] != p):
            owner = hdrs[-1][0] if hdrs else None
            if owner is None:
                prev = [n for (pp, t, n) in hdr_index if pp < p]
                owner = prev[-1] if prev else None
            cur = {'slug': slug, 'pages': [p], 'owner': owner, 'rows': len(below), 'cols': [tp['cols']],
                   'reaches_bottom': below[-1] > 0.70 * lay['h'], 'next_mid': ''}
    close()

    out = []
    for r in runs:
        if len(r['pages']) < 2:
            continue
        ps = r['pages']
        caps = Counter(c for p in ps for c in set(pages[p]['layout']['caps']))
        repeated = [c for c, k in caps.items() if k >= 2]
        out.append({'slug': slug, 'first_page': ps[0], 'last_page': ps[-1], 'n_pages': len(ps),
                    'owner_notice': r['owner'] or '', 'rows': r['rows'],
                    'cols_per_page': '/'.join(map(str, r['cols'])),
                    'repeated_captions': ' || '.join(repeated[:3]),
                    'next_notice_starts_mid_page': r['next_mid']})
    return out


# Row-level signatures first (what most rows look like), page keywords second.
ROW_KINDS = [
    ('personnel list',    re.compile(r'\b\d{4,7}\s+(?:PC|CPL|SGT|IP|CI|SSP|SP|SIP|PC/W|CPL/W)\b')),
    ('customs goods list', re.compile(r'\b[A-Z]{4}\s?\d{6,7}\b|\bCFS\b|\bICD\b', re.I)),
    ('probate cause list', re.compile(r'\b[A-Z]?\d{1,4}\s*/\s*20\d\d\b.*\b(?:intestate|testate|probate)\b', re.I)),
    ('land schedule',     re.compile(r'\b[A-Za-z][\w\. ]*/[\w\. ]+/\s?\d+\b|\bL\.?R\.?\s*No|\b0\.\d{3,4}\b')),
]


def table_kind(page_text, rows):
    votes = Counter()
    for r in rows:
        for k, rx in ROW_KINDS:
            if rx.search(r):
                votes[k] += 1; break
    if votes and votes.most_common(1)[0][1] >= 0.3 * len(rows):
        return votes.most_common(1)[0][0]
    u = page_text.upper()
    if 'EXCHEQUER' in u or 'CONSOLIDATED FUND' in u or 'STATEMENT OF ACTUAL' in u: return 'exchequer'
    if 'UNCLAIMED' in u: return 'unclaimed assets'
    if 'POLITICAL PART' in u: return 'party list'
    if 'TARIFF' in u: return 'tariff schedule'
    if re.search(r'\b(?:POLLING|WARD|CONSTITUENCY|RETURNING OFFICER|ELECTION PETITION)\b', u): return 'electoral'
    if re.search(r'ADMISSION OF ADVOCATES|PETITIONED FOR ADMISSION', u): return 'advocates admission list'
    if re.search(r'KSH|KES|SHILLINGS|BUDGET|ALLOCATION|REVENUE', u): return 'financial statement'
    return 'other'


def row_adjacency(row_cells, bigrams):
    """Share of cell boundaries whose words stay next to each other after
    cleaning. Columns interleaved by the extractor -> boundaries broken."""
    ok = tot = 0
    for a, b in zip(row_cells, row_cells[1:]):
        ta = re.findall(r'[a-z0-9]+', ' '.join(w[3] for w in a).lower())
        tb = re.findall(r'[a-z0-9]+', ' '.join(w[3] for w in b).lower())
        if ta and tb:
            tot += 1; ok += (ta[-1], tb[0]) in bigrams
    return ok, tot


# --- driver ---------------------------------------------------------------------
def issue_type(ptt_head, kind, notices_cats):
    if kind in ('scanned', 'scan+ocr-layer'): return 'scanned'
    h = ptt_head.upper()
    county = sum(1 for c in notices_cats if c == 'county') >= max(1, 0.5 * len(notices_cats))
    if 'SPECIAL ISSUE' in h: return 'county' if county else 'special'
    if 'CONTENTS' in h or 'SUPPLEMENT' in h: return 'main'
    return 'other'


def raw_slice(raw, num, n=900):
    pat = r'G\s*A\s*Z\s*E\s*T\s*T?\s*E\s*N\s*O\s*T\s*I\s*C\s*E\s*N\s*O\s*\.\s*' + r'\s*'.join(str(num))
    m = re.search(pat, raw)
    return raw[m.start():m.start() + n] if m else ''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', required=True)
    ap.add_argument('--progress', action='store_true', help='print time per gazette')
    a = ap.parse_args()
    y = a.year
    ocr_dir = os.path.join(REPO, 'reports', 'ocr', y)
    out = os.path.join(REPO, 'reports', 'corpus', y)
    os.makedirs(out, exist_ok=True)
    summaries = sorted((json.load(open(f, encoding='utf-8')) for f in glob.glob(os.path.join(ocr_dir, '*', 'summary.json'))),
                       key=lambda d: d['summary']['issue'] or 0)
    if not summaries:
        sys.exit('no OCR summaries for %s - run tools/ocr_check.py --year %s first' % (y, y))

    schema_fields = {c: list(json.load(open(os.path.join(SCHEMAS, c.lower() + '.json')))['definitions']['item']['properties'])
                     for c in ('court_legal', 'land_property', 'corrigenda')}
    fill = {k: Counter() for k in schema_fields}
    fill_n = Counter()
    fail_ex = defaultdict(list)
    fail_blocks = Counter()     # probate: per cause block, not per notice
    multi_tables = []           # tables continuing across page breaks
    notice_rows, gaz_rows, vis_rows, table_rows = [], [], [], []
    scen = defaultdict(list)
    samples = defaultdict(list)
    all_notices = []            # (slug, num, cleaned notice, truth)

    for d in summaries:
        s = d['summary']; slug = s['slug']
        if a.progress:
            print('  %s %dp ...' % (slug, s['pages']), end=' ', flush=True); t_g = time.time()
        raw = read_text(os.path.join(REPO, 'raw', y, slug + '.inspector.txt'))
        ptt = read_text(os.path.join(REPO, 'raw', y, slug + '.pdftotext.txt'))
        ocr_pages = [open(p, encoding='utf-8', errors='replace').read()
                     for p in sorted(glob.glob(os.path.join(ocr_dir, slug, 'p*.txt')))]
        ocr_all = '\n'.join(ocr_pages)

        lane, _ = PL.classify_document(raw)
        # Guard: one-character-per-line output (seen on scans) never reaches a
        # line ending in . ; : so clean()'s join buffer never flushes and the
        # loop goes quadratic (hours). Record it instead of hanging.
        ne = [l for l in raw.split('\n') if l.strip()]
        if len(ne) > 20000 and sum(len(l.strip()) for l in ne) / len(ne) < 3:
            scen['NEW: one character per line from pdf-inspector - clean() goes quadratic (hangs)'].append(
                '%s: %d lines, mean %.1f chars' % (slug, len(ne), sum(len(l.strip()) for l in ne) / len(ne)))
            raw = ''
        pre = G.clean(raw)
        cands = len(re.findall(r'^GAZETTE NOTICE NO\. \d+', pre, re.M))
        cleaned = G.apply_ascending_lock(pre)
        notices = [n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', cleaned) if n.startswith('GAZETTE NOTICE NO.')]
        source = 'inspector'
        # Scans: the pipeline sees nothing. Show what an OCR lane would see.
        if s['kind'] in ('scanned', 'scan+ocr-layer'):
            source = 'ocr (pipeline sees %d)' % len(notices)
            oc = G.apply_ascending_lock(G.clean(ocr_all))
            notices = [n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', oc) if n.startswith('GAZETTE NOTICE NO.')]
        demoted = cands - len(re.findall(r'^GAZETTE NOTICE NO\. \d+', cleaned, re.M))

        # ---- scenarios in the raw extractor output
        if '\x00' in raw:
            scen['NEW: NUL characters in pdf-inspector output'].append('%s: %d NULs, %d right after a header' % (
                slug, raw.count('\x00'), len(re.findall(r'N\s*O\s*\.\s*[\d\s]*\d\s*\n\s*\x00', raw))))
        if raw.count('�') > 20:
            scen['encoding: U+FFFD replacement characters'].append('%s: %d' % (slug, raw.count('�')))
        if s['kind'] == 'scan+ocr-layer' and len(re.findall(r'[A-Za-z]', raw)) and \
                sum(1 for ch in raw[:3000] if ch in '$&()*+') > 60:
            scen['NEW: glyph-shifted text (non-embedded CID font, scan OCR layer)'].append(
                '%s: pdf-inspector text shifted by 29 code points, digits/spaces lost' % slug)

        cats_here, tmpl_ok, tmpl_n = Counter(), 0, 0
        for n in notices:
            num = int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1))
            sq = re.sub(r'\s+', '', n.lower())
            py = CEN.categorise(n)
            cat, recs, arts, path = PL.route_notice(n)
            truth = KP.truth_of(n)
            jv = java_prefilter(sq, n)
            st = structure(n, py)
            act, subj = heading(n)
            countyish = bool(re.search(r'COUNTY\s+GOVERNMENT\s+OF|COUNTY\s+ASSEMBLY|COUNTY\s+PUBLIC\s+SERVICE\s+BOARD', n[:600]))
            reason = ''
            if path == 'template':
                tmpl_ok += 1
                key = {'court_legal': 'court_legal', 'land_property': 'land_property', 'Corrigenda': 'corrigenda'}[py]
                for r in recs:
                    fill_n[key] += 1
                    for fld in schema_fields[key]:
                        if FILL(r.get(fld)): fill[key][fld] += 1
            elif py in ('court_legal', 'land_property', 'Corrigenda'):
                reason = diagnose(py, n)
                fail_ex[(py, reason)].append('%s/%d' % (slug, num))
            if py in ('court_legal', 'land_property', 'Corrigenda'):
                tmpl_n += 1
            blocks = blocks_ok = ''
            if py == 'court_legal':
                bl = [b for b in re.split(r'(?=CAUSE NO\.)', n) if b.startswith('CAUSE NO.')]
                blocks, blocks_ok = len(bl), sum(1 for b in bl if P.extract(b, n))
                for b in bl:
                    if not P.extract(b, n):
                        fail_blocks[diagnose('court_legal', b)] += 1
            cats_here[canon(py)] += 1
            words = len(n.split())
            notice_rows.append({
                'slug': slug, 'notice': num, 'words': words, 'source': source.split()[0],
                'python_cat': canon(py), 'java_cat': jv or '(AI triage)', 'truth': canon(truth) or '',
                'county_signal': int(countyish), 'path': path, 'fail_reason': reason,
                'cause_blocks_total': blocks, 'cause_blocks_ok': blocks_ok,
                'shapes': ' '.join(st['shapes']), 'cause_blocks': st['cause_blocks'], 'xrefs': st['xrefs'],
                'dated': st['dated'], 'act': act, 'subject': subj})
            all_notices.append((slug, num, n, truth, sq))

            # ---- scenarios inside notices
            if 'THE KENYA GAZETTE' in n: scen['lesson 4: running header left inside notice'].append('%s/%d' % (slug, num))
            if re.search(r'\b\d{1,2}:\d{2}\s*[AP]\.?M\b', n): scen['lesson 4: timestamp left inside notice'].append('%s/%d' % (slug, num))
            if re.search(r'\.{6,}', n): scen['lesson 10: dot leaders (contents index) inside notice'].append('%s/%d' % (slug, num))
            if re.search(r'[A-Za-z]{28,}', n): scen['lesson 13: glued run-together token (28+ letters)'].append('%s/%d' % (slug, num))
            if words < 15: scen['short notice (<15 words): possible split or header-only'].append('%s/%d' % (slug, num))
            if st['dated'] > 1 and py not in ('court_legal',): scen['NEW?: several "Dated the" signatures: possible merged notices'].append('%s/%d' % (slug, num))
            if st['demoted_hdr']: scen['lesson 6/9: demoted header left inside a notice (merged)'].append('%s/%d' % (slug, num))

            samples[slug].append((num, py, n))

        # ---- content dates deleted by the running-header dateline rule (P_DATE)
        rl = [l.strip() for l in raw.split('\n')]
        prev, eaten = '', 0
        for i, ln in enumerate(rl):
            if not ln: continue
            if G.P_DATE.match(ln) and not re.search(r'THE\s+KENYA\s+GAZETTE', ' '.join(rl[max(0, i - 4):i + 5])) \
                    and re.search(r'(?:\bon|,|died|dated|the)\s*$', prev, re.I):
                eaten += 1
            prev = ln
        if eaten:
            scen['NEW: content date line deleted as a running-header dateline (P_DATE), '
                 'then following numbers deleted as page numbers'].append('%s: %d' % (slug, eaten))

        # ---- text damage done by extraction + joining (whole cleaned gazette)
        if source == 'inspector':
            for label, rx in (
                    ('NEW: split year inside a date ("20 21")', r'\b(?:19|20)\s+\d{2}\b(?=\s*[.,;)]|\s+(?:By|The|and)\b)'),
                    ('NEW: ordinal glued to month ("7 thJanuary")', r'\d\s*(?:st|nd|rd|th)(?:January|February|March|April|May|June|July|August|September|October|November|December)'),
                    ('NEW: drop-cap letter split off ("A dministration", "a pplication")', r'\b[Aa] (?:dministration|pplication|dvocates?|ppointment)'),
                    ('NEW: word split inside Act heading ("REGISTRAT ION")', r'REGISTRA\s+TION|REGISTRAT\s+ION|REGISTRATI\s+ON'),
                    ('lesson 13: glued common words ("bothof", "landsituate", "lastwill")', r'bothof|landsituate|thosepiece|lastwill|undertitle'),
                    ('NEW: curly punctuation not normalised (’ “ ”) - template regexes expect ASCII', r'[’“”]'),
                    ('NEW: un-numbered CORRIGENDA text glued into a neighbouring notice', r'CAUSE NO\.[^\n]{0,60}amend\s*the\b')):
                k = len(re.findall(rx, cleaned))
                if k:
                    scen[label].append('%s: %d' % (slug, k))

        # ---- visibility: printed words (OCR) -> text layer -> cleaned -> inside notices
        # content  = the word's letters are present in the text (spaces ignored),
        #            i.e. the system can see it, even if the joiner mangled it
        # intact   = the word survives as the same word (joiner did no damage)
        o = Counter(toks(ocr_all))
        tl, cl = Counter(toks(ptt)), Counter(toks(cleaned))
        on = max(1, sum(o.values()))
        sq_clean = re.sub(r'[^a-z]', '', cleaned.lower())
        sq_notes = re.sub(r'[^a-z]', '', '\n'.join(notices).lower())
        present = lambda sqt: round(sum(v for w, v in o.items() if w in sqt) / on, 3)
        tl_n = max(1, sum(tl.values()))
        malformed = sum(v for w, v in cl.items() if w not in tl)
        vis = {'slug': slug, 'kind': s['kind'], 'ocr_words': sum(o.values()),
               'text_layer': round(sum((o & tl).values()) / on, 3),
               'content_in_cleaned': present(sq_clean) if source == 'inspector' else 0.0,
               'content_inside_notices': present(sq_notes) if source == 'inspector' else 0.0,
               'words_intact_after_clean': round(sum((tl & cl).values()) / tl_n, 3) if source == 'inspector' else 0.0,
               'malformed_words_created': malformed if source == 'inspector' else '',
               'notices_seen_by_pipeline': len(notices) if source == 'inspector' else int(re.search(r'\d+', source).group()),
               'notices_printed_ocr': s['notices_T'], 'notices_text_layer': s['notices_P']}
        vis_rows.append(vis)

        # ---- tables
        ct = re.findall(r'[a-z0-9]+', cleaned.lower())
        bigrams = set(zip(ct, ct[1:]))
        flagged = PL.flag_table_pages(cleaned)
        page_info, hdr_index = {}, []
        for p in range(1, len(ocr_pages) + 1):
            tsv = os.path.join(ocr_dir, slug, 'p%04d.tsv.gz' % p)
            lay = page_layout(tsv)
            page_info[p] = {'layout': lay, 'table': table_page(tsv)}
            hdr_index += [(p, top, n) for n, top in sorted(lay['hdrs'], key=lambda x: x[1])]

        # ---- tables that continue across pages, and where their rows land
        note_sq = {int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1)): re.sub(r'[^a-z0-9]', '', n.lower())
                   for n in notices} if source == 'inspector' else {}
        for run in table_runs(slug, page_info, hdr_index):
            first = page_info[run['first_page']]['table']
            own = run['owner_notice'] or None
            run['owner_category'] = canon(CEN.categorise(next((n for n in notices if n.startswith('GAZETTE NOTICE NO. %s\n' % own)), ''))) if own else ''
            run['kind'] = table_kind(ocr_pages[run['first_page'] - 1], first['row_texts'])
            # continuation rows: which notice holds each one after cleaning
            holders, missing = Counter(), 0
            for p in range(run['first_page'] + 1, run['last_page'] + 1):
                lay = page_info[p]['layout']
                cut = min((t for _, t in lay['hdrs']), default=10 ** 9)
                for rc in page_info[p]['table']['row_cells']:
                    if rc[0][0][2] >= cut: continue          # below a new notice: not this table
                    key = re.sub(r'[^a-z0-9]', '', ' '.join(w[3] for c in rc[:2] for w in c).lower())
                    if len(key) < 8 or not note_sq: continue
                    hit = own if own in note_sq and key in note_sq[own] else \
                        next((k for k, v in note_sq.items() if key in v), None)
                    if hit is None: missing += 1
                    else: holders[hit] += 1
            # On two-column pages the owning header can sit in the other column,
            # so "last header above the table" is only a guess: prefer the notice
            # that actually holds most of the table's rows.
            if holders and holders.most_common(1)[0][1] > holders.get(own, 0):
                run['owner_by_header'] = own or ''
                own = holders.most_common(1)[0][0]
                run['owner_notice'] = own
                run['owner_category'] = canon(CEN.categorise(next((n for n in notices if n.startswith('GAZETTE NOTICE NO. %s\n' % own)), '')))
            else:
                run['owner_by_header'] = own or ''
            in_owner = holders.get(own, 0)
            run['cont_rows_in_owner'], run['cont_rows_in_other_notice'], run['cont_rows_not_found'] = \
                in_owner, sum(holders.values()) - in_owner, missing
            multi_tables.append(run)

        for p, txt in enumerate(ocr_pages, 1):
            tp = page_info[p]['table']
            if not tp: continue
            ok = tot = 0
            for rc in tp['row_cells']:
                a_, b_ = row_adjacency(rc, bigrams); ok += a_; tot += b_
            hdrs = sorted(set(int(x.translate(OC.OCR_DIGITS)) for x in OC.HDR_OCR.findall(txt)
                              if x.translate(OC.OCR_DIGITS).isdigit()))
            table_rows.append({'slug': slug, 'page': p, 'kind': table_kind(txt, tp['row_texts']),
                               'rows': tp['rows'], 'cols': tp['cols'],
                               'cell_boundaries': tot, 'boundaries_kept': ok,
                               'intact_pct': round(100 * ok / tot) if tot else 0,
                               'notices_on_page': ' '.join(map(str, hdrs[:6])),
                               'pipeline_flagged': 'yes' if set(map(str, hdrs)) & set(flagged) else 'no',
                               'example_row': tp['row_texts'][len(tp['row_texts']) // 2][:90]})

        gaz_rows.append({'slug': slug, 'issue': s['issue'], 'producer': s['producer'][:28], 'kind': s['kind'],
                         'issue_type': None, 'pages': s['pages'], 'lane': lane, 'notices': len(notices),
                         'source': source, 'lock_candidates': cands, 'lock_demoted': demoted,
                         'categories': ' '.join('%s:%d' % kv for kv in cats_here.most_common()),
                         'template_eligible': tmpl_n, 'template_ok': tmpl_ok,
                         'template_pct': round(100 * tmpl_ok / tmpl_n, 1) if tmpl_n else '',
                         'content_in_notices': vis['content_inside_notices'],
                         'words_intact': vis['words_intact_after_clean'], 'tables': 0,
                         '_ptt_head': ptt[:2500]})
        if a.progress:
            print('%d notices, %.0fs' % (len(notices), time.time() - t_g), flush=True)

    # issue types (needs county signal per gazette)
    county_by_slug = defaultdict(list)
    for r in notice_rows:
        county_by_slug[r['slug']].append('county' if r['county_signal'] else r['python_cat'])
    tcount = Counter(t['slug'] for t in table_rows)
    for g in gaz_rows:
        g['issue_type'] = issue_type(g.pop('_ptt_head'), g['kind'], county_by_slug[g['slug']])
        g['tables'] = tcount[g['slug']]

    write_csv(os.path.join(out, 'gazettes.csv'), gaz_rows)
    write_csv(os.path.join(out, 'notices.csv'), notice_rows)
    write_csv(os.path.join(out, 'visibility.csv'), vis_rows)
    write_csv(os.path.join(out, 'tables.csv'), table_rows)

    itype = {g['slug']: g['issue_type'] for g in gaz_rows}
    report_failures(out, fail_ex, fail_blocks)
    report_schema_fill(out, schema_fields, fill, fill_n)
    kw = report_keywords(out, all_notices, notice_rows)
    report_misc(out, notice_rows)
    report_structure(out, notice_rows)
    report_scenarios(out, scen)
    report_tables(out, table_rows)
    write_csv(os.path.join(out, 'tables_multipage.csv'), multi_tables)
    report_multipage(out, multi_tables)
    report_samples(out, y, samples, itype)
    print_summary(y, gaz_rows, notice_rows, vis_rows, table_rows, fail_ex, kw, scen, out)


def write_csv(path, rows):
    if not rows:
        open(path, 'w').write(''); return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, list(rows[0])); w.writeheader(); w.writerows(rows)


def report_failures(out, fail_ex, fail_blocks):
    with open(os.path.join(out, 'failures.txt'), 'w', encoding='utf-8') as f:
        f.write('Template rejections grouped by cause, largest first (these go to the AI path)\n'
                'format: grammar piece that failed | most likely text-level cause\n\n== per notice\n')
        for (cat, why), ex in sorted(fail_ex.items(), key=lambda kv: -len(kv[1])):
            f.write('%5d  %-14s %s\n       e.g. %s\n' % (len(ex), canon(cat), why, ', '.join(ex[:8])))
        f.write('\n== probate, per CAUSE NO. block (one notice holds many blocks)\n')
        for why, c in fail_blocks.most_common():
            f.write('%5d  %s\n' % (c, why))


def report_schema_fill(out, fields, fill, n):
    with open(os.path.join(out, 'schema_fill.txt'), 'w', encoding='utf-8') as f:
        f.write('How often each schema field is filled by the template, over records it extracted.\n'
                'A field at 0% is in the schema but the template never emits it.\n\n')
        for k in fields:
            f.write('== %s  (%d records)\n' % (k, n[k]))
            for fld in fields[k]:
                pct = 100.0 * fill[k][fld] / n[k] if n[k] else 0
                f.write('  %-26s %5.1f%%%s\n' % (fld, pct, '   <- never emitted' if n[k] and not fill[k][fld] else ''))
            f.write('\n')


def report_keywords(out, all_notices, rows):
    known = [(sq, canon(t)) for _, _, _, t, sq in all_notices if t]
    res = {'known': len(known), 'total': len(all_notices), 'low': []}
    lines = ['notices %d | with heading-based ground truth %d (%.1f%%)\n' % (
        len(all_notices), len(known), 100.0 * len(known) / max(1, len(all_notices)))]

    def score(cat, k):
        fired = [t for sq, t in known if k in sq]
        if not fired: return 0, None
        return len(fired), 100.0 * sum(1 for t in fired if t == cat) / len(fired)

    lines.append('== Java keywordPreFilter keys (production)')
    for cat, keys in JAVA_RULES:
        for k in keys:
            n, p = score(cat, k)
            flag = '' if p is None or p >= 95 else '   <-- BELOW 95%'
            if flag: res['low'].append('%s/%s %.1f%%' % (cat, k, p))
            lines.append('  %-22s %-28s fires %4d  precision %s%s' % (cat, k, n, '-' if p is None else '%.1f%%' % p, flag))
    lines.append('\n== keyword_probability.py candidates')
    for cat, keys in KP.CANDIDATES.items():
        for k in keys:
            n, p = score(canon(cat), k)
            lines.append('  %-22s %-28s fires %4d  precision %s' % (canon(cat), k, n, '-' if p is None else '%.1f%%' % p))

    # production coverage / precision
    jv = Counter(r['java_cat'] for r in rows)
    agree = [r for r in rows if r['truth'] and r['java_cat'] != '(AI triage)']
    prec = 100.0 * sum(1 for r in agree if r['java_cat'] == r['truth']) / max(1, len(agree))
    cov = 100.0 * sum(v for k, v in jv.items() if k != '(AI triage)') / max(1, len(rows))
    res['java_cov'], res['java_prec'] = cov, prec
    lines.append('\n== Java pre-filter overall: coverage %.1f%% of notices, precision %.1f%% (on %d with truth)'
                 % (cov, prec, len(agree)))
    lines.append('   assigned: ' + ', '.join('%s %d' % kv for kv in jv.most_common()))
    miss = Counter((r['truth'], r['java_cat']) for r in agree if r['java_cat'] != r['truth'])
    for (t, j), c in miss.most_common(8):
        lines.append('   truth %-20s -> java %-20s %d' % (t, j, c))

    # coverage per truth category, and proposals (word bi/tri-grams, squashed)
    by_truth = defaultdict(list)
    for slug, num, n, t, sq in all_notices:
        if t: by_truth[canon(t)].append(n)
    lines.append('\n== pre-filter recall per ground-truth category, and proposed new keys (precision >= 95%, >= 5 notices)')
    doc_ngrams = []
    for slug, num, n, t, sq in all_notices:
        w = re.findall(r"[a-z]+", n.lower())
        g = set()
        for k in (2, 3):
            for i in range(len(w) - k + 1):
                g.add(''.join(w[i:i + k]))
        doc_ngrams.append((g, canon(t) if t else None))
    df, dfc = Counter(), defaultdict(Counter)
    for g, t in doc_ngrams:
        if not t: continue
        for x in g:
            df[x] += 1; dfc[x][t] += 1
    for cat, ns in sorted(by_truth.items(), key=lambda kv: -len(kv[1])):
        caught = sum(1 for r in rows if r['truth'] == cat and r['java_cat'] == cat)
        rec = 100.0 * caught / len(ns)
        lines.append('\n  %-22s truth %4d  pre-filter recall %5.1f%%' % (cat, len(ns), rec))
        if rec >= 95: continue
        cands = [(dfc[x][cat], x, 100.0 * dfc[x][cat] / df[x]) for x in df
                 if dfc[x][cat] >= 5 and 100.0 * dfc[x][cat] / df[x] >= 95 and len(x) >= 10]
        missed = [r for r in rows if r['truth'] == cat and r['java_cat'] != cat]
        for sup, x, p in sorted(cands, reverse=True)[:6]:
            lines.append('     propose %-34s precision %5.1f%%  fires on %d of this category' % (x, p, sup))
        if missed:
            lines.append('     missed e.g. ' + ', '.join('%s/%s' % (r['slug'], r['notice']) for r in missed[:6]))
    open(os.path.join(out, 'keywords.txt'), 'w', encoding='utf-8').write('\n'.join(lines) + '\n')
    return res


def report_misc(out, rows):
    misc = [r for r in rows if r['python_cat'] == 'Miscellaneous' or r['java_cat'] == '(AI triage)']
    cl = defaultdict(list)
    for r in misc:
        cl[(r['act'] or '(no act line)', r['subject'] or '(no subject line)')].append(r)
    by_act = Counter(r['act'] or '(no act line)' for r in misc)
    with open(os.path.join(out, 'misc_clusters.txt'), 'w', encoding='utf-8') as f:
        f.write('Notices with no python category (Miscellaneous) or no Java pre-filter hit (sent to AI triage): %d\n\n' % len(misc))
        f.write('== by governing Act\n')
        for act, c in by_act.most_common(40):
            f.write('%5d  %s\n' % (c, act))
        f.write('\n== by Act + subject line\n')
        for (act, subj), rs in sorted(cl.items(), key=lambda kv: -len(kv[1]))[:80]:
            cats = Counter(r['python_cat'] for r in rs).most_common(2)
            f.write('%5d  %s | %s   [python: %s]\n       e.g. %s\n' % (
                len(rs), act[:70], subj[:70], ', '.join('%s %d' % c for c in cats),
                ', '.join('%s/%s' % (r['slug'], r['notice']) for r in rs[:6])))


def report_structure(out, rows):
    sh = defaultdict(list)
    for r in rows:
        for s in r['shapes'].split():
            sh[(s, r['python_cat'])].append(r)
    with open(os.path.join(out, 'structure.txt'), 'w', encoding='utf-8') as f:
        f.write('Compound / nested notice shapes, by category.\n'
                'multi-cause: several CAUSE NO. blocks in one notice (one record each)\n'
                'multi-parcel: several WHEREAS clauses or "title Nos." in one land notice\n'
                'xref-*: notice cites another Gazette Notice (revokes / amends / extends / cites)\n'
                'multi-signature: more than one "Dated the" (possible merged notices)\n'
                'demoted-header-inside: the lock demoted a header inside this notice (merged)\n'
                'multi-act, schedule, numbered-list, number-dense: embedded structure / tables\n\n')
        for (s, cat), rs in sorted(sh.items(), key=lambda kv: (kv[0][0], -len(kv[1]))):
            extra = ''
            if s == 'multi-cause':
                extra = '  (cause blocks: max %d, total %d)' % (max(r['cause_blocks'] for r in rs), sum(r['cause_blocks'] for r in rs))
            f.write('%-24s %-22s %5d%s\n     e.g. %s\n' % (s, cat, len(rs), extra,
                    ', '.join('%s/%s' % (r['slug'], r['notice']) for r in rs[:6])))


def report_scenarios(out, scen):
    with open(os.path.join(out, 'scenarios.txt'), 'w', encoding='utf-8') as f:
        for k, v in sorted(scen.items(), key=lambda kv: -len(kv[1])):
            f.write('%5d  %s\n       e.g. %s\n' % (len(v), k, '; '.join(v[:6])))


def report_tables(out, rows):
    with open(os.path.join(out, 'tables.txt'), 'w', encoding='utf-8') as f:
        f.write('Table pages detected from Tesseract word boxes (>= 8 rows of >= 3 cells with a number)\n')
        f.write('intact = share of cell boundaries (last word of a cell, first word of the next) that are\n'
                '         still adjacent in the cleaned text. Low = columns interleaved / rows scrambled.\n\n')
        by = defaultdict(list)
        for r in rows: by[r['kind']].append(r)
        for k, rs in sorted(by.items(), key=lambda kv: -len(kv[1])):
            f.write('== %s: %d pages in %d gazettes, rows %d, median cols %d, cell boundaries kept %.0f%%, pipeline-flagged %d\n' % (
                k, len(rs), len(set(r['slug'] for r in rs)), sum(r['rows'] for r in rs),
                sorted(r['cols'] for r in rs)[len(rs) // 2],
                100.0 * sum(r['boundaries_kept'] for r in rs) / max(1, sum(r['cell_boundaries'] for r in rs)),
                sum(1 for r in rs if r['pipeline_flagged'] == 'yes')))
            for r in rs[:8]:
                f.write('   %s p%d  rows %d cols %d intact %d%%  notices %s\n      row: %s\n' % (
                    r['slug'], r['page'], r['rows'], r['cols'], r['intact_pct'], r['notices_on_page'] or '-', r['example_row']))


def report_multipage(out, runs):
    with open(os.path.join(out, 'tables_multipage.txt'), 'w', encoding='utf-8') as f:
        f.write('Tables that continue across a page break (same table, no new notice header in between)\n'
                'owner = notice whose header precedes the table; continuation rows = rows on pages 2..n,\n'
                'checked against the cleaned notice text (first two cells, spaces ignored)\n\n')
        tot = Counter()
        for r in runs:
            for k in ('cont_rows_in_owner', 'cont_rows_in_other_notice', 'cont_rows_not_found'):
                tot[k] += r[k]
        n = sum(tot.values())
        f.write('%d multi-page tables, %d pages, %d rows; continuation rows: in owner notice %d, in another notice %d, '
                'not found %d (of %d checked)\n' % (len(runs), sum(r['n_pages'] for r in runs), sum(r['rows'] for r in runs),
                tot['cont_rows_in_owner'], tot['cont_rows_in_other_notice'], tot['cont_rows_not_found'], n))
        f.write('with a caption / header row repeated on continuation pages: %d; next notice starts mid-page after '
                'the table: %d\n\n' % (sum(1 for r in runs if r['repeated_captions']),
                                       sum(1 for r in runs if r['next_notice_starts_mid_page'] != '')))
        for r in sorted(runs, key=lambda r: -r['n_pages']):
            f.write('%s p%d-%d (%d pages) owner %s [%s] %s, rows %d, cols %s | cont rows owner/other/missing %d/%d/%d%s%s\n' % (
                r['slug'], r['first_page'], r['last_page'], r['n_pages'], r['owner_notice'] or '-', r['owner_category'],
                r['kind'], r['rows'], r['cols_per_page'], r['cont_rows_in_owner'], r['cont_rows_in_other_notice'],
                r['cont_rows_not_found'],
                ('\n     repeated: ' + r['repeated_captions'][:150]) if r['repeated_captions'] else '',
                ('\n     next notice %s starts on the last page, below the table' % r['next_notice_starts_mid_page'])
                if r['next_notice_starts_mid_page'] != '' else ''))


def report_samples(out, y, samples, itype):
    raw_cache = {}
    with open(os.path.join(out, 'samples.md'), 'w', encoding='utf-8') as f:
        f.write('# Before / after samples, %s\n\nBefore = pdf-inspector raw text; after = cleaned notice content.\n' % y)
        for t in ('main', 'special', 'county', 'scanned', 'other'):
            picked, seen_cat = [], set()
            for slug, lst in samples.items():
                if itype.get(slug) != t: continue
                for num, cat, n in lst:
                    if cat not in seen_cat and len(n.split()) > 30:
                        picked.append((slug, num, cat, n)); seen_cat.add(cat)
                    if len(picked) >= 3: break
                if len(picked) >= 3: break
            if not picked: continue
            f.write('\n## %s issues\n' % t)
            for slug, num, cat, n in picked:
                if slug not in raw_cache:
                    raw_cache[slug] = read_text(os.path.join(REPO, 'raw', y, slug + '.inspector.txt'))
                before = raw_slice(raw_cache[slug], num) or '(not locatable in raw - scan / OCR lane)'
                f.write('\n### %s notice %d (%s)\n\n**before**\n```\n%s\n```\n**after**\n```\n%s\n```\n' % (
                    slug, num, canon(cat), before[:700], n[:700]))


def print_summary(y, gz, rows, vis, tables, fail_ex, kw, scen, out):
    N = len(rows)
    print('== %s corpus report: %d gazettes, %d notices' % (y, len(gz), N))
    print('issue types: ' + ', '.join('%s %d' % kv for kv in Counter(g['issue_type'] for g in gz).most_common()))
    print('python categories: ' + ', '.join('%s %d' % kv for kv in Counter(r['python_cat'] for r in rows).most_common()))
    elig = sum(g['template_eligible'] for g in gz); ok = sum(g['template_ok'] for g in gz)
    print('template coverage: %d / %d eligible (%.1f%%); failures in %d causes' % (ok, elig, 100.0 * ok / max(1, elig), len(fail_ex)))
    print('  (below: pipeline text only; OCR-lane notices from scans excluded)')
    for cat in ('court_legal', 'land_property', 'Corrigenda'):
        e = [r for r in rows if r['python_cat'] == canon(cat) and r['source'] == 'inspector']
        o = sum(1 for r in e if r['path'] == 'template')
        if e: print('   %-14s %4d / %4d  %.1f%% of notices' % (canon(cat), o, len(e), 100.0 * o / len(e)))
    bl = [r for r in rows if r['cause_blocks_total'] != '' and r['source'] == 'inspector']
    bt, bo = sum(r['cause_blocks_total'] for r in bl), sum(r['cause_blocks_ok'] for r in bl)
    print('   probate blocks %d / %d  %.1f%% of CAUSE NO. blocks (call-level)' % (bo, bt, 100.0 * bo / max(1, bt)))
    print('java pre-filter: coverage %.1f%%, precision %.1f%%; keys below 95%%: %s' % (
        kw['java_cov'], kw['java_prec'], ', '.join(kw['low']) or 'none'))
    bi = [v for v in vis if v['kind'] == 'born-digital']
    if bi:
        print('visibility (born-digital, share of printed words): text layer %.3f | content in cleaned %.3f | '
              'content inside notices %.3f | words intact after cleaning %.3f' % tuple(
            sum(v[k] for v in bi) / len(bi) for k in ('text_layer', 'content_in_cleaned', 'content_inside_notices',
                                                        'words_intact_after_clean')))
    sc = [v for v in vis if v['kind'] != 'born-digital']
    for v in sc:
        print('   %s (%s): pipeline sees %d notices; OCR shows %d printed' % (
            v['slug'], v['kind'], v['notices_seen_by_pipeline'], v['notices_printed_ocr']))
    print('tables: %d pages in %d gazettes; kinds: %s' % (len(tables), len(set(t['slug'] for t in tables)),
          ', '.join('%s %d' % kv for kv in Counter(t['kind'] for t in tables).most_common())))
    print('scenarios: ' + '; '.join('%s (%d)' % (k[:50], len(v)) for k, v in sorted(scen.items(), key=lambda kv: -len(kv[1]))[:8]))
    print('reports: %s' % os.path.relpath(out, REPO))


if __name__ == '__main__':
    main()
