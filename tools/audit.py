"""
Standing audit for the Smart Gazette rule-based extraction layer.

Run after ANY change to a template or to the cleaner. It checks three classes
of defect, each of which has already cost real coverage during development:

  1. OPTIONAL-WORD typo   - `the?` means "th" + optional "e", not optional
                            "the". This one silently cost 27 points of land
                            coverage before it was found.
  2. RIGID WHITESPACE     - the fragment-joiner sometimes glues or splits
                            words, so a literal multi-word phrase matched with
                            `\\s+` will miss "THE LANDREGISTRATION ACT". This
                            silently misclassified 13 land notices.
  3. SCHEMA DRIFT         - a template emitting field names that do not match
                            its schema cannot drop into the pipeline. The land
                            template once had zero overlap with its schema.

Plus a behavioural suite: every optional construct must match both with and
without its optional part.
"""
import re, os, sys, json, glob, subprocess, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))

SUSPECT_OPTIONAL = re.compile(r"(?<![\)\]\\])\b([A-Za-z]{2,})\?")
FUNCTION_WORDS = {'the','of','and','for','in','to','at','as','is','are','that','this','a','an','with','by'}
RIGID_PHRASE = re.compile(r"[A-Z]{3,}\\s\+[A-Z]{3,}")

# template module -> schema file it must match
SCHEMA_MAP = {
    'probate_template':    'court_legal',
    'land_template':       'land_property',
}

def load(name):
    s = importlib.util.spec_from_file_location(name, os.path.join(HERE, name + '.py'))
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m

def check_optional_words(path):
    out = []
    for i, line in enumerate(open(path, encoding='utf-8').read().split('\n'), 1):
        if "re.compile" not in line and not line.strip().startswith("r'") and "tol(" not in line:
            continue
        for m in SUSPECT_OPTIONAL.finditer(line):
            w = m.group(1)
            sev = 'ERROR' if w.lower() in FUNCTION_WORDS else 'info'
            if sev == 'ERROR':
                out.append((i, sev, "'%s?' matches '%s'+optional '%s' - use '(?:%s)?'"
                            % (w, w[:-1], w[-1], w)))
    return out

def check_rigid_whitespace(path):
    out = []
    for i, line in enumerate(open(path, encoding='utf-8').read().split('\n'), 1):
        if 're.compile' not in line and not line.strip().startswith("r'"):
            continue
        for m in RIGID_PHRASE.finditer(line):
            out.append((i, 'warn', "rigid `\\s+` between literal words (%s) - joiner may glue them; prefer \\s*"
                        % m.group(0)[:28]))
    return out

def check_schema_alignment(sample_outputs):
    out = []
    for mod, schema in SCHEMA_MAP.items():
        p = os.path.join(HERE, 'schemas', schema + '.json')
        if not os.path.exists(p):
            out.append(('%s' % mod, 'skip', 'schema %s.json not found' % schema)); continue
        want = set(json.load(open(p))['definitions']['item']['properties'].keys())
        got = sample_outputs.get(mod)
        if got is None:
            out.append((mod, 'skip', 'no sample output produced')); continue
        extra, missing = set(got) - want, want - set(got)
        if extra:   out.append((mod, 'ERROR', 'emits fields not in %s: %s' % (schema, sorted(extra))))
        if missing: out.append((mod, 'warn',  'schema fields never emitted: %s' % sorted(missing)))
        if not extra and not missing:
            out.append((mod, 'ok', 'matches %s exactly (%d fields)' % (schema, len(want))))
    return out

def check_generation(samples):
    """Every extracted record must render, and the rendered output must be
    complete, correctly shaped, and free of the formatting defects that have
    shown up in practice (glued sentences, uppercase courts, clipped fields)."""
    G = load('generate')
    REQUIRED = {'title', 'summary', 'article', 'xSummary', 'actionableInfo', 'significance'}
    out = []
    for mod, cat in (('probate_template', 'court_legal'), ('land_template', 'land_property')):
        rec = samples.get(mod)
        if rec is None:
            out.append((cat, 'skip', 'no sample record')); continue
        art = G.generate(cat, rec)
        if art is None:
            out.append((cat, 'ERROR', 'extracted record did not render')); continue
        missing = REQUIRED - set(art)
        if missing:
            out.append((cat, 'ERROR', 'missing output keys: %s' % sorted(missing))); continue
        probs = []
        # xSummary is only populated when significance clears the posting
        # threshold, so an empty one is correct for routine notices.
        if art['xSummary'] is not None and len(art['xSummary']) > 240:
            probs.append('xSummary over 240 chars')
        if art['xSummary'] is not None and art['significance'] < 8:
            probs.append('xSummary populated below the posting threshold')
        if art['article'].count('\n\n') != 2:      probs.append('article is not 3 paragraphs')
        if re.search(r'[a-z]\.[A-Z]', art['article']): probs.append('missing space after a full stop')
        if re.search(r'\b(OF|AT|THE)\b', art['title']): probs.append('uppercase small word in title')
        if re.search(r'\s-\s(?:in|law)\b', art['article']): probs.append('spaced hyphen in compound')
        if not isinstance(art['significance'], int):  probs.append('significance is not an int')
        out.append((cat, 'ERROR' if probs else 'ok', '; '.join(probs) or 'renders cleanly, 3 paragraphs, all keys'))
    return out

def check_key_fields(samples):
    """Names and parcel ids carry the record. A wrong name makes a notice
    unsearchable; a wrong parcel id makes it unfindable by the one person who
    needs it. Both are checked across the corpus, not just on a sample."""
    import glob as _g
    N  = load('names'); L = load('land_template'); P = load('probate_template')
    GC = load('gazette_clean')
    names, parcels = [], []
    for f in sorted(_g.glob('/mnt/user-data/uploads/raw*.txt'))[:6]:
        d = open(f, 'rb').read()
        # UTF-16 only with a BOM; utf-16-first silently garbles UTF-8 input
        raw = d.decode('utf-16') if d[:2] in (b'\xff\xfe', b'\xfe\xff') else d.decode('utf-8', errors='replace')
        c = GC.apply_ascending_lock(GC.clean(raw))
        for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', c):
            if not n.startswith('GAZETTE'): continue
            r = L.extract(n)
            if r:
                names += r.get('parties') or []
                parcels.append(r.get('parcel_id'))
            for b in [x for x in re.split(r'(?=CAUSE NO\.)', n) if x.startswith('CAUSE NO.')]:
                pr = P.extract(b, n)
                if pr:
                    names += pr.get('petitioner_names') or []
                    if pr.get('deceased_name'): names.append(pr['deceased_name'])
    out = []
    if names:
        clean = sum(1 for x in names if not N.looks_suspect(x))
        pct = 100.0 * clean / len(names)
        out.append(('names', 'ok' if pct >= 95 else 'ERROR',
                    '%d checked, %.1f%% clean (threshold 95%%)' % (len(names), pct)))
    if parcels:
        bad = sum(1 for p in parcels if not p or len(p) < 4)
        out.append(('parcel_id', 'ok' if bad == 0 else 'ERROR',
                    '%d checked, %d unusable as a key' % (len(parcels), bad)))
    return out

def check_reference_freshness():
    """Reference watch (docs/specs/reference-watch.md): the watchers still see
    the signs they exist for (ERROR if not), and the law copies due for a check
    (WARN). The full corpus run: python tools/reference_watch.py"""
    sys.path.insert(0, HERE)
    import reference_watch as W
    out = []
    cases = [
        ('a newer amendment Act is flagged', 'the Water (Amendment) Act, 2099 amends section 72 of the Water Act',
         lambda f: any(x['watcher'] == 'law.amended_after_copy' and x['key'] == 'water_act' for x in f)),
        ('an amendment already in our copy is not', 'the Urban Areas and Cities (Amendment) Act, 2019',
         lambda f: not f),
        ('a section our copy lacks is flagged', 'pursuant to section 999 of the Land Act, 2012',
         lambda f: any(x['watcher'] == 'law.missing_section' and x['detail'] == 's.999' for x in f)),
        ('an amending supplement is flagged', 'KENYA GAZETTE SUPPLEMENT\nAN ACT of Parliament to amend the Land Act',
         lambda f: any(x['watcher'] == 'law.amending_supplement' and x['key'] == 'land_act' for x in f)),
    ]
    for label, text, ok in cases:
        out.append(('watch: ' + label, 'ok' if ok(W.flags(text)) else 'ERROR'))
    for f in W.old_copies():
        out.append(('law copy due for a check: %s (text as at %s)' % (f['key'], f['detail']), 'WARN'))
    return out


def check_escapes():
    """Escapes lost on the way into a source file: in Java "\\s" is a regex escape
    only when written "\\\\s" - a single "\\s" compiles silently to a space (Java 15+);
    a backspace byte in a .py / .js / .java file is a lost "\\b". Both happened
    (LawReferenceService, 4 Oct 2026)."""
    bs = chr(92)
    REPO = os.path.dirname(HERE)
    out = []
    java = glob.glob(os.path.join(REPO, 'src', '**', '*.java'), recursive=True)
    for f in java:
        s = open(f, encoding='utf-8').read()
        for m in re.finditer(r'"(?:[^"\\\n]|\\.)*"', s):
            lit, i = m.group(0), 0
            while i < len(lit):
                if lit[i] == bs:
                    if i + 1 < len(lit) and lit[i + 1] in 'sdwbSDWB':
                        out.append(('%s:%d single-backslash regex escape %s' % (os.path.relpath(f, REPO),
                                    s[:m.start()].count('\n') + 1, lit[:60]), 'ERROR'))
                    i += 2
                else:
                    i += 1
    for f in java + glob.glob(os.path.join(HERE, '*.py')) + glob.glob(os.path.join(HERE, '*.js')):
        if chr(8) in open(f, encoding='utf-8', errors='replace').read():
            out.append(('%s contains a backspace character (a lost \\b)' % os.path.relpath(f, REPO), 'ERROR'))
    if not out:
        out.append(('java regex escapes and backspace bytes: %d source files clean' % len(java), 'ok'))
    return out


def behavioural():
    P, L, C = load('probate_template'), load('land_template'), load('corrigenda_template')
    cases = [
      ('land: "under title No." (no "the")', L.RE_LR2, 'registered under title No. Kakamega/Chemuche/509, by virtue of a'),
      ('land: "under the title No."',        L.RE_LR2, 'registered under the title No. Juja Block 17/223, and whereas'),
      ('land: LAND TITLE act',               L.RE_ACT, 'THE LAND TITLE ACT'),
      ('land: LAND TITLES act',              L.RE_ACT, 'THE LAND TITLES ACT'),
      ('land: hectare / hectares / acres',   L.RE_AREA,'containing 0.09 hectare or thereabouts'),
      ('probate: advocate singular',         P.RE_ADVOCATE, 'through Messrs. X & Co, advocate,'),
      ('probate: advocates plural',          P.RE_ADVOCATE, 'through Messrs. X & Co, advocates,'),
      ('probate: bare "of" address',         P.RE_ADDRESS,  'of P.O. Box 123, Nairobi'),
      ('probate: "all of" address',          P.RE_ADDRESS,  'all of P.O. Box 123, Nairobi'),
      ('corrigenda: amend clause',           C.RE_AMEND,    'amend the name printed as "A" to read "B"'),
    ]
    out = [(lbl, 'ok' if rx.search(s) else 'ERROR', '') for lbl, rx, s in cases]

    # cleaner (fix 3) and vocabulary repair (fix 2) regression cases - each one
    # is a failure seen in the corpus (journal sections 12.1 / 12.2)
    G = load('gazette_clean'); VR = load('vocab_repair')
    clean_cases = [
        ('clean: header + title on one line (2026/90/7653)', 'x\nGAZETTE NOTICE NO. 7653 THE PUBLIC HOLIDAYS ACT\n(Cap. 110)', 'GAZETTE NOTICE NO. 7653\n'),
        ('clean: uppercase cross-ref rejected (6865 OF 2017)', 'x\nGAZETTE NOTICE NO. 6865 OF 2017 DETERMINATION', None),
        ('clean: NUL after header (2023/26/1150)', 'x\nGAZETTE NOTICE NO. 1150\n\x00THE LAND REGISTRATION ACT', 'GAZETTE NOTICE NO. 1150\n'),
        ('clean: date of death kept', 'who died at Kerugoya, on\n28th June, 2021\n.', '28th June, 2021'),
        ('clean: curly apostrophe -> ASCII', 'the deceased’s widow', "deceased's"),
    ]
    for lbl, s, want in clean_cases:
        got = G.clean(s)
        ok = (want in got) if want else ('GAZETTE NOTICE NO. 6865\n' not in got)
        out.append((lbl, 'ok' if ok else 'ERROR', ''))
    V = VR.load_corpus_vocab()
    repair_cases = [
        ('letters of a dministration intestate', 'letters of administration intestate'),
        ('a pplication shaving been made', 'applications having been made'),
        ('on 7 thJanuary, 20 21.', 'on 7th January, 2021.'),
        ('land knownas L.R. No. 2909', 'land known as L.R. No. 2909'),
        ('STANDING ORDERSS PECIALS ITTINGO FTHE COUNTY', 'STANDING ORDERS SPECIAL SITTING OF THE COUNTY'),
        ('THE LAND REGISTRAT ION ACT', 'THE LAND REGISTRATION ACT'),
        ('who died at Omogonchor on', 'who died at Omogonchor on'),
        ('died at Onywere, Kisii', 'died at Onywere, Kisii'),
        ('died at CityHospital in Kenya, on', 'died at City Hospital in Kenya, on'),
        ('John Doe alias JohnDoe', 'John Doe alias John Doe'),
        ('By John Doe Roe and Jane', 'By John Doe Roe and Jane'),
        ('Ronald McDonald', 'Ronald McDonald'),
        ('in Kenya', 'in Kenya'),
    ]
    for s, want in repair_cases:
        out.append(('repair: %r' % s, 'ok' if VR.repair(s, V) == want else 'ERROR', ''))

    # fix 4 template regression cases (journal section 13)
    CEN = load('category_census')
    P4 = lambda body: P.extract('CAUSE NO. E1 OF 2023 By Jane Doe Roe, the deceased\'s widow, ' + body)
    probate_cases = [
        ('probate: year-only death date', P4('for a grant of letters of administration intestate to the estate of John Roe, who died at Maragua in 1978.\n'),
         lambda r: r and r['date_of_death'] == '1978'),
        ('probate: no death date in source', P4('for a grant of letters of administration intestate to the estate of John Roe, who died at Shirere.\n'),
         lambda r: r and r['date_of_death'] is None and r['deceased_name'] == 'John Roe'),
        ('probate: "of written will" without "the"', P4('for a grant of probate of written will of John Roe, who died on 2nd July, 2019.\n'),
         lambda r: r and r['deceased_name'] == 'John Roe'),
        ('probate: "for grant" / "estate X"', P4('for grant of letters of administration intestate to the estate John Roe, who died on 2nd July, 2019.\n'),
         lambda r: r and r['deceased_name'] == 'John Roe'),
        ('probate: firm without "advocates"', P4('through Messrs. Magee of Nairobi, for a grant of letters of administration intestate to the estate of John Roe, who died on 2nd July, 2019.\n'),
         lambda r: r and r['advocate_firm'] == 'Magee'),
    ]
    for lbl, r, ok in probate_cases:
        out.append((lbl, 'ok' if ok(r) else 'ERROR', ''))
    out.append(('probate: "(Formerly CAUSE NO. ...)" not split',
                'ok' if len(P.split_causes('CAUSE NO. 16 OF 2022 (Formerly CAUSE NO. 99 OF 2019) By E F, for a grant')) == 1 else 'ERROR', ''))
    LH = 'GAZETTE NOTICE NO. 1\nTHE LAND REGISTRATION ACT (No. 3 of 2012) ISSUE OF A NEW LAND TITLE DEED WHEREAS John Doe is registered as proprietor in absolute ownership interest of '
    land_cases = [
        ('land: "of that piece" (no "all")', LH + 'that piece of land situate in the district of Ruiru, registered under title No. Ruiru/Ruiru East Block 4/301, and whereas the land title deed has been lost', 'Ruiru/Ruiru East Block 4/301'),
        ('land: parcel with initials', LH + 'all that piece of land situate in the district of Bungoma, registered under title No. E. Bukusu/N.\nSangalo/3026, and whereas the land title deed has been lost', 'E. Bukusu/N. Sangalo/3026'),
    ]
    for lbl, s, want in land_cases:
        r = L.extract(s)
        out.append((lbl, 'ok' if r and r['parcel_id'] == want else 'ERROR', ''))
    out.append(('route: land notice citing a court stays land',
                'ok' if CEN.categorise('GAZETTE NOTICE NO. 1\nTHE LAND REGISTRATION ACT (No. 3 of 2012) REGISTRATION OF INSTRUMENT WHEREAS X, is registered as proprietor, and whereas the Chief Magistrate\'s Court at Eldoret in succession CAUSE NO. 381 of 2020 has issued grant') == 'land_property' else 'ERROR', ''))
    corr = C.extract_notice('GAZETTE NOTICE NO. 15995\nCORRIGENDUM IN Gazzette Notices Nos. 5402 of 2021, amend the acquiring body\'s name printed as "A" to read "B".')
    out.append(('corrigenda: no CAUSE NO., "Gazzette" typo', 'ok' if corr and corr['amends_notice'] == '5402' and corr['amendments'][0]['to_read'] == 'B' else 'ERROR', ''))

    # fix 6 category routing (journal section 16)
    route_cases = [
        ('county: County Governments Act', 'GAZETTE NOTICE NO. 1\nTHE COUNTY GOVERNMENTS ACT\n(No. 17 of 2012)\nCOUNTY ASSEMBLY OF KISUMU\nSTANDING ORDERS\nSPECIAL SITTING', 'County_Government'),
        ('county: a county\'s own Act', 'GAZETTE NOTICE NO. 1\nTHE KIAMBU COUNTY FINANCE ACT, 2023\nPUBLICATION\nIN EXERCISE of the powers', 'County_Government'),
        ('county: assembly standing orders', 'GAZETTE NOTICE NO. 1\nTHE MERU COUNTY ASSEMBLY STANDING ORDERS\nTHIRD ASSEMBLY', 'County_Government'),
        ('county: body mention is not enough', 'GAZETTE NOTICE NO. 1\nTHE STATE CORPORATIONS ACT\nAPPOINTMENT\nIN EXERCISE of the powers, the County Assembly of Nairobi', 'appointments'),
        ('county: planning stays land', 'GAZETTE NOTICE NO. 1\nTHE PHYSICAL AND LAND USE PLANNING ACT\nCOUNTY GOVERNMENT OF ISIOLO\nCOMPLETION OF PART DEVELOPMENT PLAN', 'land_property'),
        ('county: Parliament is not a county', 'GAZETTE NOTICE NO. 1\nTHE SENATE STANDING ORDERS\nSPECIAL SITTING OF THE SENATE', 'miscellaneous'),
        # fix 6 step 2: new categories
        ('elections: Elections Act heading', 'GAZETTE NOTICE NO. 1\nTHE ELECTIONS ACT\n(No. 24 of 2011)\nTHE ELECTIONS (GENERAL) REGULATIONS, 2012\nAPPOINTMENT OF RETURNING OFFICERS', 'Elections'),
        ('elections: party notice', 'GAZETTE NOTICE NO. 1\nTHE POLITICAL PARTIES ACT\nCHANGE OF POLITICAL PARTY OFFICIALS', 'Elections'),
        ('elections: IEBC in an exchequer table is not elections', 'GAZETTE NOTICE NO. 1\nTHE PUBLIC FINANCE MANAGEMENT ACT\nSTATEMENT OF ACTUAL REVENUES AND NET EXCHEQUER ISSUES\nIndependent Electoral and Boundaries Commission 1,000', 'miscellaneous'),
        ('elections: a correction is Corrigenda', 'GAZETTE NOTICE NO. 1\nTHE ELECTIONS ACT\nBY-ELECTIONS\nCORRIGENDA\nIN Gazette Notice No. 15731 of 2025', 'Corrigenda'),
        ('uncollected goods', 'GAZETTE NOTICE NO. 1\nEXAMPLE AUCTIONEERS\nDISPOSAL OF UNCOLLECTED GOODS\nNOTICE is given under the Disposal of Uncollected Goods Act', 'Uncollected_Goods'),
        ('environment: EIA', 'GAZETTE NOTICE NO. 1\nTHE ENVIRONMENTAL MANAGEMENT AND CO-ORDINATION ACT\nNATIONAL ENVIRONMENT MANAGEMENT AUTHORITY\nENVIRONMENTAL IMPACT ASSESSMENT STUDY REPORT', 'Environment'),
        ('tariffs: water tariff', 'GAZETTE NOTICE NO. 1\nTHE WATER ACT\nEXAMPLE WATER AND SEWERAGE COMPANY\nAPPROVED TARIFF STRUCTURE FOR THE PERIOD 2024/2025', 'Utility_Tariffs'),
        ('tariffs: regulator appointment stays Appointments', 'GAZETTE NOTICE NO. 1\nTHE ENERGY ACT\nENERGY AND PETROLEUM REGULATORY AUTHORITY\nAPPOINTMENT\nIN EXERCISE of the powers', 'appointments'),
        # fix 6 step 3: keys removed or moved
        ('keys: "Retirement Benefits" is not HR', 'GAZETTE NOTICE NO. 1\nTHE RETIREMENT BENEFITS ACT\nRETIREMENT BENEFITS AUTHORITY\nAPPOINTMENT\nIN EXERCISE of the powers', 'appointments'),
        ('keys: "Export Promotion" is not HR', 'GAZETTE NOTICE NO. 1\nTHE STATE CORPORATIONS ACT\nKENYA EXPORT PROMOTION AND BRANDING AGENCY\nAPPOINTMENT', 'appointments'),
        ('keys: "amend the" in a body is not Corrigenda', 'GAZETTE NOTICE NO. 1\nTHE STATUTORY INSTRUMENTS ACT\nDRAFT REGULATIONS\nThe operator shall notify the Authority of its intent to amend the safety case.', 'Legislation'),
        ('keys: Elections Act in the body -> Elections', 'GAZETTE NOTICE NO. 1\nMAARA CONSTITUENCY\nNOTICE is given under section 4 of the Elections Act of the polling stations', 'Elections'),
    ]
    for lbl, s, want in route_cases:
        got = CEN.categorise(s)
        out.append((lbl, 'ok' if got == want else 'ERROR', '' if got == want else 'got ' + got))

    # law citations shown beside a notice (journal section 17)
    LR = load('law_refs')
    cite_cases = [
        ('cite: article with sub-article and paragraph', 'IN EXERCISE of the powers conferred by Article 179 (2) (b) of the Constitution of Kenya', ['Article 179(2)']),
        ('cite: list of articles', 'conferred by Articles 88 (4), 97 (1) (a) and 194 of the Constitution', ['Article 88(4)', 'Article 97(1)', 'Article 194']),
        ('cite: section of the County Governments Act', 'conferred by section 30 (2) (f) of the County Governments Act, 2012', ['section 30(2)']),
        ('cite: schedule', 'the Fourth Schedule to the Constitution', ['Fourth Schedule']),
        ('cite: Act we do not hold is listed, not resolved', 'under section 45 of the Land Act', ['section 45']),
        ('cite: an Article of another instrument is not the Constitution', 'Article 5 of the Treaty for the Establishment of the East African Community', []),
    ]
    for lbl, s, want in cite_cases:
        got = [r['label'] for r in LR.refs(s)]
        out.append((lbl, 'ok' if got == want else 'ERROR', '' if got == want else 'got %s' % got))
    # scan lane OCR repair (fix 5, journal section 18)
    S = load('scan_lane')
    V = VR.load_corpus_vocab()
    ocr_cases = [
        ('ocr: header with 0/O and comma', 'GAZETTE N0TICE No, 8O6O\nIN THE HIGH COURT', 'GAZETTE NOTICE NO. 8060\nIN THE HIGH COURT'),
        ('ocr: cross-reference is not a header', 'Gazette Notice No. 123 of 2024 is revoked', 'Gazette Notice No. 123 of 2024 is revoked'),
        ('ocr: cause header "oF" and specks', 'CAUSE NO. E39 0F2025,. + By Oscar', 'CAUSE NO. E39 OF 2025  By Oscar'),
        ('ocr: stray quote, colon and dot', "'By X, for a: grant of probate, who. died", 'By X, for a grant of probate, who died'),
        ('ocr: digits read as ! and S', 'died on ! 5th May and on Sth June', 'died on 15th May and on 5th June'),
        ('ocr: common-word fixes', 'tor a grant of Setters of administration, Keriya', 'for a grant of Letters of administration, Kenya'),
        ('ocr: a name seen in born-digital text is kept', 'Shaw and Mboga and Okelo', 'Shaw and Mboga and Okelo'),
        ("ocr: O'Brien keeps its apostrophe", "O'Brien", "O'Brien"),
    ]
    for lbl, s, want in ocr_cases:
        got = S.ocr_fix(S.fix_headers(s), V)
        out.append((lbl, 'ok' if got == want else 'ERROR', '' if got == want else 'got %r' % got))
    # fix 5 step 2: garbled headers Tesseract did read (2022 No 231)
    for s, want in (('GAZETTR NOTICE NO 13318', 'GAZETTE NOTICE NO. 13318'),
                    ('GAZETTE NOTICENO 13321', 'GAZETTE NOTICE NO. 13321'),
                    ('GAZETTE Nomice No 13342', 'GAZETTE NOTICE NO. 13342'),
                    ('Gazette Notice No. 123 of 2024', None),
                    ('Garissa County 2022', None)):
        got = S.fuzzy_header(s)
        out.append(('ocr: fuzzy header %r' % s, 'ok' if got == want else 'ERROR', '' if got == want else 'got %r' % got))
    # fix 5 step 2: a parcel id ends at the next clause even without its comma
    LH2 = 'GAZETTE NOTICE NO. 1\nTHE LAND REGISTRATION ACT\nISSUE OF A NEW LAND TITLE DEED\nWHEREAS John Doe, of P.O. Box 5, Kisumu, is registered as proprietor of all that piece of land '
    for lbl, s, want in (
            ('land: no comma before "and whereas" (OCR)', LH2 + 'registered under ttle No Kisumu/Karateng/1874 and whereas the land title deed has been lost', 'Kisumu/Karateng/1874'),
            ('land: no comma before "situate"', LH2 + 'known as Mombasa Block XXV/49 situate in Mombasa Municipality, and whereas the land title deed has been lost', 'Mombasa Block XXV/49'),
            ('land: "measuring" ends the parcel', LH2 + 'known as Meru Town Block II/32, measuring 0.0431 hectare, and whereas the land title deed has been lost', 'Meru Town Block II/32')):
        r = L.extract(s)
        got = r and r['parcel_id']
        out.append((lbl, 'ok' if got == want else 'ERROR', '' if got == want else 'got %r' % got))
    # fix 7: table rows (" | " cells from inspect_positions.js) through the cleaner
    tbl = ('GAZETTE NOTICE NO. 1\nTHE LAND ACT\nSCHEDULE\nParcel No. | Registered Owner (s) | Area\n'
           '1. | Kilifi/Mwahera A/794 | 0.4954\n2. | Kilifi/Mwahera A/768 | 0.1619\nTHE KENYA GAZETTE\n'
           'Parcel No. | Registered Owner (s) | Area\n3. | Kilifi/Mwahera A/767 | 1.1810\n'
           'Ward Administrator | Ex-Officio Member\nWard Administrator | Ex-Officio Member\n')
    cl = G.clean(tbl).split('\n')
    out.append(('tables: each row keeps its own line, numbered rows too',
                'ok' if '1. | Kilifi/Mwahera A/794 | 0.4954' in cl and '3. | Kilifi/Mwahera A/767 | 1.1810' in cl else 'ERROR', ''))
    out.append(('tables: header repeated at the top of a page is dropped',
                'ok' if cl.count('Parcel No. | Registered Owner (s) | Area') == 1 else 'ERROR', ''))
    out.append(('tables: repeated data rows without digits are kept',
                'ok' if cl.count('Ward Administrator | Ex-Officio Member') == 2 else 'ERROR', ''))
    # fix 8: tables as data
    TX = load('table_extract')
    ts = TX.tables('GAZETTE NOTICE NO. 1\nSCHEDULE\nParcel No. | Registered Owner (s) | Area\n--- | --- | ---\n'
                   'Location 11/Gaitega/467 | Peter Doe | 0.0085\nLocation 11/Gaitega/355 | the County Council | 0.0511\n'
                   '(Example Coffee Nursery)\nLocation 11/Gaitega/317 | Mary Jane Roe | 0.0274\n')
    ok = (len(ts) == 1 and ts[0]['columns'] == ['Parcel No.', 'Registered Owner (s)', 'Area'] and len(ts[0]['rows']) == 3
          and ts[0]['rows'][1][1] == 'the County Council (Example Coffee Nursery)')
    out.append(('tables: marked header, wrapped cell joins its row', 'ok' if ok else 'ERROR', '' if ok else repr(ts)[:200]))
    ts = TX.tables('GAZETTE NOTICE NO. 2\nSCHEDULE\nKipkelion | Kericho\nSigor | Kapenguria\nLari | Kiambu\n')
    out.append(('tables: a digit-free data row is not a header', 'ok' if ts and ts[0]['columns'] is None and len(ts[0]['rows']) == 3 else 'ERROR', ''))
    ts = TX.tables('GAZETTE NOTICE NO. 3\nName | Position\nPaul John Roe | Chairperson\nSamuel Jim Doe | Member\n')
    out.append(('tables: a body-font header recognised by its words', 'ok' if ts and ts[0]['columns'] == ['Name', 'Position'] else 'ERROR', ''))
    # fix 9: parcel tables -> records -> land / corrigenda template
    LTT = load('land_table_template'); TRR = load('table_records')
    acq = ('GAZETTE NOTICE NO. 1\nTHE LAND ACT (No. 6 of 2012) CONSTRUCTION OF MERU-MIKINDURI-MAUA ROAD PROJECT DELETION, '
           'CORRIGENDA AND ADDENDUM IN PURSUANCE of the Land Act and further to Gazette Notice Nos. 5621 of 2009 and 5785 of 2023, '
           'the National Land Commission on behalf of Kenya Rural Roads Authority (KeRRA) gives notice.\n'
           'Deletion\nParcel Number | Registered Owner | Area Acq. (Ha)\n--- | --- | ---\n'
           'Akachiu/Auki/334 | Charles Jim Roe | 0.0113\nAkachiu/Auki/288 | Sebastian Doe | 0.0192\n'
           'Addendum\nParcel Number | Registered Owner | Area Acq. (Ha)\n--- | --- | ---\n'
           'Akachiu/Auki/632 | 0.0786\nAkachiu/Auki/ 25 | John Doe | 0.1084\n'
           'Akachiu/Auki/40 Jane Doe 0.0017 | TBD | 0.0050\n')
    r = LTT.extract(acq)
    ok = (r and [s['section'] for s in r['sections']] == ['Deletion', 'Addendum'] and r['related_notices'] == ['5621 of 2009', '5785 of 2023']
          and r['sections'][1]['parcels'][0] == {'parcel_id': 'Akachiu/Auki/632', 'owner': '', 'area_ha': '0.0786'}
          and r['sections'][1]['parcels'][1]['parcel_id'] == 'Akachiu/Auki/25' and r['rows_not_read'] == 1
          and r['acquiring_body'] == 'Kenya Rural Roads Authority')
    out.append(('land tables: sections, related notices, missing owner, split parcel, glued row refused', 'ok' if ok else 'ERROR', '' if ok else repr(r)[:300]))
    out.append(('land tables: an owner glued to a parcel is not a parcel id',
                'ok' if not TRR.parcel_ok('Kiganjo/Kiamwangi/2510 Hana Jane Roe') and TRR.parcel_ok('Mombasa South/Block I/1683') else 'ERROR', ''))
    # template gaps found by the Java unit tests (journal section 22)
    pb = ('CAUSE NO. E9 OF 2024\nBy Mary Roe, the deceased\'s widow, for a grant of probate of written will '
          'to the estate of Paul Roe, who died on 3rd January, 2022.\n')
    r = P.extract(pb)
    out.append(('probate: "of written will to the estate of"', 'ok' if r and r['deceased_name'] == 'Paul Roe' else 'ERROR', ''))
    CO = load('corrigenda_template')
    cn = ('GAZETTE NOTICE NO. 2\nCORRIGENDUM\nIN Gazette Notice No. 5121 of 2022,\n'
          'CAUSE NO. E7 of 2020, amend the deceased\'s name printed as "Harrison Doe" to read "Harrison Roe".')
    r = CO.extract(cn[cn.index('CAUSE NO.'):], cn)
    out.append(('corrigenda: corrected notice named before the cause (not its own header)',
                'ok' if r and r['amends_notice'] == '5121' and r['amends_notice_year'] == '2022' else 'ERROR', repr(r)[:120] if not r or r['amends_notice'] != '5121' else ''))
    # change-of-name template (docs/specs/change-of-name-template.md)
    CNT = load('change_of_name_template')
    base = ('GAZETTE NOTICE NO. 1\nCHANGE OF NAME NOTICE is given that by a deed poll dated 11th September, 2026, duly '
            'executed and registered in the Registry of Documents at Nairobi as Presentation No. 463, in Volume D1, '
            'Folio 362/3386, File No.\nMMXXVI, by our client, John Doe, of P.O. Box 111-10206, Thika in the Republic of '
            'Kenya, formerly known as John Roe, formally and absolutely renounced and abandoned the use of his former name '
            'John Roe and in lieu thereof assumed and adopted the name John Doe, for all purposes and authorizes and '
            'requests all persons at all times to designate, describe and address him by his assumed name John Doe only.\n'
            'EXAMPLE & COMPANY, Advocates for John Doe.')
    r = CNT.extract(base)
    ok = (r and r['former_name'] == 'John Roe' and r['assumed_name'] == 'John Doe' and r['registry'] == 'Nairobi'
          and (r['presentation_number'], r['volume'], r['folio'], r['file_number']) == ('463', 'D1', '362/3386', 'MMXXVI')
          and r['filed_by'] == 'advocates' and r['advocate_firm'] == 'EXAMPLE & COMPANY' and r['applicants'] == ['John Doe']
          and r['deed_poll_date'] == '11th September, 2026' and r['notice_id'] == '1')
    out.append(('change of name: standard deed poll', 'ok' if ok else 'ERROR', '' if ok else repr(r)[:200]))
    r = CNT.extract(base.replace('assumed and adopted the name John Doe', 'assumed and adopted the name Dr. John Doe'))
    out.append(('change of name: "Dr." does not end the name', 'ok' if r and r['assumed_name'] == 'Dr. John Doe' else 'ERROR', ''))
    r = CNT.extract(base.replace('his former name John Roe', 'his former name Peter Poe'))
    out.append(('change of name: the two former names disagree -> refused', 'ok' if r is None else 'ERROR', ''))
    r = CNT.extract(base.replace('dated 11th September, 2026', 'dated 16th September August, 2026'))
    out.append(('change of name: misprinted date -> read, date empty', 'ok' if r and r['deed_poll_date'] is None else 'ERROR', ''))
    r = CNT.extract(base.replace('assumed and adopted the name', 'in lieu thereof adopted the name').replace('Folio 362', 'Folio. 362'))
    out.append(('change of name: "in lieu thereof adopted" (no "assumed"), "Folio."', 'ok' if r and r['assumed_name'] == 'John Doe' and r['folio'] == '362/3386' else 'ERROR', repr(r)[:120] if not r else ''))
    r = CNT.extract(base.replace('assumed and adopted the name', 'assumed and re-adopted the name'))
    out.append(('change of name: "assumed and re-adopted"', 'ok' if r and r['assumed_name'] == 'John Doe' else 'ERROR', ''))
    minor = base.replace('by our client, John Doe, of', 'by our clients, (1) Jane Doe and (2) Jim Doe (Guardians), both of') \
                .replace('Kenya, formerly known', 'Kenya, on behalf of Baby Doe (minor), formerly known')
    r = CNT.extract(minor)
    out.append(('change of name: minor with guardians', 'ok' if r and r['on_behalf_of_minor'] and r['applicants'] == ['Jane Doe', 'Jim Doe'] else 'ERROR',
                '' if r and r['applicants'] == ['Jane Doe', 'Jim Doe'] else repr(r and r['applicants'])))
    bank = ('GAZETTE NOTICE NO. 2\nTHE CENTRAL BANK OF KENYA ACT (Cap. 491) CHANGE OF NAME IT IS notified that Example '
            'Microfinance Bank Limited has changed its name to Sample Microfinance Bank Limited.')
    out.append(('change of name: a bank renaming is not a deed poll -> refused', 'ok' if CNT.extract(bank) is None else 'ERROR', ''))
    # appointments template (docs/specs/appointments-template.md)
    AP = load('appointments_template')
    one = ('GAZETTE NOTICE NO. 3\nTHE UNIVERSITIES ACT (Cap. 210) EXAMPLE UNIVERSITY APPOINTMENT IN EXERCISE of the powers '
           'conferred by section 36 (1) (a) of the Universities Act, the Cabinet Secretary for Information, Communications and '
           'the Digital Economy appoints- JOHN DOE (DR.) to be the Non-Executive Chairperson of the Council of Example University, '
           'for a period of three (3) years, with effect from the 28th November, 2025. The appointment* of Jim Roe is revoked.\n'
           'Dated the 27th November, 2025.\nJANE ROE, Cabinet Secretary for Information, Communications and the Digital Economy.\n*G.N. 401/2025\n')
    r = AP.extract(one)
    ok = (r and len(r) == 1 and r[0]['person_name'] == 'JOHN DOE' and r[0]['honorific'] == 'DR'
          and r[0]['position'] == 'Non-Executive Chairperson' and r[0]['agency'] == 'Council of Example University'
          and r[0]['appointing_authority'] == 'Cabinet Secretary for Information, Communications and the Digital Economy'
          and r[0]['term_length'] == 'three (3) years' and r[0]['effective_date'] == '28th November, 2025'
          and r[0]['revokes_gn_number'] == '401/2025' and r[0]['act'] == 'Universities Act'
          and r[0]['legal_provision'] == 'section 36 (1) (a)' and r[0]['signatory'] == 'JANE ROE')
    out.append(('appointments: one person, authority title kept whole, revoked G.N.', 'ok' if ok else 'ERROR', '' if ok else repr(r)[:200]))
    lst = one.replace('JOHN DOE (DR.) to be the Non-Executive Chairperson',
                      'Under paragraph (a)- John Doe - Chairperson, Under paragraph (d)- Jane Doe, Jim Doe (Prof.), to be the Chairperson and Members')
    r = AP.extract(lst)
    ok = r and [(x['person_name'], x['position'], x['legal_provision'], x['honorific']) for x in r] == [
        ('John Doe', 'Chairperson', 'section 36 (1) (a); paragraph (a)', None),
        ('Jane Doe', 'Member', 'section 36 (1) (a); paragraph (d)', None),
        ('Jim Doe', 'Member', 'section 36 (1) (a); paragraph (d)', 'Prof')]
    out.append(('appointments: list with chair label -> one record per person', 'ok' if ok else 'ERROR', '' if ok else repr(r)[:200]))
    r = AP.extract(one.replace('appoints- JOHN DOE (DR.) to be', 'appoints- JOHN DOEto be').replace('appoints-', 're-appoints-'))
    out.append(('appointments: glued "DOEto be", re-appointment', 'ok' if r and r[0]['person_name'] == 'JOHN DOE' and r[0]['appointment_type'] == 're-appointment' else 'ERROR', repr(r)[:120] if not r else ''))
    r = AP.extract(one.replace('JOHN DOE (DR.) to be', 'JOHN DOE to serve as'))
    out.append(('appointments: "to serve as" is not part of the name', 'ok' if r and r[0]['person_name'] == 'JOHN DOE' else 'ERROR', repr(r)[:120]))
    r = AP.extract(one.replace('the Cabinet Secretary for Information, Communications and the Digital Economy appoints- JOHN DOE (DR.) to be',
                               'the Chief Justice makes the following appointment- HON. JOHN DOE as'))
    out.append(('appointments: "makes the following appointment-" (held-out No 175)', 'ok' if r and r[0]['person_name'] == 'JOHN DOE'
                and r[0]['honorific'] == 'HON' and r[0]['appointing_authority'] == 'Chief Justice' else 'ERROR', repr(r)[:160]))
    r = AP.extract(one.replace('JOHN DOE (DR.)', 'John Doe, Jane Roe to'))
    out.append(('appointments: a fragmented name refuses the whole list', 'ok' if r is None else 'ERROR', repr(r)[:120]))
    r = AP.extract(one.replace('JOHN DOE (DR.) to be the Non-Executive Chairperson', 'Jane Doe, Jim Doe, to be the Chairperson and Members'))
    out.append(('appointments: unlabelled "Chairperson and Members" -> refused (no guessing)', 'ok' if r is None else 'ERROR', repr(r)[:120]))
    r = AP.extract('GAZETTE NOTICE NO. 4\nAPPOINTMENT OF COMMISSIONERS FOR OATHS it is notified\nS/No. | Name\n1. | John Doe\n')
    out.append(('appointments: a table notice is refused', 'ok' if r is None else 'ERROR', ''))
    # ruled tables (fix 7b, docs/specs/ruled-tables.md): the extractor's pass, on synthetic pages
    r = subprocess.run(['node', os.path.join(HERE, 'ruled_test.js')], capture_output=True, text=True)
    lines = [l.split('\t') for l in r.stdout.splitlines() if l.strip()]
    if r.returncode or not lines:
        out.append(('ruled: tools/ruled_test.js ran', 'ERROR', (r.stderr or '')[:160]))
    for ln in lines:
        out.append((ln[1], ln[0], ln[2] if len(ln) > 2 else ''))
    TX = load('table_extract')
    rows = TX.tables('GAZETTE NOTICE NO. 5\nParcel | Owner | Area\nX/1 | | 0.10\nX/2 | Jane Doe | 0.20\n')[0]['rows']
    out.append(('tables: an empty cell ("a | | c") keeps its column', 'ok' if rows[0] == ['X/1', '', '0.10'] else 'ERROR', repr(rows)))
    # a row merged across the table (a date / venue label) arrives as "label |" and stays in the table;
    # table_records does not take it for a record
    G2 = load('gazette_clean')
    sched = ('GAZETTE NOTICE NO. 6\nTHE LAND ACT\nSCHEDULE\nParcel No. | Registered Owner (s) | Area (Ha)\n--- | --- | ---\n'
             'Tuesday, 1st July, 2025 at the Chief\'s Office from 9.00 a.m. |\nX/Block 1/1 | Jane Doe | 0.10\n'
             'Wednesday, 2nd July, 2025 at the Chief\'s Office from 9.00 a.m. |\nX/Block 1/2 | | 0.20\n')
    tabs = TX.tables(G2.clean(sched))
    out.append(('tables: a label row ("text |") keeps header and rows in one table',
                'ok' if len(tabs) == 1 and tabs[0]['columns'] and len(tabs[0]['rows']) == 4 else 'ERROR', repr(tabs)[:160]))
    TRC = load('table_records')
    recs = TRC.records(tabs[0]) if tabs else []
    out.append(('table records: a group label is not a record', 'ok' if [r.get('parcel_id') for r in recs] == ['X/Block 1/1', 'X/Block 1/2'] else 'ERROR', repr(recs)[:160]))
    # scan tables (docs/specs/scan-tables.md): synthetic word boxes, rules and image
    STB = load('scan_tables')
    import numpy as _np
    def _w(line, x, y, text, w=None):
        return {'key': (1, 1, line), 'x': x, 'y': y, 'w': w or 12 * len(text), 'h': 20, 'text': text}
    ruled = {'h': [(100.0, 50.0, 650.0), (140.0, 50.0, 650.0), (220.0, 50.0, 650.0), (260.0, 50.0, 650.0)],
             'v': [(50.0, 100.0, 260.0), (250.0, 100.0, 260.0), (450.0, 100.0, 260.0), (650.0, 100.0, 260.0)], 'W': 2400.0, 'H': 3400.0}
    words = [_w(1, 60, 110, 'Parcel'), _w(1, 260, 110, 'Owner'), _w(1, 460, 110, 'Area'),
             _w(2, 200, 150, '1319]', 45), _w(2, 240, 150, '|'), _w(2, 260, 150, 'Jane'), _w(2, 320, 150, 'Doe'), _w(2, 460, 150, '0.10'),
             _w(3, 260, 185, 'and'), _w(3, 310, 185, 'John'), _w(3, 370, 185, 'Roe'),
             _w(4, 60, 230, '1320'), _w(4, 260, 230, 'Jim'), _w(4, 320, 230, 'Doe'), _w(4, 460, 230, '0.20')]
    txt = STB.page_text(words, ruled)
    out.append(('scan tables: ruled rows - wrapped cell joined, rule characters dropped',
                'ok' if txt.split('\n') == ['Parcel | Owner | Area', '1319 | Jane Doe and John Roe | 0.10', '1320 | Jim Doe | 0.20'] else 'ERROR', repr(txt)[:200]))
    prose = [_w(1, 60, 110, 'hereof,'), _w(1, 160, 110, '|'), _w(1, 180, 110, 'shall'), _w(1, 260, 110, 'issue'),
             _w(2, 60, 150, 'NOTICE'), _w(2, 160, 150, 'NO.'), _w(2, 220, 150, '|'), _w(2, 240, 150, '2057'),
             _w(3, 60, 190, 'district'), _w(3, 160, 190, 'of'), _w(3, 200, 190, '|'), _w(3, 220, 190, 'Kakamega')]
    txt = STB.page_text(prose, None)
    out.append(('scan tables: no cells outside tables ("| shall" -> "I shall", "| 2057" -> "|2057", speck dropped)',
                'ok' if txt.split('\n') == ['hereof, I shall issue', 'NOTICE NO. |2057', 'district of Kakamega'] else 'ERROR', repr(txt)[:200]))
    img = _np.full((400, 600), 255, dtype=_np.uint8)
    for yy in (50, 150, 250):
        img[yy, 50:550] = 0
    for xx in (50, 300, 550):
        img[50:251, xx] = 0
    R = STB.image_rules(img, 1, 150)
    out.append(('scan tables: ruling lines found in an image', 'ok' if len(R['h']) == 3 and len(R['v']) == 3 and len(STB.lattices(R)) == 1 else 'ERROR', repr(R)[:200]))
    # page-level OCR (docs/specs/mixed-pages.md)
    MPG = load('mixed_pages')
    raw = 'GAZETTE NOTICE NO. 1 THE LAND ACT notice text here\n\f\n\fsecond page with enough words here now'
    txt, done = MPG.fill(raw, lambda p: 'OCR text of page %d' % p)
    out.append(('page OCR: a textless page is replaced at its place', 'ok' if MPG.textless_pages(raw) == [2] and done == [2]
                and txt.split('\f') == ['GAZETTE NOTICE NO. 1 THE LAND ACT notice text here\n', 'OCR text of page 2\n',
                                        'second page with enough words here now'] else 'ERROR', repr(txt)))
    r = subprocess.run([sys.executable, os.path.join(HERE, 'mixed_pages_check.py')], capture_output=True, text=True)
    ln = (r.stdout.strip().splitlines() or ['ERROR\tpage OCR: image page check ran\t' + r.stderr[-160:]])[-1].split('\t')
    if ln[0] != 'skip':
        out.append((ln[1], ln[0], ln[2] if len(ln) > 2 else ''))
    t = LR.provision_text('constitution', '179', '(2)')
    out.append(('cite: clause text includes its paragraphs', 'ok' if t and t.startswith('(2)') and '(b) members appointed' in t and '(3)' not in t else 'ERROR', ''))
    return out

if __name__ == '__main__':
    fail = 0
    print('== 1. optional-word typos ==')
    for f in sorted(glob.glob(os.path.join(HERE, '*_template.py'))) + [os.path.join(HERE, 'gazette_clean.py')]:
        if not os.path.exists(f): continue
        for ln, sev, msg in check_optional_words(f):
            print('  %-5s %s:%d  %s' % (sev, os.path.basename(f), ln, msg)); fail += 1
    print('  (none)' if not fail else '')

    print('\n== 2. rigid whitespace between literal words ==')
    n2 = 0
    for f in sorted(glob.glob(os.path.join(HERE, '*_template.py'))):
        for ln, sev, msg in check_rigid_whitespace(f):
            print('  %-5s %s:%d  %s' % (sev, os.path.basename(f), ln, msg)); n2 += 1
    if not n2: print('  (none)')

    print('\n== 3. schema alignment ==')
    samples = {}
    try:
        P, L = load('probate_template'), load('land_template')
        t = open(os.path.join(HERE, 'fresh', 'raw2Retest.txt'), encoding='utf-8', errors='replace').read()
        for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', t):
            for b in [x for x in re.split(r'(?=CAUSE NO\.)', n) if x.startswith('CAUSE NO.')]:
                r = P.extract(b, n)
                if r: samples['probate_template'] = r; break
            if 'probate_template' in samples: break
        t2 = open(os.path.join(HERE, 'fresh', 'raw0110.txt'), encoding='utf-8', errors='replace').read()
        for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', t2):
            r = L.extract(n)
            if r: samples['land_template'] = r; break
    except Exception as e:
        print('  could not build samples:', e)
    for mod, sev, msg in check_schema_alignment(samples):
        print('  %-5s %-20s %s' % (sev, mod, msg))
        if sev == 'ERROR': fail += 1

    print('\n== 4. generation output ==')
    for cat, sev, msg in check_generation(samples):
        print('  %-5s %-20s %s' % (sev, cat, msg))
        if sev == 'ERROR': fail += 1

    print('\n== 5. key fields (names, parcel ids) ==')
    for what, sev, msg in check_key_fields(samples):
        print('  %-5s %-20s %s' % (sev, what, msg))
        if sev == 'ERROR': fail += 1

    print('\n== 6. behavioural ==')
    for lbl, sev, _ in behavioural():
        print('  %-5s %s' % (sev, lbl))
        if sev == 'ERROR': fail += 1

    print('\n== 7. source escapes ==')
    for lbl, sev in check_escapes():
        print('  %-5s %s' % (sev, lbl))
        if sev == 'ERROR': fail += 1

    print('\n== 8. reference freshness ==')
    for lbl, sev in check_reference_freshness():
        print('  %-5s %s' % (sev, lbl))
        if sev == 'ERROR': fail += 1

    print('\nERRORS: %d' % fail)
    sys.exit(1 if fail else 0)
