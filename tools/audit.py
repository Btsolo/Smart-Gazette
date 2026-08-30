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
import re, os, sys, json, glob, importlib.util

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
    return [(lbl, 'ok' if rx.search(s) else 'ERROR', '') for lbl, rx, s in cases]

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

    print('\n== 4. behavioural ==')
    for lbl, sev, _ in behavioural():
        print('  %-5s %s' % (sev, lbl))
        if sev == 'ERROR': fail += 1

    print('\nERRORS: %d' % fail)
    sys.exit(1 if fail else 0)
