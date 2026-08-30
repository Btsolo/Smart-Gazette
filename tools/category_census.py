"""
Template coverage measured per schema category, across the corpus.

Maps each notice to its schema category using the same kind of signals the
keyword pre-filter uses (act title, subject heading), then reports how many
notices in each category a rule-based template can handle without an AI call.
"""
import re, glob, os, sys, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
def load(name):
    s = importlib.util.spec_from_file_location(name, os.path.join(HERE, name + '.py'))
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m

P, L, C = load('probate_template'), load('land_template'), load('corrigenda_template')

# Order matters: first match wins, most specific first.
# Ordered: most specific first, first match wins. Whitespace is kept loose
# (\s*) between literal words because the fragment-joiner sometimes glues
# them ("THE LANDREGISTRATION ACT").
CATEGORY_SIGNALS = [
    ('Corrigenda',            r'\bCORRIGEND(?:UM|A)\b|\bamend\s*the\b'),
    ('Change_of_Name',        r'CHANGE\s*OF\s*NAME|by\s*a\s*deed\s*poll'),
    ('court_legal',           r'PROBATE\s*AND\s*ADMINISTRATION|TAKE\s*NOTICE\s*that\s*(?:an?\s*)?applications?\s*having\s*been\s*made'
                              r'|IN\s*THE\s*(?:HIGH\s*COURT|CHIEF\s*MAGISTRATE|SENIOR\s*PRINCIPAL|PRINCIPAL\s*MAGISTRATE|RESIDENT\s*MAGISTRATE)'),
    ('land_property',         r'THE\s*LAND\s*(?:R\s*E\s*G\s*I\s*S\s*T\s*R\s*A\s*T\s*I\s*O\s*N|TITLES?)\s*ACT'
                              r'|ISSUE\s*OF\s*A?\s*(?:NEW|PROVISIONAL|REPLACEMENT)'
                              r'|THE\s*LAND\s*ACT|PHYSICAL\s*AND\s*LAND\s*USE\s*PLANNING'),
    ('tenders',               r'\bTENDER\b|INVITATION\s*TO\s*TENDER|PROCUREMENT|EXPRESSION\s*OF\s*INTEREST|PREQUALIFICATION'),
    ('licensing',             r'\bLICEN[CS]E|LICENSING|PERMIT\b|THE\s*MINING\s*ACT|SPECTRUM|OPERATOR\s*LICEN'),
    ('company_registrations', r'THE\s*COMPANIES\s*ACT|struck\s*off|dissolution\s*of|THE\s*INSOLVENCY\s*ACT|liquidat'),
    ('Legislation',           r'\bBILL\b|LEGAL\s*NOTICE\s*NO|THE\s*STATUTORY\s*INSTRUMENTS\s*ACT|REGULATIONS?,\s*20\d\d|is\s*hereby\s*enacted'),
    ('public_service_hr',     r'PROMOTION|REDESIGNATION|TRANSFER\s*OF\s*SERVICE|RETIREMENT|DISMISSAL|CONFIRMATION\s*IN\s*(?:POST|APPOINTMENT)'),
    ('appointments',          r'\bAPPOINTMENTS?\b|\bRE\s*-?\s*APPOINTMENT\b|\bappoints\b|NOMINATION|REVOCATION\s*OF\s*APPOINTMENT'),
]

def categorise(notice):
    for name, pat in CATEGORY_SIGNALS:
        if re.search(pat, notice, re.I | re.S):
            return name
    return 'miscellaneous'

def template_for(cat, notice):
    """Return the extractor result, or None if no template covers this category."""
    if cat == 'court_legal':
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', notice) if b.startswith('CAUSE NO.')]
        if not blocks: return None
        return all(P.extract(b, notice) for b in blocks) or None
    if cat == 'land_property':
        return L.extract(notice)
    if cat == 'Corrigenda':
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', notice) if b.startswith('CAUSE NO.')]
        return C.extract(blocks[0]) if blocks else None
    return None          # no template yet for this category

if __name__ == '__main__':
    files = sys.argv[1:] or sorted(glob.glob(os.path.join(HERE, 'fresh', '*.txt')))
    seen = {}
    for f in files:
        if os.path.basename(f).replace('.txt','') in ('raw0100Retest','raw0139Retest','raw068Retest',
                                                      'raw0136Retest','raw2','raw200266','raw20077',
                                                      'raw0186Retest','raw0186RetestR','raw070Retest','raw071Retest'):
            continue
        t = open(f, encoding='utf-8', errors='replace').read()
        for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', t):
            if not n.startswith('GAZETTE NOTICE NO.'): continue
            cat = categorise(n)
            d = seen.setdefault(cat, [0, 0, 0, 0])
            d[0] += 1
            if template_for(cat, n): d[1] += 1
            # block-level: how many individual extraction calls are avoided
            if cat == 'court_legal':
                bl = [b for b in re.split(r'(?=CAUSE NO\.)', n) if b.startswith('CAUSE NO.')]
                d[2] += len(bl); d[3] += sum(1 for b in bl if P.extract(b, n))
            elif cat == 'Corrigenda':
                bl = [b for b in re.split(r'(?=CAUSE NO\.)', n) if b.startswith('CAUSE NO.')]
                d[2] += max(len(bl), 1); d[3] += sum(1 for b in bl if C.extract(b))
            else:
                d[2] += 1; d[3] += (1 if template_for(cat, n) else 0)

    tot = sum(v[0] for v in seen.values()); cov = sum(v[1] for v in seen.values())
    btot = sum(v[2] for v in seen.values()); bcov = sum(v[3] for v in seen.values())
    print('%-22s %8s %7s %10s %9s' % ('schema category', 'notices', 'share', 'notice-lvl', 'call-lvl'))
    print('-' * 62)
    for cat, v in sorted(seen.items(), key=lambda x: -x[1][0]):
        n, ok, bn, bok = v
        print('%-22s %8d %6.1f%% %9s %9s' % (cat, n, 100*n/tot,
              ('%.1f%%' % (100*ok/n)) if n else '-',
              ('%.1f%%' % (100*bok/bn)) if bn else '-'))
    print('-' * 62)
    print('%-22s %8d %6.1f%% %9.1f%% %8.1f%%' % ('TOTAL', tot, 100.0, 100*cov/tot, 100*bcov/btot))
    print('\nnotice-level = every sub-case in the notice extracted (all-or-nothing)')
    print('call-level   = individual extraction calls avoided (%d of %d)' % (bcov, btot))
