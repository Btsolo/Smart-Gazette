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
LT = load('land_table_template')
CN = load('change_of_name_template')
AP = load('appointments_template')

# Order matters: first match wins, most specific first.
# Ordered: most specific first, first match wins. Whitespace is kept loose
# (\s*) between literal words because the fragment-joiner sometimes glues
# them ("THE LANDREGISTRATION ACT").
CATEGORY_SIGNALS = [
    # fix 6 step 3: "amend the" dropped - 5 notices, none a correction
    ('Corrigenda',            r'\bCORRIGEND(?:UM|A)\b'),
    ('Change_of_Name',        r'CHANGE\s*OF\s*NAME|by\s*a\s*deed\s*poll'),
    ('court_legal',           r'PROBATE\s*AND\s*ADMINISTRATION|TAKE\s*NOTICE\s*that\s*(?:an?\s*)?applications?\s*having\s*been\s*made'
                              r'|IN\s*THE\s*(?:HIGH\s*COURT|CHIEF\s*MAGISTRATE|SENIOR\s*PRINCIPAL|PRINCIPAL\s*MAGISTRATE|RESIDENT\s*MAGISTRATE)'),
    ('land_property',         r'THE\s*LAND\s*(?:R\s*E\s*G\s*I\s*S\s*T\s*R\s*A\s*T\s*I\s*O\s*N|TITLES?)\s*ACT'
                              r'|ISSUE\s*OF\s*A?\s*(?:NEW|PROVISIONAL|REPLACEMENT)'
                              r'|THE\s*LAND\s*ACT|PHYSICAL\s*AND\s*LAND\s*USE\s*PLANNING'),
    # fix 6 step 3: an Elections Act notice whose heading did not say so
    # (Java key "electionsact", moved here from Legislation)
    ('Elections',             r'ELECTIONS\s*ACT'),
    ('tenders',               r'\bTENDER\b|INVITATION\s*TO\s*TENDER|PROCUREMENT|EXPRESSION\s*OF\s*INTEREST|PREQUALIFICATION'),
    ('licensing',             r'\bLICEN[CS]E|LICENSING|PERMIT\b|THE\s*MINING\s*ACT|SPECTRUM|OPERATOR\s*LICEN'),
    ('company_registrations', r'THE\s*COMPANIES\s*ACT|struck\s*off|dissolution\s*of|THE\s*INSOLVENCY\s*ACT|liquidat'),
    ('Legislation',           r'\bBILL\b|LEGAL\s*NOTICE\s*NO|THE\s*STATUTORY\s*INSTRUMENTS\s*ACT|REGULATIONS?,\s*20\d\d|is\s*hereby\s*enacted'),
    # fix 6 step 3: bare PROMOTION / RETIREMENT dropped - 21 notices, ~none HR
    # ("Export Promotion Agency", "Retirement Benefits Authority", exchequer tables)
    ('public_service_hr',     r'REDESIGNATION|TRANSFER\s*OF\s*SERVICE|DISMISSAL|CONFIRMATION\s*IN\s*(?:POST|APPOINTMENT)'),
    ('appointments',          r'\bAPPOINTMENTS?\b|\bRE\s*-?\s*APPOINTMENT\b|\bappoints\b|NOMINATION|REVOCATION\s*OF\s*APPOINTMENT'),
]

# A land title notice whose BODY mentions a court ("whereas the Chief
# Magistrate's Court ... in succession CAUSE NO. 158 of 2019 has issued grant")
# is still a land notice: its heading is the Land Registration / Titles Act.
# Checked before the court signals (fix 4: ~370 land notices a year were
# filed as probate - lesson 12 again).
LAND_HEAD = re.compile(r'THE\s*LAND\s*(?:R\s*E\s*G\s*I\s*S\s*T\s*R\s*A\s*T\s*I\s*O\s*N|TITLES?)\s*ACT', re.I)
PROBATE_HEAD = re.compile(r'PROBATE\s*AND\s*ADMINISTRATION', re.I)

# County_Government (fix 6): its schema existed but nothing routed to it - 558
# notices over 2022-2026 went to Appointments / Miscellaneous (Python) or to
# the AI triage call (Java). Matched on the UPPERCASE heading only (case-
# sensitive, first 400 characters): the body of many national notices mentions
# "the County Assembly of ...", the heading of a county notice names the Act,
# the assembly, or a county's own Act ("THE KIAMBU COUNTY FINANCE ACT").
COUNTY_HEAD = re.compile(
    r'C\s*O?\s*UNTY\s*GOVERNMENTS?\s*(?:\(\s*AMENDMENT\s*\)\s*)?S?\s*ACT'
    r'|URBAN\s*AREAS\s*AND\s*CITIES'
    r'|COUNTY\s*ASSEMBLY\s*(?:OF\s+[A-Z]|STANDING\s*ORDERS)'
    r'|COUNTY\s*GOVERNMENT\s*OF\s+[A-Z]'
    r"|\bTHE\s+[A-Z'\-]+(?:\s+[A-Z'\-]+)?\s+COUNTY\s+[A-Z ,()\-]*?ACT\b")
# a county heading inside a probate, land-title or elections notice is not one;
# county physical-planning notices stay Land_Property (land is the subject,
# the county only the issuer - 58 notices, 2022-2026)
NOT_COUNTY_HEAD = re.compile(r'PROBATE\s*AND\s*ADMINISTRATION|ELECTIONS?\s*ACT|PHYSICAL\s*AND\s*LAND\s*USE\s*PLANNING', re.I)


def county_heading(notice):
    head = notice[:400]
    return bool(COUNTY_HEAD.search(head) and not NOT_COUNTY_HEAD.search(head)
                and not LAND_HEAD.search(notice[:300]))


# New categories (fix 6 step 2), each read from the UPPERCASE heading only
# (case-sensitive, first 400 characters), like County_Government. Order =
# priority. Measured 2022-2026 (tools/category_eval.py): Elections 146,
# Uncollected_Goods 350, Environment 158, Utility_Tariffs 93 notices, which
# Python filed as Miscellaneous / Legislation / Licensing / Tenders and Java
# mostly sent to AI triage. Body keywords are NOT used: the IEBC's name and
# "Registrar of Political Parties" also appear in Treasury exchequer tables.
HEADING_CATEGORIES = [
    ('Elections', re.compile(
        r'ELECTIONS?\s*ACT|ELECTORAL\s*AND\s*BOUNDARIES|INDEPENDENT\s*ELECTORAL|POLITICAL\s*PARTIES'
        r'|ELECTION\s*PETITION|BY-?\s*ELECTION|GENERAL\s*ELECTION|REGISTRATION\s*OF\s*VOTERS'
        r'|ELECTION\s*OFFENCES|ELECTION\s*CAMPAIGN'), None),
    ('Uncollected_Goods', re.compile(r'UNCOLLECTED\s*GOODS'), None),
    ('Environment', re.compile(
        r'ENVIRONMENTAL\s*MANAGEMENT|ENVIRONMENTAL\s*IMPACT'
        r'|NATIONAL\s*ENVIRONMENT(?:AL)?\s*(?:MANAGEMENT|TRIBUNAL|COMPLAINTS)|STRATEGIC\s*ENVIRONMENTAL'), None),
    # a regulator's board appointment stays Appointments (7 notices)
    ('Utility_Tariffs', re.compile(
        r'TARIFF|WATER\s*SERVICES\s*REGULATORY|ENERGY\s*AND\s*PETROLEUM\s*REGULATORY|RETURN\s*ON\s*ASSETS'),
        re.compile(r'APPOINTMENT')),
]


def heading_category(notice):
    """Category decided by the heading alone, or None."""
    head = notice[:400]
    if LAND_HEAD.search(notice[:300]) and not PROBATE_HEAD.search(notice[:300]):
        return None                      # a land title notice is land (fix 4)
    for name, pat, unless in HEADING_CATEGORIES:
        if pat.search(head) and not (unless and unless.search(head)):
            return name
    return 'County_Government' if county_heading(notice) else None


def categorise(notice):
    for name, pat in CATEGORY_SIGNALS:
        # after Corrigenda / Change_of_Name, before every body keyword
        if name == 'court_legal':
            h = heading_category(notice)
            if h:
                return h
        if name == 'court_legal' and LAND_HEAD.search(notice[:300]) and not PROBATE_HEAD.search(notice[:300]):
            return 'land_property'
        if re.search(pat, notice, re.I | re.S):
            return name
    return 'miscellaneous'

def template_for(cat, notice):
    """Return the extractor result, or None if no template covers this category."""
    if cat == 'court_legal':
        blocks = P.split_causes(notice)
        if not blocks: return None
        return all(P.extract(b, notice) for b in blocks) or None
    if cat == 'land_property':
        # parcel tables (acquisition schedules, conversions) after the
        # one-parcel lost-title template (fix 9)
        return L.extract(notice) or LT.extract(notice)
    if cat == 'Corrigenda':
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', notice) if b.startswith('CAUSE NO.')]
        return (C.extract(blocks[0], notice) if blocks else None) or C.extract_notice(notice) or LT.extract(notice)
    if cat == 'Change_of_Name':
        return CN.extract(notice)          # deed polls (docs/specs/change-of-name-template.md)
    if cat == 'appointments':
        return AP.extract(notice)          # one record per person (docs/specs/appointments-template.md)
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
