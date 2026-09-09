"""
Measures how reliable each pre-filter keyword actually is.

The pre-filter assigns a category with no AI call, so a loose keyword is
expensive: the notice gets the wrong schema, extraction returns mostly empty
fields, and the generated article is useless. That is exactly what happened to
notice 13502, an election notice that matched "nominationof" and was filed as
an Appointment.

Method
------
Ground truth comes from the notice's own heading block - the Act it is made
under and its subject line - which is unambiguous and independent of the
keywords being tested. Each candidate keyword is then scored on the corpus:

    precision = notices where the keyword fired AND the truth agrees
                ------------------------------------------------------
                all notices where the keyword fired

    coverage  = how many notices the keyword fires on at all

A keyword is only worth keeping if precision is high. Low precision means it
is stealing notices from other categories.
"""
import re, glob, os, importlib.util
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
_s = importlib.util.spec_from_file_location('G', os.path.join(HERE, 'gazette_clean.py'))
G = importlib.util.module_from_spec(_s); _s.loader.exec_module(G)

# ---- ground truth: the governing Act and the subject heading ---------------
# These are structural (they appear in the notice's masthead block), not
# keyword-based, so they can referee the keywords fairly.
TRUTH = [
    ('court_legal',           r'PROBATE\s*AND\s*ADMINISTRATION'),
    ('Corrigenda',            r'\bCORRIGEND(?:UM|A)\b'),
    ('Change_of_Name',        r'CHANGE\s*OF\s*NAME'),
    ('land_property',         r'THE\s*LAND\s*(?:R\s*E\s*G\s*I\s*S\s*T\s*R\s*A\s*T\s*I\s*O\s*N|TITLES?)\s*ACT'),
    ('company_registrations', r'THE\s*COMPANIES\s*ACT|THE\s*INSOLVENCY\s*ACT'),
    ('Legislation',           r'THE\s*ELECTIONS?\s*ACT|THE\s*STATUTORY\s*INSTRUMENTS\s*ACT|\bBILL\b|L\.N\.\s*\d+'),
    ('licensing',             r'THE\s*MINING\s*ACT|LICEN[CS]E\s*NO|THE\s*BETTING'),
    ('tenders',               r'INVITATION\s*TO\s*TENDER|PREQUALIFICATION'),
    ('public_service_hr',     r'\bPROMOTION\b|\bREDESIGNATION\b|\bRETIREMENT\b'),
    ('appointments',          r'\bAPPOINTMENT\s*OF\s*(?:THE|A|MEMBERS)|\bRE\s*-?\s*APPOINTMENT\b'),
]

def truth_of(notice):
    head = notice[:900]                     # heading block only
    for cat, pat in TRUTH:
        if re.search(pat, head, re.I | re.S):
            return cat
    return None                             # unknown - excluded from scoring

# ---- candidate keywords, matched against whitespace-squashed text ----------
CANDIDATES = {
    'court_legal': ['lettersofadministration', 'grantofprobate', 'successioncause',
                    'probateandadministration', 'takenoticethatanapplication',
                    'intheestateof', 'deceased', 'intestate'],
    'land_property': ['landregistrationact', 'landtitlesact', 'titledeed',
                      'certificateoflease', 'landregistrar', 'provisionalcertificate',
                      'hasbeenlost', 'registeredasproprietor', 'landtitle'],
    'Corrigenda': ['corrigendum', 'corrigenda', 'amendthe', 'printedas', 'toread'],
    'Change_of_Name': ['changeofname', 'byadeedpoll', 'abandonedthename',
                       'assumedthename', 'deedpoll'],
    'appointments': ['isherebyappointed', 'areherebyappointed', 'appointmentof',
                     'reappointment', 'nominationof', 'appoints', 'nomination'],
    'company_registrations': ['companiesact', 'struckoff', 'dissolutionof',
                              'certificateofincorporation', 'insolvencyact'],
    'Legislation': ['bill,20', 'legalnotice', 'statutoryinstruments', 'regulations,20',
                    'electionsact', 'isherebyenacted'],
    'licensing': ['licence', 'license', 'permit', 'miningact', 'licensing'],
    'tenders': ['tender', 'procurement', 'expressionofinterest', 'prequalification'],
    'public_service_hr': ['promotion', 'redesignation', 'retirement', 'transferofservice'],
}

def main():
    notices, seen = [], set()
    for f in sorted(glob.glob('/mnt/user-data/uploads/raw*.txt')):
        base = os.path.basename(f)[:-4]
        # Deduplicate: several gazettes were extracted twice (a "Retest" run).
        # Keep exactly one copy of each, without dropping the gazette entirely -
        # an earlier version of this filter excluded both copies of the
        # probate-heavy issue and left court_legal with nothing to score.
        stem = re.sub(r'Retest.*$', '', base)
        if stem in seen:
            continue
        seen.add(stem)
        d = open(f, 'rb').read()
        try:    t = d.decode('utf-16')
        except  UnicodeError: t = d.decode('utf-8', errors='replace')
        c = G.apply_ascending_lock(G.clean(t))
        for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', c):
            if n.startswith('GAZETTE NOTICE NO.'):
                notices.append((n, re.sub(r'\s+', '', n.lower())))

    labelled = [(n, sq, truth_of(n)) for n, sq in notices]
    known = [x for x in labelled if x[2]]
    print('notices: %d | with reliable ground truth: %d\n' % (len(notices), len(known)))

    print('%-24s %-26s %7s %9s  %s' % ('category', 'keyword', 'fires', 'precision', 'verdict'))
    print('-' * 86)
    for cat, keys in CANDIDATES.items():
        for k in keys:
            fired = [x for x in known if k in x[1]]
            if not fired:
                print('%-24s %-26s %7d %9s  %s' % (cat, k, 0, '-', 'NEVER FIRES - drop'))
                continue
            hit = sum(1 for x in fired if x[2] == cat)
            p = 100.0 * hit / len(fired)
            if p >= 95:   v = 'KEEP'
            elif p >= 80: v = 'ok, watch'
            else:
                wrong = defaultdict(int)
                for x in fired:
                    if x[2] != cat: wrong[x[2]] += 1
                top = sorted(wrong.items(), key=lambda kv: -kv[1])[:2]
                v = 'DROP - steals from %s' % ', '.join('%s(%d)' % t for t in top)
            print('%-24s %-26s %7d %8.1f%%  %s' % (cat, k, len(fired), p, v))
        print()

if __name__ == '__main__':
    main()
