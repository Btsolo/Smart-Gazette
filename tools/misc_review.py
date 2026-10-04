"""
Miscellaneous review across years - measurement only.

Pools notices the Python categoriser files as Miscellaneous or the Java
pre-filter sends to AI triage, then tests proposed homes for them: existing
categories that should have caught them, and candidate new categories. Each
rule is a heading pattern (Act + subject) - structural, like the truth rules
in keyword_probability.py - so it can be checked for precision over ALL
notices, not only the Misc ones.

Usage: python tools/misc_review.py 2022 2023 2024 2026
Writes reports/eval/misc_review.txt
"""
import csv, os, re, sys
from collections import Counter, defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (proposed home, existing?, heading pattern on "act | subject")
RULES = [
    ('Elections',                'new',      r'ELECTIONS?\s*ACT|ELECTORAL AND BOUNDARIES|ELECTION\S*\s*\((?:GENERAL|REGISTRATION|PARLIAMENTARY)|POLITICAL\s*PART|LEGISLATIVE ASSEMBLY ELECTIONS'),
    ('County_Government',        'existing', r'COUNTY\s*GOVERNMENTS?\s*S?\s*ACT|COUNTY ASSEMBLY|URBAN AREAS AND CITIES|STANDING ORDERS|ASSUMPTION OF THE OFFICE OF GOVERNOR'),
    ('Environment (EIA/NEMA)',   'new',      r'ENVIRONMENTAL\s*MANAGEMENT|ENVIRONMENTAL IMPACT|NATIONAL ENVIRONMENT'),
    ('Land_Property (planning / Land Act)', 'existing', r'PHYSICAL AND LAND USE PLANNING|THE LAND ACT|LAND CONTROL ACT|SURVEY ACT|SECTIONAL PROPERTIES|COMPULSOR'),
    ('Water & utility tariffs',  'new',      r'WATER\s*ACT|TARIFF|ENERGY ACT|PETROLEUM'),
    ('Appointments (state bodies)', 'existing', r'STATE\s*CORPORATIONS|UNIVERSITIES\s*ACT|APPOINTMENT|BOARD|COUNCIL|COMMITTEE|TASKFORCE|TASK FORCE'),
    ('Uncollected goods / auctions', 'new',  r'UNCOLLECTED GOODS|AUCTIONEER|DISPOSAL OF'),
    ('Customs & revenue',        'new',      r'CUSTOMS|EXCISE|KENYA REVENUE|OVERSTAYED|GOODS TO BE SOLD'),
    ('Legal profession / courts','new',      r'ADVOCATES\s*ACT|ADMISSION OF ADVOCATES|RECORDS DISPOSAL|CIVIL PROCEDURE|JUDICATURE|MAGISTRATES|HIGH COURT \(ORGANIZATION|PRACTICE DIRECTIONS|COURT OF APPEAL'),
    ('Asset recovery (POCAMLA)', 'new',      r'PROCEEDS OF CRIME|MONEY\s*-?\s*LAUNDERING|ASSETS RECOVERY|PRESERVATION ORDER|FORFEITURE'),
    ('Public finance',           'new',      r'PUBLIC FINANCE MANAGEMENT|EXCHEQUER|NATIONAL TREASURY|APPROPRIATION|STATEMENT OF ACTUAL|UNCLAIMED FINANCIAL'),
    ('Company_Registrations',    'existing', r'COMPANIES\s*ACT|INSOLVENCY|CO\s*-?\s*OPERATIVE SOCIETIES|PARTNERSHIPS|TRANSFER OF BUSINESSES|BUSINESS NAMES|COMPETITION ACT'),
    ('Licensing',                'existing', r'MINING|LICEN[CS]|INFORMATION AND COMMUNICATIONS|ALCOHOLIC DRINKS|BETTING|TOURISM ACT|PERMIT'),
    ('Public holidays & notices of state', 'new', r'PUBLIC HOLIDAYS|NATIONAL FLAG|PRESIDENTIAL|STATE HOUSE|PUBLIC ORDER'),
    ('Health',                   'new',      r'HEALTH\s*ACT|HEALTH SERVICES|PHARMACY|MEDICAL|HOSPITAL|MENTAL HEALTH'),
    ('Education',                'new',      r'EDUCATION|EXAMINATIONS COUNCIL|TEACHERS SERVICE|KNEC|TVET'),
    ('Roads & transport',        'new',      r'ROADS|TRAFFIC|RAILWAY|AVIATION|MARITIME|NATIONAL TRANSPORT'),
]


def load(years):
    rows = []
    for y in years:
        p = os.path.join(REPO, 'reports', 'corpus', y, 'notices.csv')
        for r in csv.DictReader(open(p, encoding='utf-8')):
            if r['source'] != 'inspector':
                continue
            r['year'] = y
            rows.append(r)
    return rows


def home(r):
    h = re.sub(r'\s+', ' ', (r['act'] + ' | ' + r['subject']).upper())
    for name, kind, pat in RULES:
        if re.search(pat, h):
            return name, kind
    return None, None


def main(years):
    rows = load(years)
    misc = [r for r in rows if r['python_cat'] == 'Miscellaneous' or r['java_cat'] == '(AI triage)']
    lines = ['Miscellaneous / AI-triage review, years %s' % ', '.join(years),
             'pipeline notices %d | Misc or AI triage %d (%.1f%%)' % (len(rows), len(misc), 100.0 * len(misc) / len(rows)), '']
    placed = defaultdict(list)
    for r in misc:
        placed[home(r)].append(r)
    lines.append('== proposed homes for Misc / AI-triage notices (heading rules)')
    lines.append('%-40s %-9s %6s  %-44s %s' % ('home', 'category', 'notices', 'currently filed (python cat)', 'examples'))
    for (name, kind), rs in sorted(placed.items(), key=lambda kv: -len(kv[1])):
        cur = Counter(r['python_cat'] for r in rs).most_common(3)
        lines.append('%-40s %-9s %6d  %-44s %s' % (name or '(no rule - stays Miscellaneous)', kind or '', len(rs),
                     ', '.join('%s %d' % c for c in cur)[:44],
                     ', '.join('%s/%s/%s' % (r['year'], r['slug'], r['notice']) for r in rs[:4])))
    # precision of each rule over ALL notices: what else does it catch?
    lines += ['', '== each rule over ALL notices: where its matches are filed today (a rule that catches many',
              '   templated land/probate notices would be unsafe as a category signal)']
    for name, kind, pat in RULES:
        hit = [r for r in rows if re.search(pat, re.sub(r'\s+', ' ', (r['act'] + ' | ' + r['subject']).upper()))]
        if hit:
            c = Counter(r['python_cat'] for r in hit).most_common(4)
            lines.append('%-40s matches %5d  filed: %s' % (name, len(hit), ', '.join('%s %d' % x for x in c)))
    # what is left in Misc
    rest = placed[(None, None)]
    lines += ['', '== left in Miscellaneous after the rules: %d, by heading' % len(rest)]
    by = Counter()
    ex = defaultdict(list)
    for r in rest:
        k = (r['act'] or r['subject'])[:70]
        by[k] += 1
        if len(ex[k]) < 3:
            ex[k].append('%s/%s/%s' % (r['year'], r['slug'], r['notice']))
    for k, c in by.most_common(45):
        lines.append('%4d  %-70s %s' % (c, k, ', '.join(ex[k])))
    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    open(os.path.join(REPO, 'reports', 'eval', 'misc_review.txt'), 'w', encoding='utf-8').write('\n'.join(lines) + '\n')
    print('\n'.join(lines[:40]))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2022', '2023', '2024', '2026'])
