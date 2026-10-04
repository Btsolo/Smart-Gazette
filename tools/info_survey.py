"""
Survey (Phase 4c): kinds of information inside notice TEXT that no template
or category captures today. Counts notices (2022-2026, cleaned cache) holding
each kind, by category, with examples.

Usage: python tools/info_survey.py > reports/corpus/info_kinds.txt
"""
import collections, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import category_eval as E, category_census as C

F = re.I
KINDS = collections.OrderedDict([
    ('coordinates (UTM / Arc 1960 / lat-long)', re.compile(r'\bUTM\b|Arc\s*1960|\bEastings?\b|\bNorthings?\b|\blatitude\b|\blongitude\b|\d+\s*°\s*\d+\s*[\'′]', F)),
    ('figure / map / chart referenced', re.compile(r'\b(?:Figure|Fig\.)\s*\d|\bmap\b|\bchart\b|\bdiagram\b|\bphotograph', F)),
    ('images prescribed / symbol', re.compile(r'\bimages?\b|\bsymbol\b|\blogo\b|\bemblem\b', F)),
    ('web address / email', re.compile(r'https?://|www\.[a-z0-9-]+\.[a-z]|[\w.-]+@[\w-]+\.[a-z]{2,}', F)),
    ('telephone number', re.compile(r'\b(?:Tel|Telephone|Phone|Mobile|Cell)\b[.:]?\s*[+\d(]|\+254\s?\d|\b07\d{2}\s?\d{3}\s?\d{3}\b', F)),
    ('money amounts (KSh / US$)', re.compile(r'\b(?:KSh|Kshs?|KES|Ksh\.)\s?\d|US\s?\$\s?\d|\bshillings\b', F)),
    ('percentages / rates', re.compile(r'\d\s?%|\bper\s?cent\b', F)),
    ('meeting / hearing / auction: date + venue', re.compile(r'\b(?:will be held|shall be held|public hearing|public auction|meeting will|annual general meeting|AGM)\b', F)),
    ('deadline / objection period', re.compile(r'within\s+(?:\w+\s+)?\(\d+\)\s+days|on or before the|not later than', F)),
    ('forms to fill / apply', re.compile(r'\bFORM\s+[A-Z0-9]{1,4}\b|\bapplication form\b|\bprescribed form\b', F)),
    ('footnote / asterisk reference', re.compile(r'(?m)^\s*\*+\s*G\.?\s*N\.?|\*\s*G\.N\.', F)),
    ('ID / passport / KRA PIN numbers', re.compile(r'\bID\s*(?:No\.?|Number)?\s*\d{6,9}\b|\bPassport\s*No|\b[AP]\d{9}[A-Z]\b', F)),
    ('vehicle registrations / chassis', re.compile(r'\bK[A-Z]{2}\s?\d{3}[A-Z]\b|\bchassis\b', F)),
    ('land parcel references', re.compile(r'\b[A-Z][A-Za-z]+/[A-Za-z]+(?:/[A-Za-z0-9 ]+)?/\d+\b|\bL\.?R\.?\s*No\.?\s*\d', F)),
    ('court case numbers', re.compile(r'\b(?:Petition|Misc\.?\s*Application|Civil\s+Suit|Cause|E)\s*No\.?\s*E?\d+\s*of\s*\d{4}', F)),
    ('gazette cross-references', re.compile(r'Gazette\s+Notice\s+Nos?\.?\s*\d+', F)),
    ('Legal Notice / Act / Bill references', re.compile(r'\bLegal\s+Notice\s+No\.?\s*\d+|\bL\.N\.\s*\d+|\bBill,?\s+\d{4}', F)),
    ('lists of people with roles (committees, boards)', re.compile(r'\b(?:Chairperson|Secretary|Member)\s*[-–]', F)),
    ('statistics / financial statements', re.compile(r'\b(?:Statement of|Balance Sheet|Revenue|Expenditure|Exchequer|Surplus|Deficit)\b', F)),
    ('tariffs / fees / charges schedules', re.compile(r'\b(?:tariff|levy|fees?|charges?)\b.*\d', F)),
    ('environment / EIA', re.compile(r'Environmental\s+Impact\s+Assessment|\bEIA\b|\bNEMA\b', F)),
    ('wildlife / species / plants', re.compile(r'\bspecies\b|\bvariet(?:y|ies)\b|\bwildlife\b', F)),
])


def main():
    count = collections.Counter()
    by_cat = collections.defaultdict(collections.Counter)
    ex = collections.defaultdict(list)
    total = 0
    for y in ['2022', '2023', '2024', '2025', '2026']:
        for slug, num, t in E.cleaned_notices(y):
            total += 1
            cat = C.categorise(t)
            body = t.split('\n', 1)[1] if '\n' in t else ''     # not the notice's own header
            for k, rx in KINDS.items():
                m = rx.search(body)
                if m:
                    count[k] += 1
                    by_cat[k][cat] += 1
                    if len(ex[k]) < 3 and total % 7 == 0:
                        s = max(0, m.start() - 60)
                        ex[k].append('%s %s %d: ...%s...' % (y, slug, num, re.sub(r'\s+', ' ', body[s:m.end() + 60])))
    print('notices', total)
    for k in KINDS:
        print('\n## %-50s %6d notices (%.1f%%)' % (k, count[k], 100.0 * count[k] / total))
        print('   categories:', dict(by_cat[k].most_common(5)))
        for e in ex[k]:
            print('   e.g.', e[:220])


if __name__ == '__main__':
    main()
