"""
Finds the laws a notice cites and returns the provisions they point to, so
the app can show the notice next to the text it relies on.

  "Article 179 (2) (b) of the Constitution"            -> constitution, 179, (2)
  "Articles 10 and 232 of the Constitution"           -> constitution, 10 and 232
  "section 45 (1) of the County Governments Act, 2012" -> county_governments_act, 45, (1)
  "the Fourth Schedule to the Constitution"           -> constitution, schedule FOURTH

Laws come from src/main/resources/reference/<key>.json (tools/build_law_reference.py).
A citation of a law we do not hold yet is still returned with found=False, so
coverage can be measured and the next law to add can be chosen by count.

Java port: LawReferenceService (keep in sync).

Usage: python tools/law_refs.py 2022 2023 2024 2025 2026   (coverage report)
"""
import json, os, re, sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
REF = os.path.join(REPO, 'src', 'main', 'resources', 'reference')

# law key -> how notices name it (whitespace-tolerant: the joiner glues words),
# from the catalog reference/laws.json (tools/build_law_reference.py pdfs). The
# longest names are tried first, so "the Land Registration Act" is never read
# as a shorter title.
def _load_names():
    cat = json.load(open(os.path.join(REF, 'laws.json'), encoding='utf-8'))['laws']
    pairs = [(l['key'], n) for l in cat for n in l['names']]
    pairs.sort(key=lambda kv: -len(kv[1]))
    return pairs


LAW_NAMES = _load_names()

_NUM = r'\d+[A-Z]?(?:\s*\(\s*[0-9a-z]{1,4}\s*\))*'
# "Article 179 (2) (b)", "Articles 10, 27 and 232", "sections 30 (2) and 45"
RE_CITE = re.compile(
    r'\b(?P<kind>Articles?|Art\.|sections?|ss?\.)\s*(?P<list>' + _NUM +
    r'(?:\s*(?:,|and|or|&)\s*' + _NUM + r')*)'
    r'(?P<gap>[^.;]{0,60}?)\b(?:of|under|to|in)\s+(?P<law>[^.;\n]{0,70})', re.I)
RE_SCHED = re.compile(r'\b(?P<n>First|Second|Third|Fourth|Fifth|Sixth)\s+Schedule\s+(?:to|of)\s+(?P<law>(?:the\s*)?Constitution)', re.I)

_laws = {}


def law(key):
    if key not in _laws:
        p = os.path.join(REF, key + '.json')
        _laws[key] = json.load(open(p, encoding='utf-8')) if os.path.exists(p) else None
    return _laws[key]


def which_law(text, kind):
    for key, pat in LAW_NAMES:
        if re.match(r'\s*' + pat + r'\b', text, re.I):
            return key
    # an Act we do not hold: return its name for the coverage report
    m = re.match(r"\s*(?:the\s+)?((?:[A-Z][A-Za-z'\-]*\s+){1,9}?Act)\b", text)
    return ('?' + re.sub(r'\s+', ' ', m.group(1))) if m else None


def refs(notice):
    """[{law, provision, clause, label, title, found}] in order of first mention."""
    out, seen = [], set()

    def add(key, num, clause):
        if (key, num, clause) in seen:
            return
        seen.add((key, num, clause))
        doc = law(key) if not key.startswith('?') else None
        prov = doc['provisions'].get(num) if doc else None
        out.append({'law': key, 'provision': num, 'clause': clause,
                    'label': (doc['provision_label'] if doc else 'section') + ' ' + num + (clause or ''),
                    'title': prov['title'] if prov else None, 'found': prov is not None})

    for m in RE_CITE.finditer(notice):
        kind = m.group('kind').lower()
        key = which_law(m.group('law'), kind)
        if key is None:
            continue
        if kind.startswith('art') and key != 'constitution':
            continue                     # "Article 5 of the Treaty", an Act's own article
        if kind.startswith('s') and key == 'constitution':
            continue                     # "section 7 of the Sixth Schedule" - handled below
        for one in re.finditer(r'(\d+[A-Z]?)((?:\s*\(\s*[0-9a-z]{1,4}\s*\))*)', m.group('list')):
            clause = re.sub(r'\s+', '', one.group(2))
            first = re.match(r'\(\d+\)', clause)          # keep the sub-article, e.g. (2)
            add(key, one.group(1), first.group(0) if first else None)
    for m in RE_SCHED.finditer(notice):
        add('constitution', m.group('n').upper() + ' SCHEDULE', None)
    for r in out:                                         # schedules are looked up by title
        if r['law'] == 'constitution' and r['provision'].endswith('SCHEDULE'):
            s = next((s for s in law('constitution')['schedules'] if s['title'] == r['provision']), None)
            r.update(label=r['provision'].title(), title=s['subtitle'] if s else None, found=s is not None)
    return out


# where a notice's heading ends: its first operative words
RE_HEAD_END = re.compile(r'\b(?:WHEREAS|IN EXERCISE|PURSUANT|NOTICE is|TAKE NOTICE|IT IS NOTIFIED|IN PURSUANCE)\b')
_implied = None


def implied_rules():
    global _implied
    if _implied is None:
        doc = json.load(open(os.path.join(REF, 'implied.json'), encoding='utf-8'))
        _implied = [dict(r, _heading=re.compile(r['heading'], re.I), _text=re.compile(r['text'], re.I))
                    for r in doc['rules']]
    return _implied


def heading_of(notice):
    m = RE_HEAD_END.search(notice[:600])
    return notice[:m.start()] if m else notice[:400]


def heading_laws(notice):
    """keys of the laws named in the notice's heading, in order; a shorter name
    inside a longer one already matched is not counted"""
    head, spans, found = heading_of(notice), [], []
    for key, pat in LAW_NAMES:                       # longest first
        for m in re.finditer(pat + r'\b', head, re.I):
            if any(a < m.end() and m.start() < b for a, b in spans):
                continue
            spans.append((m.start(), m.end()))
            found.append((m.start(), key))
    out = []
    for _, key in sorted(found):
        if key not in out:
            out.append(key)
    return out


def laws_for(notice):
    """Everything the notice rests on, for the 'Laws Cited' tab and the article:
      kind 'cited'   - a provision the notice cites (refs)
      kind 'implied' - the section this kind of notice is issued under, when the
                       heading names the Act and no section of it is cited
                       (reference/implied.json)
      kind 'act'     - a law named in the heading with nothing more specific"""
    out = [dict(r, kind='cited') for r in refs(notice)]
    cited = {r['law'] for r in out}
    named = heading_laws(notice)
    for rule in implied_rules():
        if rule['law'] in named and rule['law'] not in cited and rule['_heading'].search(heading_of(notice)) \
                and rule['_text'].search(notice):
            doc = law(rule['law'])
            prov = doc['provisions'].get(rule['section']) if doc else None
            out.append({'law': rule['law'], 'provision': rule['section'], 'clause': rule['clause'],
                        'label': 'section ' + rule['section'] + (rule['clause'] or ''),
                        'title': prov['title'] if prov else None, 'found': prov is not None, 'kind': 'implied'})
            cited.add(rule['law'])
            break
    for key in named:
        if key not in cited:
            doc = law(key)
            out.append({'law': key, 'provision': None, 'clause': None, 'label': doc['title'] if doc else key,
                        'title': doc.get('citation') if doc else None, 'found': doc is not None, 'kind': 'act'})
            cited.add(key)
    return out


def provision_text(key, num, clause=None):
    """The text to display: the whole provision, or one clause of it."""
    doc = law(key)
    p = doc and doc['provisions'].get(num)
    if not p:
        return None
    if clause:
        c = next((c for c in p['clauses'] if c['ref'] == clause), None)
        if c:
            # the clause and its lettered paragraphs, which follow it indented
            lines = p['text'].split('\n')
            i = next(i for i, l in enumerate(lines) if l.startswith(clause))
            j = next((j for j in range(i + 1, len(lines)) if not lines[j].startswith(' ')), len(lines))
            return '\n'.join(lines[i:j])
    return p['text']


def main(years):
    sys.path.insert(0, HERE)
    import category_eval as E
    import category_census as CEN
    notices = with_ref = with_found = 0
    by_law, missing, county = Counter(), Counter(), Counter()
    for y in years:
        for slug, num, n in E.cleaned_notices(y):
            notices += 1
            r = refs(n)
            if not r:
                continue
            with_ref += 1
            with_found += any(x['found'] for x in r)
            if CEN.categorise(n) == 'County_Government':
                county['with_ref'] += 1
                county['found'] += any(x['found'] for x in r)
            for x in r:
                by_law[x['law'] if not x['law'].startswith('?') else '(not held)'] += 1
                if x['law'].startswith('?'):
                    missing[x['law'][1:]] += 1
                elif not x['found']:
                    missing['%s %s (number not in the law)' % (x['law'], x['provision'])] += 1
    print('notices %d | citing a provision: %d | at least one resolved: %d' % (notices, with_ref, with_found))
    print('County_Government notices citing a provision: %d | resolved: %d' % (county['with_ref'], county['found']))
    print('citations by law: ' + ', '.join('%s %d' % kv for kv in by_law.most_common()))
    print('\nmost-cited laws we do not hold yet (add next):')
    for k, v in missing.most_common(25):
        print('  %5d  %s' % (v, k))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2022', '2023', '2024', '2025', '2026'])
