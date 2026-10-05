"""
Reference watch (docs/specs/reference-watch.md): notice, from what the system
reads, when reference data may be out of date - and say exactly what to do.
Watchers only raise flags; they never edit reference data.

Law watchers (reference/laws.json + the law files):
  law.missing_section      a notice cites section N of a law we hold; our copy has no section N
  law.amended_after_copy   a notice cites "<Law> (Amendment) Act, YEAR" / the "Statute Law
                           (Miscellaneous Amendments) Act, YEAR" naming a law we hold, YEAR after our copy
  law.amending_supplement  a Gazette Supplement: "AN ACT of Parliament to amend the <Law> Act"
  law.not_held             an Act we do not hold, cited (counted over the corpus, see main)
  law.old_copy             our copy's "text as at" date is more than 3 years old

  flags(text, gazette_date=None) -> [flag]      one notice (or one supplement)
  flag = {set, key, watcher, detail, message, action, evidence}

Java port: ReferenceWatch (keep in sync).
Usage: python tools/reference_watch.py [years]    (corpus: the flags audit.py section 8 lists)
"""
import collections, datetime, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import law_refs as L

OLD_COPY_YEARS = 5
NOT_HELD_MIN = 5
RE_AMENDMENT = re.compile(r"((?:[A-Z][A-Za-z'-]*\s+(?:and\s+|of\s+|on\s+)?){1,8})\(\s*Amendment\s*\)\s*Act,?\s*(\d{4})")
RE_MISC = re.compile(r"Statute\s+Law\s*\(\s*Miscellaneous\s+Amendments?\s*\)\s*Act,?\s*(\d{4})", re.I)
RE_SUPPLEMENT_AMEND = re.compile(r"AN\s+ACT\s+of\s+Parliament\s+to\s+amend\s+the\s+((?:[A-Z][A-Za-z'-]*\s+(?:and\s+|of\s+|on\s+)?){1,9}?Act)", re.I)
_catalog = None
_dismissed = None


def catalog():
    global _catalog
    if _catalog is None:
        _catalog = {l['key']: l for l in json.load(open(os.path.join(L.REF, 'laws.json'), encoding='utf-8'))['laws']}
    return _catalog


def dismissed():
    """(key, watcher, detail) the owner has looked at and dismissed (reference/watch_dismissed.json)"""
    global _dismissed
    if _dismissed is None:
        p = os.path.join(L.REF, 'watch_dismissed.json')
        doc = json.load(open(p, encoding='utf-8')) if os.path.exists(p) else {'dismissed': []}
        _dismissed = {(d['key'], d['watcher'], d['detail']) for d in doc['dismissed']}
    return _dismissed


def year_of(law_key):
    v = (catalog().get(law_key) or {}).get('version_date')
    return int(v[:4]) if v else None


def sentence_at(text, start, end):
    a = max(text.rfind('.', 0, start), text.rfind('\n', 0, start)) + 1
    b = min([i for i in (text.find('.', end), text.find('\n', end)) if i >= 0] or [len(text)])
    return re.sub(r'\s+', ' ', text[a:b + 1]).strip()[:300]


def flags(text, gazette_date=None):
    """the flags one notice (or supplement) raises"""
    out = []
    # 1. cited sections our copy does not have
    for r in L.refs(text):
        if r['law'].startswith('?') or r['found'] or r['provision'].endswith('SCHEDULE'):
            continue
        law = catalog().get(r['law'], {})
        m = re.search(r'\b' + re.escape(r['provision']) + r'\b', text)
        out.append({'set': 'law', 'key': r['law'], 'watcher': 'law.missing_section', 'detail': 's.' + r['provision'],
                    'message': 'A notice cites section %s of the %s; our copy (text as at %s) has no section %s.'
                               % (r['provision'], law.get('title', r['law']), law.get('version_date'), r['provision']),
                    'action': 'Check the current version on Kenya Law (%s); if it has section %s, save its PDF to raw/law/ '
                              'and run: python tools/build_law_reference.py pdfs raw/law' % (law.get('source_url') or 'kenyalaw.org', r['provision']),
                    'evidence': sentence_at(text, m.start(), m.end()) if m else ''})
    # 2. amendment Acts newer than our copy
    for m in RE_AMENDMENT.finditer(text):
        key = L.which_law(m.group(1).strip() + ' Act', 's')
        year = int(m.group(2))
        if key and not key.startswith('?') and year_of(key) and year > year_of(key):
            out.append(_amended(key, '%s (Amendment) Act, %d' % (catalog()[key]['title'][:-4], year), year, text, m))
    for m in RE_MISC.finditer(text):
        year = int(m.group(1))
        for key in L.heading_laws(text) + [r['law'] for r in L.refs(text)]:
            if not key.startswith('?') and year_of(key) and year > year_of(key):
                out.append(_amended(key, 'Statute Law (Miscellaneous Amendments) Act, %d' % year, year, text, m))
    # 3. a supplement that amends a law we hold
    for m in RE_SUPPLEMENT_AMEND.finditer(text[:3000]):
        key = L.which_law(m.group(1), 's')
        if key and not key.startswith('?'):
            law = catalog()[key]
            out.append({'set': 'law', 'key': key, 'watcher': 'law.amending_supplement', 'detail': m.group(1).strip(),
                        'message': 'A Gazette Supplement amends the %s; our copy is text as at %s.' % (law['title'], law['version_date']),
                        'action': 'When Kenya Law publishes the amended version, save its PDF to raw/law/ and rebuild '
                                  '(python tools/build_law_reference.py pdfs raw/law).',
                        'evidence': sentence_at(text, m.start(), m.end())})
    seen, unique = set(dismissed()), []
    for f in out:
        k = (f['key'], f['watcher'], f['detail'])
        if k not in seen:
            seen.add(k)
            unique.append(f)
    return unique


def _amended(key, amending, year, text, m):
    law = catalog()[key]
    return {'set': 'law', 'key': key, 'watcher': 'law.amended_after_copy', 'detail': amending,
            'message': 'A notice cites the %s; our copy of the %s is text as at %s, before it.' % (amending, law['title'], law['version_date']),
            'action': 'Save the current version of the %s from Kenya Law to raw/law/ and rebuild '
                      '(python tools/build_law_reference.py pdfs raw/law).' % law['title'],
            'evidence': sentence_at(text, m.start(), m.end())}


def old_copies(today=None):
    """law.old_copy flags: copies older than OLD_COPY_YEARS"""
    today = today or datetime.date.today()
    out = []
    for key, law in catalog().items():
        v = law.get('version_date')
        if v and (today - datetime.date.fromisoformat(v)).days > OLD_COPY_YEARS * 365 and (key, 'law.old_copy', v) not in dismissed():
            out.append({'set': 'law', 'key': key, 'watcher': 'law.old_copy', 'detail': v,
                        'message': 'Our copy of the %s is text as at %s (over %d years old).' % (law['title'], v, OLD_COPY_YEARS),
                        'action': 'Check Kenya Law for a newer version (%s).' % (law.get('source_url') or 'kenyalaw.org'),
                        'evidence': ''})
    return out


def amendment_flags():
    """law.amended_after_copy from the amending Acts saved in raw/law/ (reference/
    amendments.json): an amendment dated after our copy of a law it names"""
    p = os.path.join(L.REF, 'amendments.json')
    if not os.path.exists(p):
        return []
    out = []
    for a in json.load(open(p, encoding='utf-8'))['amendments']:
        for key in a['amends']:
            v = (catalog().get(key) or {}).get('version_date')
            if a['date'] and v and a['date'] > v and (key, 'law.amended_after_copy', a['title']) not in dismissed():
                law = catalog()[key]
                out.append({'set': 'law', 'key': key, 'watcher': 'law.amended_after_copy', 'detail': a['title'],
                            'message': 'The %s (%s) amends the %s; our copy is text as at %s, before it.'
                                       % (a['title'], a['date'], law['title'], v),
                            'action': 'Save the current version of the %s from Kenya Law to raw/law/ and rebuild '
                                      '(python tools/build_law_reference.py pdfs raw/law).' % law['title'],
                            'evidence': 'raw/law/' + a['file']})
    return out


def is_closed(flag):
    """a flag the library itself now answers (after a rebuild)"""
    law = L.law(flag['key'])
    if flag['watcher'] == 'law.missing_section':
        return bool(law and flag['detail'][2:] in law['provisions'])
    if flag['watcher'] == 'law.amended_after_copy':
        p = os.path.join(L.REF, 'amendments.json')
        dated = {a['title']: a['date'] for a in json.load(open(p, encoding='utf-8'))['amendments']} if os.path.exists(p) else {}
        if dated.get(flag['detail']):
            v = (catalog().get(flag['key']) or {}).get('version_date')
            return bool(v and v >= dated[flag['detail']])
    if flag['watcher'] in ('law.amended_after_copy', 'law.amending_supplement'):
        y = re.search(r'(\d{4})\s*$', flag['detail'])
        return bool(y and year_of(flag['key']) and year_of(flag['key']) >= int(y.group(1)))
    if flag['watcher'] == 'law.old_copy':
        return flag['detail'] != (catalog().get(flag['key']) or {}).get('version_date')
    return False


def main(years):
    import category_eval as E
    found, counts, not_held = {}, collections.Counter(), collections.Counter()
    for y in years:
        for slug, num, n in E.cleaned_notices(y):
            for f in flags(n):
                k = (f['key'], f['watcher'], f['detail'])
                counts[k] += 1
                found.setdefault(k, dict(f, first='%s %s %s' % (y, slug, num)))
            for r in L.refs(n):
                if r['law'].startswith('?'):
                    not_held[r['law'][1:]] += 1
    print('flags from notices: %d' % len(found))
    for k, f in sorted(found.items(), key=lambda kv: -counts[kv[0]]):
        print('  %-22s %-50s x%-3d %s\n      %s\n      evidence: %s' % (f['watcher'], f['key'] + ' ' + f['detail'], counts[k], f['first'], f['action'], f['evidence'][:160]))
    nh = [(a, c) for a, c in not_held.most_common() if c >= NOT_HELD_MIN]
    print('law.not_held (cited >= %d times): %d Acts - %s' % (NOT_HELD_MIN, len(nh), ', '.join('%s %d' % x for x in nh[:12])))
    af = amendment_flags()
    print('law.amended_after_copy from saved amending Acts: %d' % len(af))
    for f in af:
        print('  %-45s %s' % (f['key'], f['message']))
    oc = old_copies()
    print('law.old_copy: %d - %s' % (len(oc), ', '.join('%s (%s)' % (f['key'], f['detail']) for f in oc)))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2022', '2023', '2024', '2025', '2026'])
