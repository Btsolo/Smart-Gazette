"""
Rule-based extractor for change-of-name (deed poll) notices.
Spec: docs/specs/change-of-name-template.md

The notice is one fixed formula (99% of 1,550 notices, 2022-2026):
  NOTICE is given that by a deed poll dated <date>, duly executed and
  registered in the Registry of Documents at <place> as Presentation No. <n>,
  in Volume <v>, Folio <f>, File No. <file>, by our client(s) / by me,
  <applicants>, of <address> in the Republic of Kenya, [on behalf of <minor>
  (minor),] formerly known as <former>, formally and absolutely renounced and
  abandoned the use of his/her former name <former> and in lieu thereof
  assumed and adopted the name <assumed>, for all purposes ...
  <FIRM>, Advocates for <person>.

Returns None for anything else (company / association renamings, year-of-birth
corrections) or when the two printed statements of the former name disagree,
so the caller falls back to the AI path. Field names follow
schemas/field/change_of_name.json (including the fields added for the
collective table). Java port: ChangeOfNameTemplate (keep in sync).
"""
import os, re, sys
import importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
_ns = importlib.util.spec_from_file_location('names', os.path.join(HERE, 'names.py'))
NAMES = importlib.util.module_from_spec(_ns); _ns.loader.exec_module(NAMES)

F = re.I | re.S
# A deed poll notice says "deed poll"; its date is read when it is printed
# cleanly. Misprinted dates ("16th September August, 2026", "27th October
# Nairobi, 2023") leave deed_poll_date empty - the notice is still read (spec 4).
RE_DEED_POLL = re.compile(r'\bdeed[\s-]*poll\b', F)
RE_DEED = re.compile(r'deed\s*poll\s*(?:dated\s*)?,?\s*(?:the\s*)?(?P<date>\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:day\s+of\s+)?[A-Za-z]+\s*,?\s*\d{4})', F)
# A name ends at "for all purposes" / "authorizes", at a line end, or at a
# comma followed by a lowercase word (the clause goes on). NOT at ". ": that
# cut "Dr. Charles ...", "E. M. Stephen", "St. Claire ..." to "Dr", "E", "St".
# "the name" is sometimes missing ("adopted the Khadija ...") or glued
# ("the nameShabir ...").
NAME_END = r'(?=for\s+all\s+purposes|for\s+all|and\s+authori[sz]es|,\s*[a-z]|\.?\s*\n|\.?\s*$)'
# "assumed and re-adopted" and "in lieu thereof adopted" also occur (held-out No 166)
RE_ASSUMED = re.compile(r'(?:assumed\s+and|lieu\s+thereof)\s+(?:re-?)?adopted\s+the\s*(?:names?\s*)?(?:of\s+)?(?P<name>[A-Z].+?)\s*,?\s*' + NAME_END, re.S)
# the place is case-SENSITIVE: case-insensitive, "at Nairobi as Presentation"
# gave the registry "Nairobi as" (found in the 50-record review)
RE_REGISTRY = re.compile(r'(?i:Registry\s+of\s+Documents\s+(?:at|in))\s+(?P<v>[A-Z][a-z]+)', re.S)
RE_PRES = re.compile(r'Presentation\s+No\.?\s*(?P<v>\d+[\w/]*)', F)
RE_VOL = re.compile(r'Volume\s+(?P<v>[A-Z]{1,2}\s?\d*|\d+)\b', F)
RE_FOLIO = re.compile(r'Folio\.?\s*(?P<v>\d+(?:\s*/\s*\d+)?)', F)
RE_FILE = re.compile(r'File\s+(?:No\.?\s*)?(?P<v>[A-Z0-9][A-Z0-9/\-]*)\s*,', F)
RE_FILER = re.compile(r'\bby\s+(?P<who>our\s+clients?|my\s+clients?|me|us)\s*,?\s*(?P<applicants>.+?)\s*,?\s*'
                      r'(?=\bof\s+P\.?\s*O|\bformerly\s+known|\bon\s+behalf\s+of|\bin\s+the\s+Republic)', F)
RE_ADDRESS = re.compile(r'\bof\s+(?P<addr>P\.?\s*O\.?\s*Box\s*[^,]*,\s*[^,]*?)'
                        r'(?:\s+in\s+the\s+Republic\s+of\s+(?P<country>[A-Z][a-z]+))?\s*,', F)
RE_MINOR = re.compile(r'on\s+behalf\s+of\s+(?:the\s+)?(?:minor\s*,?\s*)?(?P<minor>.+?)\s*\(\s*(?:a\s+)?minors?\s*\)', F)
RE_FORMER1 = re.compile(r'formerly\s+known\s+as\s+(?P<name>.+?)\s*,?\s*'
                        r'(?=formally|and\s+absolutely|absolutely|renounced|who\s+has|has\s+formally|do\s+hereby)', F)
RE_FORMER2 = re.compile(r'former\s+names?\s+(?:of\s+)?(?P<name>.+?)\s*,?\s*(?=and\s+in\s+lieu|in\s+lieu)', F)
RE_FIRM = re.compile(r'(?P<firm>[A-Z][A-Z0-9&.,\'\-\s]{2,90}?)\s*,?\s*Advocates?\s+for\b', re.S)
RE_REPUBLIC = re.compile(r'in\s+the\s+Republic\s+of\s+(?P<c>[A-Z][a-z]+)', F)


def _t(s):
    if not s:
        return None
    s = re.sub(r'\s+', ' ', s).strip(' ,.;:')
    return s or None


def _key(s):
    return re.sub(r'[^a-z]', '', (s or '').lower())


def _former(raw):
    """A printed former name: without a leading "as" ("former name as X") and
    without a clause run on after a comma ("X, formally and absolutely ...")."""
    if not raw:
        return None
    raw = re.sub(r'^\s*(?:known\s+)?as\s+', '', raw, flags=re.I)
    return re.split(r',\s*(?=[a-z])', raw)[0]


def _name(raw):
    return NAMES.clean_name(_t(raw))[0] if raw else None


def _applicants(raw):
    if not raw:
        return []
    raw = re.sub(r'\(\s*(?:guardians?|parents?|mother|father)\s*\)', '', raw, flags=re.I)
    raw = re.sub(r'\(\d+\)', '|', raw)
    raw = re.sub(r'\s+and\s+', '|', raw)
    raw = re.sub(r'[,\s]+(?:both|all)$', '', raw.strip(), flags=re.I)
    names, _ = NAMES.clean_names([p for p in raw.split('|')])
    return names


def extract(notice):
    return explain(notice)[0]


def explain(notice):
    """(record, None) or (None, reason) - the reason names the step that refused."""
    deed = RE_DEED.search(notice)
    assumed = RE_ASSUMED.search(notice)
    if not RE_DEED_POLL.search(notice):
        return None, 'not a deed poll'
    if not assumed:
        return None, 'no assumed name'
    f1 = RE_FORMER1.search(notice)
    f2 = RE_FORMER2.search(notice)
    n1 = _former(f1.group('name')) if f1 else None
    n2 = _former(f2.group('name')) if f2 else None
    former_raw = n1 or n2
    # the two printed statements of the former name are a built-in witness:
    # different people ("Binal ..." / "Sospeter ...") -> refuse
    if n1 and n2 and _key(NAMES.clean_name(_t(n1))[0]) != _key(NAMES.clean_name(_t(n2))[0]):
        return None, 'former names disagree'
    former, extras = NAMES.clean_name(_t(former_raw)) if former_raw else (None, {})
    assumed_name = _name(assumed.group('name'))
    if not former:
        return None, 'no former name'
    if not assumed_name:
        return None, 'assumed name empty'
    for n in (former, assumed_name):
        if len(n) > 80 or re.search(r'\b(?:formerly|assumed|renounced|deed\s+poll)\b', n, re.I):
            return None, 'name check: %r' % n[:70]

    filer = RE_FILER.search(notice)
    who = filer.group('who').lower() if filer else ''
    minor = RE_MINOR.search(notice)
    addr = RE_ADDRESS.search(notice)
    rep = RE_REPUBLIC.search(notice)
    firm = None
    for m in RE_FIRM.finditer(notice):
        firm = _t(m.group('firm'))
    g = lambda rx: _t(rx.search(notice).group('v')) if rx.search(notice) else None
    return ({
        'former_name': former,
        'assumed_name': assumed_name,
        'aliases': extras.get('aliases', []),
        'person_address': _t(addr.group('addr')) if addr else None,
        'citizenship': rep.group('c') if rep else None,
        'deed_poll_date': _t(deed.group('date')) if deed else None,
        'registration_date': None,
        'advocate_firm': firm if who.startswith(('our', 'my')) else None,
        'registry': g(RE_REGISTRY),
        'presentation_number': g(RE_PRES),
        'volume': g(RE_VOL),
        'folio': re.sub(r'\s', '', g(RE_FOLIO)) if g(RE_FOLIO) else None,
        'file_number': g(RE_FILE),
        'filed_by': 'advocates' if who.startswith(('our', 'my')) else ('self' if who in ('me', 'us') else None),
        'applicants': _applicants(filer.group('applicants')) if filer else [],
        'on_behalf_of_minor': bool(minor),
        'notice_id': (re.match(r'\s*GAZETTE NOTICE NO\.\s*(\d+)', notice) or [None, None])[1],
    }, None)


if __name__ == '__main__':
    import random, collections
    sys.path.insert(0, HERE)
    import category_eval as E, category_census as C
    rows = [(y, n, t) for y in ['2022', '2023', '2024', '2025', '2026'] for s, n, t in E.cleaned_notices(y)
            if C.categorise(t) == 'Change_of_Name']
    deed = [r for r in rows if re.search(r'deed\s+poll', r[2], re.I) and re.search(r'assumed\s+and\s+adopted', r[2], re.I)]
    res = [(r, extract(r[2])) for r in rows]
    ok = [x for x in res if x[1]]
    print('change of name %d | deed-poll formula %d | read %d (%.1f%% of deed polls)'
          % (len(rows), len(deed), len(ok), 100.0 * len(ok) / max(1, len(deed))))
    fill = collections.Counter(k for _, r in ok for k, v in r.items() if v not in (None, [], False))
    print('fields filled:', ', '.join('%s %.0f%%' % (k, 100.0 * v / len(ok)) for k, v in fill.most_common()))
    random.seed(int(sys.argv[1]) if len(sys.argv) > 1 else 1)
    fails = [x[0] for x in res if not x[1] and x[0] in deed]

    why = lambda t: explain(t)[1].split(':')[0]
    reasons = collections.Counter(why(r[2]) for r in fails)
    print('not read, by reason:', dict(reasons))
    for r in random.sample(fails, min(6, len(fails))):
        f1, f2 = RE_FORMER1.search(r[2]), RE_FORMER2.search(r[2])
        print('--- not read [%s]' % explain(r[2])[1], r[0], r[1],
              '| f1=%r f2=%r' % (f1 and f1.group('name')[:60], f2 and f2.group('name')[:60]))
