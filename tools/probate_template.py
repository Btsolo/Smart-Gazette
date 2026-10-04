"""
Rule-based extractor for probate (Court_Legal) notices.

Replaces an LLM extraction call for the ~92% of cause blocks that follow the
standard Kenyan probate grammar. Returns None when any required field is
missing, so the caller can fall back to the AI path for irregular notices.
"""
import re
import os, importlib.util
_ns = importlib.util.spec_from_file_location('names', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'names.py'))
NAMES = importlib.util.module_from_spec(_ns); _ns.loader.exec_module(NAMES)
import os, importlib.util as _il
_pn = _il.spec_from_file_location("proper_nouns", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proper_nouns.py"))
PN = _il.module_from_spec(_pn); _pn.loader.exec_module(PN)

# The standard grammar, in pieces so each part can fail independently:
#   CAUSE NO. <ref> OF <year>
#   By <petitioners>, [of <address>,] [the <relationship>,]
#   [through Messrs. <advocates>, advocates,]
#   for a <action> to the estate of <deceased>, [late of <residence>,]
#   who died [at <place>] on <date>.

# The optional parenthetical absorbs "(FORMERLY E285 OF 2024)" and the
# stray ")" left by cases whose formerly-clause was split across lines.
RE_CAUSE = re.compile(
    r'CAUSE NO\.\s*(?P<case_ref>[A-Z]?\s*\d+)\s*OF\s*(?P<case_year>\d[\d\s]{2,5}?)\s*'
    r'(?:\([^)]*\)|\))?\s*'
    r'By\s+(?P<body>.+?)(?=\s*(?:CAUSE NO\.|GAZETTE NOTICE NO\.|$))',
    re.S)

# Connectors to the deceased's name: "to the estate of" (most notices), "will
# of" (probate of a will names the testator) and their Gazette variants.
# Fix 4 (2022-2026 measured): "for grant" without a/the, "of written will"
# without "the" (~280 blocks a year), "estate X" without "of", and other
# grant types (ad litem, ad colligenda bona, with the will annexed, limited).
RE_ACTION = re.compile(
    r'for\s+(?:a\s+|the\s+)?(?P<action>'
    r'(?:limited\s+)?grant\s+of\s+letters\s+of\s+administration'
    r'(?:\s+(?:intestate|testate|with\s+(?:the\s+)?will\s+annexed|ad\s+colligenda\s+bona|ad\s+litem))?|'
    r'grant\s+of\s+probate|'
    r'resealing\s+of\s+(?:a\s+|the\s+)?grant[a-z\s]*?|'
    r'limited\s+grant[a-z\s]*?'
    # "probate of written will to the estate of X" (285 blocks, 2022-2026,
    # none read until a Java unit test written from a real notice shape hit it)
    r')\s*(?:of\s+(?:the\s+)?(?:last\s+)?(?:written\s+|oral\s+)?will(?:\s+and\s+testament)?\s+(?:to\s+the\s+estate\s+)?of'
    r'|to\s+the\s+estate(?:\s+of)?'
    r'|in\s+respect\s+of\s+the\s+estate\s+of'
    r'|(?:written|last)?\s*will\s+of)\s+(?P<tail>.+)$',
    re.S | re.I)

# Place and date of death are both optional. Forms seen in 2022-2026:
#   who died at X on 7th May, 2020   | who died on 7th May, 2020
#   who died in/along/near X, on ... | who died at X, 6th April, 1998 (no "on")
#   who died at X in 1978 (year only)| who died at X.  (no date in the notice)
# A missing date is stored as None rather than rejecting the whole record:
# the schema does not require it and the article generator handles it.
_DATE = r'\d{1,2}\s*(?:st|nd|rd|th)?\s*[A-Za-z]+\s*,?\s*\d{4}'
RE_DEATH = re.compile(
    r'^(?P<deceased>.+?)'
    r'(?:\s*,?\s*late\s+of\s+(?P<residence>.+?))?'
    r'[,\s]*who\s+died\s*(?:(?:at|in|along|near|on\s+the)\s+(?P<place>.+?)|(?P<there>there))??'
    r'(?:[,\s]*(?:on|n)?\s*(?P<date>' + _DATE + r')'
    r'|(?:\s+in)?\s+(?P<year>(?:18|19|20)\d\d)(?=\s*[.,]?\s*(?:\n|$))'
    r'|\s*(?=\.\s*(?:\n|$)|\n|$))',
    re.S | re.I)

# "(Formerly CAUSE NO. ...)" / "(as consolidated with CAUSE NO. ...)" quote a
# second cause number inside the header; splitting there cut the record in two.
_INNER_CAUSE = re.compile(r'(\((?:\s*formerly|\s*as\s+consolidated\s+with)\s*)CAUSE\s+NO\.', re.I)


def split_causes(notice):
    """CAUSE NO. blocks of a probate notice, not split at a cause number quoted
    inside a parenthesis."""
    t = _INNER_CAUSE.sub(lambda m: m.group(1) + 'CAUSE NO.', notice)
    return [b.replace('CAUSE NO.', 'CAUSE NO.') for b in re.split(r'(?=CAUSE NO\.)', t) if b.startswith('CAUSE NO.')]

RE_COURT    = re.compile(r'IN\s+THE\s+(?P<court>(?:HIGH\s*COURT|CHIEF\s*MAGISTRATE|SENIOR\s*PRINCIPAL\s*MAGISTRATE|PRINCIPAL\s*MAGISTRATE|SENIOR\s*RESIDENT\s*MAGISTRATE|RESIDENT\s*MAGISTRATE)[^\n]{0,60}?)\s*(?:PROBATE|\n)', re.I)
RE_DEADLINE = re.compile(r'within\s+(?P<deadline>[a-z\-]+\s*\(\s*\d+\s*\)\s*days'
                         r'(?:\s+from\s+the\s+date\s+of\s+publication)?)', re.I)
# The firm ends at ", advocates" - or, as most notices are printed, at
# " of <Town>, for a grant" / ", for a grant" with no "advocates" at all
# (advocate_firm was filled for only 3-5% of records before fix 4).
# re.S: the firm's initials can be split over a line ("A. B." / "Example & Co.").
RE_ADVOCATE = re.compile(
    r'through\s+(?:Messrs\.?|M/s\.?)?\s*(?P<advocates>.+?)'
    r'(?:,?\s+advocates?\b'
    r'|,?\s+(?:of|in)\s+[A-Z][A-Za-z\'-]+(?:\s+[A-Z][A-Za-z\'-]+)?\s*,?\s*(?=for\b)'
    r'|,\s*(?=for\s+(?:a\s+|the\s+)?(?:grant|resealing|limited|confirmation)))', re.I | re.S)
RE_ADDRESS  = re.compile(r'(?:all\s+of|of)\s+(?P<address>P\.?\s*O\.?\s*Box[^,]*(?:,\s*[^,]*)?)', re.I)
RE_RELATION = re.compile(r"the\s+deceased'?s?\s+(?P<relationship>[a-z\s\-]+?)\s*,", re.I)

def _t(s):
    return _tidy(s)

def _tidy(s):
    if not s: return None
    s = re.sub(r'\s+', ' ', s).strip(' ,.')
    # the fragment-joiner sometimes glues an ordinal to a month ("15thMarch")
    s = re.sub(r'(\d(?:st|nd|rd|th))([A-Z])', r'\1 \2', s)
    s = re.sub(r'([a-z])([A-Z])(?=[a-z])', r'\1 \2', s) if re.search(r'\d', s) else s
    # drop quantifiers the name-splitter can trail ("... Kasi, both")
    s = re.sub(r'[,\s]+(both|all)$', '', s, flags=re.I).strip(' ,.')
    return s or None

def _names(raw):
    """Split '(1) A, (2) B and (3) C' into a list, then hand each name to the
    shared proper-noun cleaner (see names.py)."""
    if not raw: return []
    raw = re.sub(r'\(\d+\)', '|', raw)
    raw = re.sub(r'\s+and\s+', '|', raw)
    cleaned, _ = NAMES.clean_names(re.split(r'[|]', raw))
    return cleaned

def extract(block, notice=None):
    """block  = one CAUSE NO. segment.
    notice = the full notice text, if available. The court name and the
    objection deadline are stated once per notice (in the header and the
    closing paragraph), not inside each cause block, so they are read from
    the notice when it is supplied."""
    ctx = notice if notice else block
    m = RE_CAUSE.search(block)
    if not m: return None
    body = m.group('body')

    a = RE_ACTION.search(body)
    if not a: return None
    tail = a.group('tail')

    d = RE_DEATH.search(tail)
    if not d: return None

    # petitioner segment is everything before the action clause
    pet_seg = body[:a.start()]
    # searched in the whole body: the firm often ends at ", for a grant", which
    # lies just past pet_seg - but the match must start before the action clause
    adv = RE_ADVOCATE.search(body)
    if adv and adv.start() >= a.start():
        adv = None
    addr = RE_ADDRESS.search(pet_seg)
    rel = RE_RELATION.search(pet_seg)
    rel_text = _tidy(rel.group('relationship')) if rel else None
    # "the executor(s) named in the deceased's last will" -> executor(s)
    ex = re.search(r'the\s+(executors?|executrix)\s+named', pet_seg, re.I)
    if ex and (not rel_text or re.search(r'will$', rel_text, re.I)):
        rel_text = ex.group(1).lower()

    # Names run from the start up to whichever marker comes first. The
    # ", of " fallback catches overseas addresses that are not P.O. Boxes
    # (e.g. "of 180 Hale Lane, Edgware Middlesex, United Kingdom").
    cuts = [x.start() for x in (adv, addr, rel) if x]
    fb = re.search(r',\s*(?:both\s+|all\s+)?of\s+', pet_seg)
    if fb: cuts.append(fb.start())
    cut = min(cuts) if cuts else len(pet_seg)
    names = _names(pet_seg[:cut])

    # Field names follow schemas/court_legal.json exactly.
    act = _t(a.group('action')).lower()
    out = {
        'court_name':              _t(RE_COURT.search(ctx).group('court')) if RE_COURT.search(ctx) else None,
        'case_reference':          _tidy(re.sub(r'\s', '', m.group('case_ref')) + ' OF ' + re.sub(r'\s','',m.group('case_year'))),
        'notice_subtype':          act.title(),
        'deceased_name':           PN.repair_name(_tidy(d.group('deceased'))),
        'date_of_death':           _tidy(d.group('date')) or _tidy(d.group('year')),
        'place_of_death':          _tidy(d.group('place')) if d.group('place') else (_tidy(d.group('residence')) if d.group('there') else None),
        'deceased_residence':      _tidy(d.group('residence')),
        'petitioner_names':        PN.repair_names(names),
        'petitioner_relationship': rel_text,
        'action_type':             act,
        'filing_deadline':         _t(RE_DEADLINE.search(ctx).group('deadline')) if RE_DEADLINE.search(ctx) else None,
        'judgment_summary':        None,
        'advocate_firm':           _tidy(adv.group('advocates')) if adv else None,
    }
    # required fields (date of death is optional since fix 4: ~200 notices a
    # year print none, and a year-only date is kept as the year)
    if not (out['deceased_name'] and out['petitioner_names']):
        return None
    # a deceased "name" that swallowed the clause is a mis-parse, not a name
    if len(out['deceased_name']) > 80 or re.search(r'\bwho\s+died\b', out['deceased_name'], re.I):
        return None
    return out

if __name__ == '__main__':
    import sys, json
    res = open(sys.argv[1], encoding='utf-8', errors='replace').read()
    blocks = [b for b in re.split(r'(?=CAUSE NO\.)', res) if b.startswith('CAUSE NO.')]
    if not blocks:
        print('no CAUSE NO. blocks found - this gazette has no probate notices.')
        print('(nothing for the probate template to do; other categories are unaffected)')
        sys.exit(0)

    ok, fail = [], []
    for b in blocks:
        r = extract(b)
        (ok if r else fail).append(b)
    print('blocks: %d | template-extracted: %d (%.1f%%) | fallback to AI: %d'
          % (len(blocks), len(ok), 100*len(ok)/len(blocks), len(fail)))
    if ok:
        print()
        print('--- sample extraction ---')
        print(json.dumps(extract(ok[0]), indent=2))
    if fail:
        print()
        print('--- first 2 failures (would go to AI) ---')
        for b in fail[:2]: print(repr(b[:230])); print()
