"""
Rule-based extractor for probate (Court_Legal) notices.

Replaces an LLM extraction call for the ~92% of cause blocks that follow the
standard Kenyan probate grammar. Returns None when any required field is
missing, so the caller can fall back to the AI path for irregular notices.
"""
import re

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

# Two connectors lead to the deceased's name: "to the estate of" (most
# notices) and "will of" (probate-of-will notices, which name the testator
# directly rather than the estate).
RE_ACTION = re.compile(
    r'for\s+(?:a|the)\s+(?P<action>'
    r'grant\s+of\s+letters\s+of\s+administration(?:\s+intestate|\s+testate)?|'
    r'grant\s+of\s+probate(?:\s+of\s+the\s+(?:last\s+)?(?:written\s+)?will(?:\s+and\s+testament)?)?|'
    r'resealing\s+of\s+a\s+grant[a-z\s]*?'
    r')\s*(?:to\s+the\s+estate\s+of|(?:written|last)?\s*will\s+of)\s+(?P<tail>.+)$',
    re.S | re.I)

# Place of death is optional ("who died on 23rd July, 2024" is common) and
# the comma before "who" is not always present after the extractor's joins.
RE_DEATH = re.compile(
    r'^(?P<deceased>.+?)'
    r'(?:\s*,?\s*late\s+of\s+(?P<residence>.+?))?'
    r'[,\s]*who\s+died\s*(?:at\s+(?P<place>.+?)|(?P<there>there))?'
    r'[,\s]*on\s+(?P<date>\d{1,2}\s*(?:st|nd|rd|th)?\s*[A-Za-z]+\s*,?\s*\d{4})',
    re.S | re.I)

RE_COURT    = re.compile(r'IN\s+THE\s+(?P<court>(?:HIGH\s*COURT|CHIEF\s*MAGISTRATE|SENIOR\s*PRINCIPAL\s*MAGISTRATE|PRINCIPAL\s*MAGISTRATE|SENIOR\s*RESIDENT\s*MAGISTRATE|RESIDENT\s*MAGISTRATE)[^\n]{0,60}?)\s*(?:PROBATE|\n)', re.I)
RE_DEADLINE = re.compile(r'within\s+(?P<deadline>[a-z\-]+\s*\(\s*\d+\s*\)\s*days'
                         r'(?:\s+from\s+the\s+date\s+of\s+publication)?)', re.I)
RE_ADVOCATE = re.compile(r'through\s+(?:Messrs\.?\s*)?(?P<advocates>.+?),\s*advocates?', re.I)
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
    """Split '(1) A, (2) B and (3) C' or 'A and B' into a list."""
    if not raw: return []
    raw = re.sub(r'\(\d+\)', '|', raw)
    raw = re.sub(r'\s+and\s+', '|', raw)
    parts = [p for p in re.split(r'[|]', raw)]
    return [_tidy(p) for p in parts if _tidy(p)]

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
    adv = RE_ADVOCATE.search(pet_seg)
    addr = RE_ADDRESS.search(pet_seg)
    rel = RE_RELATION.search(pet_seg)

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
        'deceased_name':           _tidy(d.group('deceased')),
        'date_of_death':           _tidy(d.group('date')),
        'place_of_death':          _tidy(d.group('place')) if d.group('place') else (_tidy(d.group('residence')) if d.group('there') else None),
        'deceased_residence':      _tidy(d.group('residence')),
        'petitioner_names':        names,
        'petitioner_relationship': _tidy(rel.group('relationship')) if rel else None,
        'action_type':             act,
        'filing_deadline':         _t(RE_DEADLINE.search(ctx).group('deadline')) if RE_DEADLINE.search(ctx) else None,
        'judgment_summary':        None,
        'advocate_firm':           _tidy(adv.group('advocates')) if adv else None,
    }
    # required fields
    if not (out['deceased_name'] and out['date_of_death'] and out['petitioner_names']):
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
