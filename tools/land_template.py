"""
Rule-based extractor for land / title-replacement notices.

These are the single largest category in the Gazette (~60% of notices in the
tested corpus) and the most formulaic: a registered proprietor has lost a
title document, and the Registrar gives notice that a replacement will issue
unless an objection arrives within a stated period.

Patterns are whitespace-tolerant because the upstream fragment-joiner
occasionally drops or adds a space ("parcelof", "TITLEWHEREAS", "proprie tor").
Returns None when required fields are missing, so the caller falls back to AI.
"""
import re

FLAGS = re.I | re.S

def tol(word):
    """Whitespace-tolerant pattern for a word the joiner may have split
    ("proprietors" can arrive as "proprie tors" or "propriet ors")."""
    return r'\s*'.join(re.escape(ch) for ch in word)

RE_ACT   = re.compile(r'THE\s*LAND\s+(?:REGISTRATION|TITLES?)\s+ACT', FLAGS)
RE_SUBJ  = re.compile(r'ISSUE\s+OF\s+A\s+(?P<subject>(?:NEW\s+)?(?:PROVISIONAL\s+)?'
                      r'(?:LAND\s+)?(?:CERTIFICATE|TITLE|LEASE)[A-Z\s]*?)(?=WHEREAS|\s*$)', FLAGS)

# Address is optional - many notices name only the proprietor. The ownership
# clause ("in freehold ownership interest") is also optional and varies.
RE_PROP  = re.compile(
    r'WHEREAS\s+(?P<names>.+?)\s*,?\s*'
    r'(?:of\s+(?P<address>P\.?\s*O\.?\s*Box[^,]*(?:,\s*[^,]*?)?)\s*,?\s*)?'
    r'(?:in\s+the\s+Republic\s+of\s+Kenya\s*,?\s*)?'
    r'(?:is|are)?\s*(?:directors?\s+of\s+[^,]+,\s*)?' + tol('registered') +
    r'(?:\s+as\s+' + tol('proprietor') + r's?)?'
    r'(?P<tenure>.{0,70}?)\s*of\s*all\s*th(?:at|ose)', FLAGS)

# Property references appear in several shapes and positions:
#   "... known as L.R. No. 2177, situate ..."
#   "... situate in Kisumu County, known as Kisumu/Nyahera/2033, by virtue ..."
#   "... registered under the title No. Njoro/Ngata Block 2/2"
RE_LR    = re.compile(r'known\s+as\s+(?P<lr>.+?)\s*,\s*'
                      r'(?=containing|situate|by\s+virtue|and\s+whereas|' + tol('registered') + r')', FLAGS)
RE_LR2   = re.compile(tol('registered') + r'\s+under\s*(?:the\s*)?title\s+No\.?\s*'
                      r'(?P<lr>.+?)\s*(?:,\s*(?=and\s+whereas|by\s+virtue|containing|situate)|\.\s|$)', FLAGS)

RE_AREA  = re.compile(r'containing\s+(?P<area>[\d\.]+\s*(?:hectares?|acres?))', FLAGS)
# Location appears in several forms:
#   "situate in the Kitale Township in TransNzoia District"
#   "situate in the district of Nakuru"  /  "situate in the county of Kakamega"
#   "situate in Kisumu County"  /  "situate in Kiambu County"
RE_LOC   = re.compile(r'situate\s+in\s+(?:the\s+)?(?P<location>.+?)\s+in\s+(?:the\s+)?(?P<district>[\w\s\-]+?)\s*'
                      r'(?:District|Area|County)', FLAGS)
RE_LOC2  = re.compile(r'situate\s+in\s+the\s+(?:district|county)\s+of\s+(?P<district>[\w\s\-]+?)\s*(?:,|\.|$)', FLAGS)
RE_LOC3  = re.compile(r'situate\s+in\s+(?:the\s+)?(?P<district>[\w\s\-]+?)\s+(?:County|District)', FLAGS)
RE_INSTR = re.compile(r'by\s+virtue\s+of\s+a\s+(?P<instrument>[a-z\s]+?)\s*,', FLAGS)
RE_IR    = re.compile(tol('registered') + r'\s+as\s+(?:I\.?\s*R\.?|C\.?\s*R\.?)\s*(?P<ir>[\w/\-\.]+)', FLAGS)
RE_LOST  = re.compile(r'show\s+that\s+the\s+said\s*(?P<lost>.+?)\s*(?:issued\s+thereof\s*)?has\s+been\s+lost', FLAGS)
RE_DAYS  = re.compile(r'expiration\s+of\s+(?P<period>[a-z\-]+)\s*\(\s*(?P<days>\d+)\s*\)\s*days', FLAGS)
RE_REGISTRAR = re.compile(r'([A-Z][A-Za-z\.\s\']{3,40}?)\s*,\s*(?:MR\s*/?\s*\d+\s*)?(?:Land\s*|Deputy\s*|District\s*)?Registrar(?:\s*of\s*Titles)?', FLAGS)
RE_CAUSE_REF = re.compile(r'(?:succession\s+)?cause\s+no\.?\s*(?P<cause>[A-Z]?\d+\s*of\s*\d{4})', FLAGS)
RE_DATED = re.compile(r'Dated\s+the\s+(?P<dated>\d{1,2}\s*(?:st|nd|rd|th)?\s*[A-Za-z]+\s*,?\s*\d{4})', FLAGS)

def _t(s):
    if not s: return None
    s = re.sub(r'\s+', ' ', s).strip(' ,.')
    return s or None

def _names(raw):
    if not raw: return []
    raw = re.sub(r'\(\d+\)', '|', raw)
    raw = re.sub(r'\s+and\s+', '|', raw)
    out = []
    for p in raw.split('|'):
        p = _t(p)
        if p:
            p = re.sub(r'^(?:both|all)\s+', '', p, flags=re.I)
            out.append(p)
    return out

def extract(notice):
    """notice = full text of one GAZETTE NOTICE block."""
    if not RE_ACT.search(notice):
        return None
    p = RE_PROP.search(notice)
    if not p:
        return None

    lr    = RE_LR.search(notice) or RE_LR2.search(notice)
    loc   = RE_LOC.search(notice)
    loc2  = (RE_LOC2.search(notice) or RE_LOC3.search(notice)) if not loc else None
    instr = RE_INSTR.search(notice)
    ir    = RE_IR.search(notice, p.end())
    lost  = RE_LOST.search(notice)
    days  = RE_DAYS.search(notice)
    dated = RE_DATED.search(notice)
    subj  = RE_SUBJ.search(notice)

    # Field names follow schemas/land_property.json exactly, so the output
    # drops straight into the same pipeline slot as the AI extraction.
    doc  = _t(lost.group('lost')) if lost else None
    period = None
    if days:
        period = '%s (%s) days' % (_t(days.group('period')), days.group('days'))

    out = {
        'notice_subtype':          _t(subj.group('subject')) if subj else 'Issue of a Provisional Certificate',
        'is_ownership_change':     False,   # lost-document notices never transfer ownership
        'parcel_id':               _t(lr.group('lr')) if lr else None,
        'document_type':           doc,
        'parties':                 _names(p.group('names')),
        'new_owner':               None,
        'succession_cause_number': _t(RE_CAUSE_REF.search(notice).group('cause')) if RE_CAUSE_REF.search(notice) else None,
        'action_type':             'replacement of lost document',
        'county':                  (_t(loc.group('district')) if loc
                                    else (_t(loc2.group('district')) if loc2 else None)),
        'registrar_name':          _t(RE_REGISTRAR.search(notice).group(1)) if RE_REGISTRAR.search(notice) else None,
        'reference_docs':          _t(ir.group('ir')) if ir else None,
        'valuation':               _t(RE_AREA.search(notice).group('area')) if RE_AREA.search(notice) else None,
        'objection_period':        period,
    }
    if not (out['parties'] and out['parcel_id']):
        return None
    return out

if __name__ == '__main__':
    import sys, json, glob, os, importlib.util
    s = importlib.util.spec_from_file_location('G', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gazette_clean.py'))
    G = importlib.util.module_from_spec(s); s.loader.exec_module(G)

    files = sys.argv[1:] or sorted(glob.glob('/mnt/user-data/uploads/cleaned*.txt'))
    tot = ok = 0
    for f in files:
        t = open(f, encoding='utf-8', errors='replace').read()
        notices = [n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', t) if n.startswith('GAZETTE NOTICE NO.')]
        land = [n for n in notices if RE_ACT.search(n) and re.search(r'has\s+been\s+lost', n, FLAGS)]
        if not land: continue
        got = sum(1 for n in land if extract(n))
        tot += len(land); ok += got
        print('%-26s land: %4d  extracted: %4d  (%.1f%%)' % (os.path.basename(f), len(land), got, 100*got/len(land)))
    if tot:
        print('\nTOTAL land notices: %d | extracted: %d (%.1f%%)' % (tot, ok, 100*ok/tot))
        for f in files:
            t = open(f, encoding='utf-8', errors='replace').read()
            for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', t):
                if n.startswith('GAZETTE NOTICE NO.') and extract(n):
                    print('\n--- sample ---'); print(json.dumps(extract(n), indent=2)); sys.exit(0)
