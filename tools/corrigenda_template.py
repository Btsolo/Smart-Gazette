"""
Rule-based extractor for corrigenda (correction notices).

Corrigenda amend a previously published notice. They appear in most main
issues and follow a very regular grammar:

  IN Gazette Notice No. <ref> of <year>, Cause No. <cause> of <year>,
  amend the <thing> printed as "<old>" to read "<new>".

Returns None on anything that does not fit, so the caller falls back to AI.
"""
import re

RE_CAUSE = re.compile(r'CAUSE NO\.\s*(?P<cause>[A-Z]?\s*[\d\s]+?)\s*of\s*(?P<cause_year>[\d\s]{4,6}?)\s*,', re.I)
RE_REF   = re.compile(r'(?:IN\s+)?Gazette\s+Notice\s+No\.\s*(?P<ref>[\d\s]+?)\s*(?:of\s*(?P<ref_year>[\d\s]{4,6}?))?\s*[,\n]', re.I)
RE_AMEND = re.compile(
    r'amend\s*the\s+(?P<field>.+?)\s*printed\s+as\s*"\s*(?P<old>[^"]*?)\s*"'
    r'\s*to\s+read\s*"\s*(?P<new>[^"]*?)\s*"', re.I | re.S)

def _t(s):
    return re.sub(r'\s+', ' ', s).strip(' ,.') if s else None

def _digits(s):
    return re.sub(r'\s', '', s) if s else None

def extract(block, notice=None):
    """block = the CAUSE NO. segment; notice = the whole notice. The corrected
    notice is named BEFORE the cause ("IN Gazette Notice No. 5121 of 2022,
    CAUSE NO. ..."), outside the block - so it is searched in the notice too
    (amends_notice was always empty when only the block was searched)."""
    c = RE_CAUSE.search(block)
    a = RE_AMEND.search(block)
    if not (c and a):
        return None

    amendments = [{'field': _t(m.group('field')),
                   'printed_as': _t(m.group('old')),
                   'to_read': _t(m.group('new'))}
                  for m in RE_AMEND.finditer(block)]

    # (not the corrigendum's own header line "GAZETTE NOTICE NO. 2")
    body = re.sub(r'^\s*GAZETTE NOTICE NO\.[^\n]*\n', '', notice) if notice else None
    r = RE_REF.search(block) or (RE_REF.search(body) if body else None)
    return {
        'notice_subtype': 'Corrigendum',
        'cause_reference': '%s of %s' % (_digits(c.group('cause')), _digits(c.group('cause_year'))),
        'amends_notice': _digits(r.group('ref')) if r else None,
        'amends_notice_year': _digits(r.group('ref_year')) if r and r.group('ref_year') else None,
        'amendments': amendments,
    }

# --- corrections with no CAUSE NO. (fix 4) ---------------------------------
# Most numbered corrigenda are not probate corrections: appointment names,
# NLC land-acquisition schedules, IEBC "delete and insert" instructions.
# They share one shape: which notice is corrected, then what changes.
RE_TARGET = re.compile(
    r'(?:IN|in|further\s+to)\s+(?:the\s+)?(?:Kenya\s+)?Gaz+et+e\s+Notices?\s+Nos?\.?\s*(?P<ref>\d[\d\s]{0,6}\d|\d)'
    r'(?:\s*of\s*(?P<year>(?:19|20)\d\d))?', re.I)
# (Gaz+et+e: the source misspells it - "Gazzette" 2022/283/15995, "GAZETE" lesson 8)
# printed as "A" to read "B" - quotes are often missing on one side in the source
RE_AMEND_LOOSE = re.compile(
    r'amend\s*the\s+(?P<field>.+?)\s*printed\s+as\s*"?\s*(?P<old>[^"\n]+?)\s*"?\s*to\s+read\s*"?\s*(?P<new>[^"\n]+?)\s*"?\s*(?:\.|;|$)',
    re.I | re.M)
# numbered delete / insert / replace instructions (IEBC, schedules)
RE_INSTR = re.compile(r'^\s*\d{1,3}\.\s+(?P<text>[^\n]*\b(?:delete|insert|replace|substitute|amend)\b[^\n]*)', re.I | re.M)


def extract_notice(notice):
    """A correction notice without a CAUSE NO.: needs the corrected notice
    number and at least one stated change. Returns None otherwise (-> AI)."""
    t = RE_TARGET.search(notice)
    if not t:
        return None
    amendments = [{'field': _t(m.group('field')), 'printed_as': _t(m.group('old')), 'to_read': _t(m.group('new'))}
                  for m in RE_AMEND_LOOSE.finditer(notice)]
    if not amendments:
        amendments = [{'field': 'instruction', 'printed_as': None, 'to_read': _t(m.group('text'))}
                      for m in RE_INSTR.finditer(notice)]
    if not amendments:
        return None
    return {
        'notice_subtype': 'Corrigendum',
        'cause_reference': None,
        'amends_notice': _digits(t.group('ref')),
        'amends_notice_year': t.group('year'),
        'amendments': amendments,
    }


if __name__ == '__main__':
    import sys, json, glob, os
    targets = sys.argv[1:] or sorted(glob.glob('/mnt/user-data/uploads/cleaned*.txt'))
    tot_ok = tot = 0
    for f in targets:
        t = open(f, encoding='utf-8', errors='replace').read()
        blocks = [b for b in re.split(r'(?=CAUSE NO\.)', t) if b.startswith('CAUSE NO.')]
        corr = [b for b in blocks if re.search(r'\bamend\s*the\b', b, re.I)]
        if not corr:
            continue
        ok = sum(1 for b in corr if extract(b))
        tot_ok += ok; tot += len(corr)
        print('%-24s corrigenda: %3d  extracted: %3d  (%.1f%%)'
              % (os.path.basename(f), len(corr), ok, 100*ok/len(corr)))
    if tot:
        print('\nTOTAL corrigenda: %d | extracted: %d (%.1f%%)' % (tot, tot_ok, 100*tot_ok/tot))
        # show one
        for f in targets:
            t = open(f, encoding='utf-8', errors='replace').read()
            for b in re.split(r'(?=CAUSE NO\.)', t):
                if b.startswith('CAUSE NO.') and extract(b):
                    print('\n--- sample ---'); print(json.dumps(extract(b), indent=2)); sys.exit(0)
