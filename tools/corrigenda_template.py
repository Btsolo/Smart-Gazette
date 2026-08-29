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

def extract(block):
    c = RE_CAUSE.search(block)
    a = RE_AMEND.search(block)
    if not (c and a):
        return None

    amendments = [{'field': _t(m.group('field')),
                   'printed_as': _t(m.group('old')),
                   'to_read': _t(m.group('new'))}
                  for m in RE_AMEND.finditer(block)]

    r = RE_REF.search(block)
    return {
        'notice_subtype': 'Corrigendum',
        'cause_reference': '%s of %s' % (_digits(c.group('cause')), _digits(c.group('cause_year'))),
        'amends_notice': _digits(r.group('ref')) if r else None,
        'amends_notice_year': _digits(r.group('ref_year')) if r and r.group('ref_year') else None,
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
