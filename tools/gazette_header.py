"""
Issue details from the cover text - volume, number, date (extractor stage).

Every born-digital issue prints them on page 1 ("Vol. CXXVIII-No. 166 ...
NAIROBI, 18th September, 2026"; all 264 issues carry both markings). The app
asked the AI for them; when that call failed (test run 3 Oct 2026: vision
"service unavailable") every notice of the issue was stored without its
gazette number and date. A regex reads them exactly and for free; the AI is
only the fallback.

Java port: GazetteHeaderParser (keep in sync).
Usage: python tools/gazette_header.py   (checks every cached issue)
"""
import re

MONTHS = {m: i for i, m in enumerate(['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
                                      'september', 'october', 'november', 'december'], 1)}
RE_VOLNO = re.compile(r'Vol\.?\s*(?P<vol>[CXLVI]+)\s*[-–—]?\s*No\.?\s*(?P<no>\d{1,3})', re.I)
RE_DATE = re.compile(r'NAIROBI,?\s*(?P<d>\d{1,2})\s*(?:st|nd|rd|th)?\s*(?P<m>[A-Za-z]+),?\s*(?P<y>(?:19|20)\d\d)')


def parse(text):
    """{'gazetteVolume': 'Vol. CXXVIII', 'gazetteNumber': 'No. 166',
        'gazetteDate': '2026-09-18'} from the first 3,000 characters, or None
    when either the volume/number or the date is not printed there."""
    head = text[:3000]
    v = RE_VOLNO.search(head)
    d = RE_DATE.search(head)
    if not v or not d:
        return None
    month = MONTHS.get(d.group('m').lower())
    if not month:
        return None
    return {'gazetteVolume': 'Vol. ' + v.group('vol').upper(),
            'gazetteNumber': 'No. ' + str(int(v.group('no'))),
            'gazetteDate': '%s-%02d-%02d' % (d.group('y'), month, int(d.group('d')))}


if __name__ == '__main__':
    import glob, os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import corpus_report as R
    ok = bad = 0
    for p in sorted(glob.glob(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'raw', '*', '*.positions.txt'))):
        r = parse(R.G.clean(R.read_text(p)))
        slug = os.path.basename(p).split('.')[0]
        if r and r['gazetteNumber'] == 'No. ' + str(int(re.match(r'no(\d+)', slug).group(1))):
            ok += 1
        else:
            bad += 1
            print('  not read / wrong:', p.split(os.sep)[-2], slug, r)
    print('issues %d | header read and number matches the file %d | not %d' % (ok + bad, ok, bad))
