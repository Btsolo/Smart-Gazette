"""
Field check - OCR as an independent witness for extracted values (measurement).

For every notice the templates extract from the positions text (fix 1), look
for its key values in the Tesseract text of the SAME page (and the next one,
since notices run on): notice number, probate case reference, land parcel id,
and the deceased / proprietor names. Matching is fuzzy: case, spacing,
punctuation and the usual OCR confusions (O/0, l/1/I, S/5, B/8, Z/2) are
ignored on both sides.

A value OCR cannot find on its page is a FLAG, not a verdict (lesson: OCR
makes its own errors). Flags are grouped and sampled so the cause can be
checked by eye.

Usage: python tools/field_check.py 2023 [2025 ...]
Writes reports/eval/field_check_<year>.txt
"""
import glob, json, os, re, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import corpus_report as R

CONF = str.maketrans({'o': '0', 'l': '1', 'i': '1', 's': '5', 'b': '8', 'z': '2', '|': '1'})


def norm(s):
    return re.sub(r'[^a-z0-9]', '', (s or '').lower()).translate(CONF)


def found(value, hay):
    v = norm(value)
    return len(v) >= 3 and v in hay


def name_found(name, hay):
    """All name tokens of 3+ letters present (order-free)."""
    toks = [norm(t) for t in re.findall(r"[A-Za-z']{3,}", name or '')]
    return bool(toks) and all(t in hay for t in toks)


def main(years):
    for y in years:
        stats = Counter()
        flags = defaultdict(list)
        for sj in sorted(glob.glob(os.path.join(R.REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            if s['kind'] != 'born-digital':
                continue
            pos_path = os.path.join(R.REPO, 'raw', y, s['slug'] + '.positions.txt')
            if not os.path.exists(pos_path):
                continue
            pages = R.read_text(pos_path).split('\f')
            ocr = [open(p, encoding='utf-8', errors='replace').read()
                   for p in sorted(glob.glob(os.path.join(R.REPO, 'reports', 'ocr', y, s['slug'], 'p*.txt')))]
            ocr_n = [norm(t) for t in ocr]
            cleaned = R.G.apply_ascending_lock(R.G.clean('\f'.join(pages)))
            for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', cleaned):
                if not n.startswith('GAZETTE NOTICE NO.'):
                    continue
                num = int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1))
                pg = next((i for i, p in enumerate(pages) if re.search(r'NOTICE\s+NO\.\s*%d\b' % num, p)), None)
                if pg is None or pg >= len(ocr_n):
                    stats['notice page not located'] += 1
                    continue
                hay = ''.join(ocr_n[pg:pg + 3])           # notice may run onto following pages
                where = '%s/%s/%d (p%d)' % (y, s['slug'], num, pg + 1)

                def check(field, value, ok):
                    stats[field + ' checked'] += 1
                    if ok:
                        stats[field + ' confirmed'] += 1
                    elif len(flags[field]) < 400:
                        flags[field].append((where, value))

                check('notice_number', str(num), found('noticeno%d' % num, hay) or found(str(num), ocr_n[pg]))
                cat = R.CEN.categorise(n)
                if cat == 'court_legal':
                    for b in [x for x in re.split(r'(?=CAUSE NO\.)', n) if x.startswith('CAUSE NO.')]:
                        r = R.P.extract(b, n)
                        if not r:
                            continue
                        check('case_reference', r['case_reference'], found(r['case_reference'], hay))
                        check('deceased_name', r['deceased_name'], name_found(r['deceased_name'], hay))
                elif cat == 'land_property':
                    r = R.L.extract(n)
                    if r:
                        check('parcel_id', r['parcel_id'], found(r['parcel_id'], hay))
                        for p in (r.get('parties') or [])[:2]:
                            check('proprietor_name', p, name_found(p, hay))
        lines = ['Field check %s - extracted values (positions text) vs Tesseract text of the same page(s)' % y, '']
        for f in ('notice_number', 'case_reference', 'deceased_name', 'parcel_id', 'proprietor_name'):
            c, ok = stats[f + ' checked'], stats[f + ' confirmed']
            if c:
                lines.append('%-16s checked %6d  confirmed %6d (%.2f%%)  flagged %d' % (f, c, ok, 100.0 * ok / c, c - ok))
        if stats['notice page not located']:
            lines.append('notices whose page could not be located: %d' % stats['notice page not located'])
        for f, fl in flags.items():
            lines += ['', '== flagged %s (first %d)' % (f, min(len(fl), 25))]
            lines += ['   %-28s %r' % (w, v) for w, v in fl[:25]]
        os.makedirs(os.path.join(R.REPO, 'reports', 'eval'), exist_ok=True)
        open(os.path.join(R.REPO, 'reports', 'eval', 'field_check_%s.txt' % y), 'w', encoding='utf-8').write('\n'.join(lines) + '\n')
        print('\n'.join(lines[:8]))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2023'])
