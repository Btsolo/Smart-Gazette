"""
Residual template failures - measurement only.

Runs the probate and land templates on the pdftotext text layer (clean text:
no fragmentation, split years or drop-caps) for the same notices the pipeline
sees. A block that fails on the pipeline text but passes here failed because
of TEXT DAMAGE. A block that fails here too is a real GRAMMAR / TEMPLATE gap
(or a quirk of the source) - those are listed with samples.

Usage: python tools/residual_failures.py 2022 2023 2024 2026
Writes reports/eval/residual_failures.txt
"""
import glob, json, os, re, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import corpus_report as R
import extractor_eval as E

P, L = R.P, R.L


def light_clean(t):
    """What any pdftotext lane would need at minimum: drop running headers and
    page numbers, normalise curly punctuation."""
    t = re.sub(r'^\s*(?:\d{1,5}\s+)?THE KENYA GAZETTE(?:\s+\d{1,2}(?:st|nd|rd|th)\s+\w+,?\s+\d{4})?(?:\s+\d{1,5})?\s*$', '', t, flags=re.M)
    t = re.sub(r'^\s*\d{1,2}(?:st|nd|rd|th)\s+\w+,?\s+\d{4}\s+THE KENYA GAZETTE.*$', '', t, flags=re.M)
    for a, b in (('’', "'"), ('‘', "'"), ('“', '"'), ('”', '"'), ('–', '-'), ('—', '-')):
        t = t.replace(a, b)
    return t


def why_block(b):
    m = P.RE_CAUSE.search(b)
    if not m:
        if re.match(r'CAUSE NO\.\s*\S+\s+of\b', b): return 'cause header: lowercase "of" (corrigendum / cross-reference text)'
        if re.search(r'amend|printed as|to read', b, re.I): return 'cause header: corrigendum text'
        if not re.search(r'\bBy\b', b[:120]): return 'cause header: no "By <petitioner>" after the cause number'
        return 'cause header: other'
    a = P.RE_ACTION.search(m.group('body'))
    if not a:
        body = m.group('body')
        if re.search(r'for\s+grant\s+of', body, re.I): return 'action: "for grant" without a/the'
        if re.search(r'estate\s+(?!of)', body, re.I) and not re.search(r'estate\s+of', body, re.I): return 'action: "estate <name>" without "of"'
        if re.search(r'confirmation|revocation|rectification|summons|limited grant|ad colligenda|ad litem', body, re.I):
            return 'action: other grant type (limited / ad litem / confirmation / revocation)'
        if re.search(r'letters\s+of\s+administration', body, re.I): return 'action: letters of administration, other wording'
        return 'action: other'
    tail = a.group('tail')
    if not P.RE_DEATH.search(tail):
        if re.search(r'died\s+(?:at\s+[^,]+?\s*)?in\s+(?:19|20)\d\d', tail, re.I): return 'death: "died in <year>" only'
        if re.search(r'died\s+(?:at|in)\s+[^.]{2,80}\.\s*$', tail.strip()[:200], re.I) or not re.search(r'\d{4}', tail[:300]):
            return 'death: no date in the notice'
        if re.search(r'died\s+(?:on\s+)?(?:or\s+)?(?:about|around)', tail, re.I): return 'death: "on or about"'
        if re.search(r'\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{2,4}', tail): return 'death: numeric date dd/mm/yyyy'
        if re.search(r'who\s+died\s+at\s+[^,]+,\s*\d', tail, re.I): return 'death: date without "on"'
        return 'death: other'
    return 'required field empty after name cleaning'


def why_land(n):
    if not L.RE_ACT.search(n): return 'not a Land Registration / Titles Act title notice'
    if not L.RE_PROP.search(n):
        if re.search(r'of\s+(?:that|those)\s+piece', n) and not re.search(r'of\s+all\s+th', n): return 'proprietor: "of that piece" (no "all")'
        if re.search(r'REGISTRATION OF INSTRUMENT|CANCELLATION', n): return 'proprietor: registration of instrument / cancellation grammar'
        if re.search(r'LOSS OF|RECONSTRUCTION', n): return 'proprietor: loss / reconstruction grammar'
        return 'proprietor: other'
    lr = L.RE_LR.search(n) or L.RE_LR2.search(n)
    if not lr:
        if re.search(r'Plot\s+No|Block\s+\d|Subdivision', n, re.I): return 'parcel: Plot / Block / Subdivision form not matched'
        if re.search(r'under\s+(?:certificate|lease|title)', n, re.I): return 'parcel: "registered under certificate of lease / title" form'
        return 'parcel: other'
    return 'parcel unusable or parties empty'


def main(years):
    blk = Counter(); blk_ex = defaultdict(list)
    land = Counter(); land_ex = defaultdict(list)
    tot = Counter()
    for y in years:
        for sj in sorted(glob.glob(os.path.join(R.REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            if s['kind'] != 'born-digital':
                continue
            ptt = light_clean(R.read_text(os.path.join(R.REPO, 'raw', y, s['slug'] + '.pdftotext.txt')))
            for num, n in E.ocr_notices(ptt.split('\f')).items():
                cat = R.CEN.categorise(n)
                if cat == 'court_legal':
                    for b in [x for x in re.split(r'(?=CAUSE NO\.)', n) if x.startswith('CAUSE NO.')]:
                        tot['probate blocks'] += 1
                        if P.extract(b, n):
                            tot['probate blocks ok'] += 1
                            continue
                        k = why_block(b); blk[k] += 1
                        if len(blk_ex[k]) < 4:
                            blk_ex[k].append('%s/%s/%d: %s' % (y, s['slug'], num, re.sub(r'\s+', ' ', b[:170])))
                elif cat == 'land_property':
                    tot['land notices'] += 1
                    if L.extract(n):
                        tot['land ok'] += 1
                        continue
                    k = why_land(n); land[k] += 1
                    if len(land_ex[k]) < 3:
                        i = n.find('WHEREAS')
                        land_ex[k].append('%s/%s/%d: %s' % (y, s['slug'], num, re.sub(r'\s+', ' ', n[max(0, i):max(0, i) + 200])))
    out = ['Residual template failures on the pdftotext text layer (light clean), years %s' % ', '.join(years),
           'probate blocks %d, ok %d (%.1f%%) | land notices %d, ok %d (%.1f%%)' % (
               tot['probate blocks'], tot['probate blocks ok'], 100.0 * tot['probate blocks ok'] / max(1, tot['probate blocks']),
               tot['land notices'], tot['land ok'], 100.0 * tot['land ok'] / max(1, tot['land notices'])),
           '', '== probate blocks still failing, by cause']
    for k, v in blk.most_common():
        out.append('%6d  %s' % (v, k))
        out += ['          %s' % e for e in blk_ex[k][:3]]
    out += ['', '== land notices still failing, by cause']
    for k, v in land.most_common():
        out.append('%6d  %s' % (v, k))
        out += ['          %s' % e for e in land_ex[k][:2]]
    os.makedirs(os.path.join(R.REPO, 'reports', 'eval'), exist_ok=True)
    open(os.path.join(R.REPO, 'reports', 'eval', 'residual_failures.txt'), 'w', encoding='utf-8').write('\n'.join(out) + '\n')
    print('\n'.join(l for l in out if not l.startswith('          ')))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2022', '2023', '2024', '2026'])
