"""
What the templates still reject on the CURRENT pipeline text (positions
extractor + cleaner + vocabulary repair) - measurement for fix 4.

Usage: python tools/template_gaps.py 2023 2025
Writes reports/eval/template_gaps.txt
"""
import glob, json, os, re, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import corpus_report as R
import residual_failures as RF
import vocab_repair as VR

C = R.C


def pipeline_notices(y, slug, V):
    t = R.read_text(os.path.join(R.REPO, 'raw', y, slug + '.positions.txt'))
    c = R.G.apply_ascending_lock(VR.repair(R.G.clean(t), V))
    return [n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', c) if n.startswith('GAZETTE NOTICE NO.')]


def why_corr(n):
    if re.search(r'CAUSE NO\.', n): return 'has CAUSE NO. but template failed'
    if re.search(r'printed\s+as', n, re.I): return 'amend ... printed as ... to read (no CAUSE NO.)'
    if re.search(r'\bdelete\b|\binsert\b', n, re.I): return 'delete / insert instructions (no CAUSE NO.)'
    if re.search(r'intends?\s+to\s+(?:correct|delete)|further\s+to\s+(?:Kenya\s+)?Gazette', n, re.I): return 'land acquisition correction (schedule)'
    return 'other (possibly not a correction at all)'


def main(years):
    V = VR.load_corpus_vocab()
    stats, causes, ex = Counter(), defaultdict(Counter), defaultdict(list)
    for y in years:
        for sj in sorted(glob.glob(os.path.join(R.REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            if s['kind'] != 'born-digital' or not os.path.exists(os.path.join(R.REPO, 'raw', y, s['slug'] + '.positions.txt')):
                continue
            for n in pipeline_notices(y, s['slug'], V):
                num = re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1)
                where = '%s/%s/%s' % (y, s['slug'], num)
                cat = R.CEN.categorise(n)
                if cat == 'court_legal':
                    for b in [x for x in re.split(r'(?=CAUSE NO\.)', n) if x.startswith('CAUSE NO.')]:
                        stats['probate blocks'] += 1
                        r = R.P.extract(b, n)
                        if r:
                            stats['probate ok'] += 1
                            if 'through' in b and not r['advocate_firm']:
                                k = 'advocate missed'
                                causes['probate fields'][k] += 1
                                if len(ex[k]) < 4: ex[k].append((where, re.search(r'through.{0,140}', b, re.S).group(0)))
                            continue
                        k = RF.why_block(b)
                        causes['probate'][k] += 1
                        if len(ex[k]) < 4: ex[k].append((where, b[:260]))
                elif cat == 'land_property':
                    stats['land'] += 1
                    r = R.L.extract(n)
                    if r:
                        stats['land ok'] += 1
                        continue
                    k = RF.why_land(n)
                    causes['land'][k] += 1
                    i = n.find('WHEREAS')
                    if len(ex[k]) < 4: ex[k].append((where, n[max(0, i):max(0, i) + 300] if i >= 0 else n[:300]))
                elif cat == 'Corrigenda':
                    stats['corrigenda'] += 1
                    k = why_corr(n)
                    causes['corrigenda'][k] += 1
                    if len(ex[k]) < 4: ex[k].append((where, n[:300]))
    out = ['Template gaps on current pipeline text, %s' % ', '.join(years),
           'probate blocks %d ok %.1f%% | land %d ok %.1f%% | corrigenda notices %d' % (
               stats['probate blocks'], 100.0 * stats['probate ok'] / max(1, stats['probate blocks']),
               stats['land'], 100.0 * stats['land ok'] / max(1, stats['land']), stats['corrigenda'])]
    for grp, cs in causes.items():
        out += ['', '== ' + grp]
        for k, v in cs.most_common():
            out.append('%6d  %s' % (v, k))
            out += ['          %s: %r' % e for e in ex[k][:3]]
    open(os.path.join(R.REPO, 'reports', 'eval', 'template_gaps.txt'), 'w', encoding='utf-8').write('\n'.join(out) + '\n')
    print('\n'.join(l for l in out if not l.startswith('          ')))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2023', '2025'])
