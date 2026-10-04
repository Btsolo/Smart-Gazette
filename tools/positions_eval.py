"""
Evaluate the positions-based pdf-inspector extractor (tools/inspect_positions.js)
against today's extractText + cleaner - measurement only.

Same scorecard as extractor_eval.py: word / number accuracy vs the text layer,
notices found by the existing clean() + lock, template success, table rows.

Usage: python tools/positions_eval.py 2022 2023 2024 2026
Writes reports/eval/positions_<year>.json
"""
import csv, glob, json, os, re, subprocess, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import corpus_report as R
import extractor_eval as E
import vocab_repair as VR

REPO = R.REPO
CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')


def positions_text(y, slug, pdf):
    path = os.path.join(REPO, 'raw', y, slug + '.positions.txt')
    if not os.path.exists(path):
        r = subprocess.run(['node', os.path.join('tools', 'inspect_positions.js'), pdf], capture_output=True, cwd=REPO, timeout=600)
        open(path, 'wb').write(r.stdout)
    return R.read_text(path)


def notices_of(text):
    c = R.G.apply_ascending_lock(R.G.clean(text))
    return c, {int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1)): n
               for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', c) if n.startswith('GAZETTE NOTICE NO.')}


def main(years):
    for y in years:
        tabs = defaultdict(set)
        tpath = os.path.join(REPO, 'reports', 'corpus', y, 'tables.csv')
        for r in csv.DictReader(open(tpath, encoding='utf-8')):
            tabs[r['slug']].add(int(r['page']))
        T = Counter(); ex = defaultdict(list); D = defaultdict(list)
        for sj in sorted(glob.glob(os.path.join(REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            if s['kind'] != 'born-digital':
                continue
            slug = s['slug']
            pdf = os.path.join(CORPUS, y, s['file'])
            ptt = R.read_text(os.path.join(REPO, 'raw', y, slug + '.pdftotext.txt'))
            pos = positions_text(y, slug, pdf)
            raw = R.read_text(os.path.join(REPO, 'raw', y, slug + '.inspector.txt'))
            ref_w, ref_n = E.words(ptt), E.nums(ptt)
            ci, ni = notices_of(raw)
            cp, npos = notices_of(pos)
            # third variant: positions text + vocabulary repair (words from the text layer)
            V = VR.build_vocab(ptt)
            cv = VR.repair(cp, V)
            nv = {int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1)): n
                  for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', cv) if n.startswith('GAZETTE NOTICE NO.')}
            for num in set(ni) & set(nv):
                cat = R.CEN.categorise(ni[num])
                if cat in ('court_legal', 'land_property'):
                    T[R.canon(cat) + ' positions+vocab ok'] += R.PL.route_notice(nv[num])[3] == 'template'
                    if cat == 'court_legal':
                        bl = [x for x in re.split(r'(?=CAUSE NO\.)', nv[num]) if x.startswith('CAUSE NO.')]
                        T['blocks positions+vocab'] += len(bl); T['blocks ok positions+vocab'] += sum(1 for x in bl if R.P.extract(x, nv[num]))
            T['positions+vocab words'] += sum((ref_w & E.words(cv)).values())
            T['positions+vocab nums'] += sum((ref_n & E.nums(cv)).values())
            for k, t in (('inspector', ci), ('positions', cp)):
                T[k + ' words'] += sum((ref_w & E.words(t)).values())
                T[k + ' nums'] += sum((ref_n & E.nums(t)).values())
            T['ref words'] += sum(ref_w.values()); T['ref nums'] += sum(ref_n.values())
            # notices vs the three-way truth (pdftotext / Tesseract agree on born-digital)
            truth = set(int(x) for x in re.findall(r'GAZETTE\s+NOTICE\s+NO\.\s*(\d+)', ptt))
            T['notices inspector'] += len(ni); T['notices positions'] += len(npos)
            miss = sorted(set(ni) - set(npos)); extra = sorted(set(npos) - set(ni))
            T['positions misses (inspector has)'] += len(miss); T['positions extra (inspector lacks)'] += len(extra)
            if miss and len(ex['miss']) < 8: ex['miss'].append('%s: %s' % (slug, miss[:5]))
            if extra and len(ex['extra']) < 8: ex['extra'].append('%s: %s' % (slug, extra[:5]))
            for num in set(ni) & set(npos):
                cat = R.CEN.categorise(ni[num])
                T['category same' if R.CEN.categorise(npos[num]) == cat else 'category different'] += 1
                if cat not in ('court_legal', 'land_property'):
                    continue
                c = R.canon(cat)
                T[c + ' eligible'] += 1
                a = R.PL.route_notice(ni[num])[3] == 'template'
                b = R.PL.route_notice(npos[num])[3] == 'template'
                T[c + ' inspector ok'] += a; T[c + ' positions ok'] += b
                if a and not b and len(ex[c + ' lost']) < 6: ex[c + ' lost'].append('%s/%d' % (slug, num))
                if cat == 'court_legal':
                    for k, t in (('inspector', ni[num]), ('positions', npos[num])):
                        bl = [x for x in re.split(r'(?=CAUSE NO\.)', t) if x.startswith('CAUSE NO.')]
                        T['blocks ' + k] += len(bl); T['blocks ok ' + k] += sum(1 for x in bl if R.P.extract(x, t))
            # tables: rows complete, reference pdftotext -layout
            if tabs[slug]:
                lay = R.read_text(os.path.join(REPO, 'raw', y, slug + '.layout.txt')).split('\f') \
                    if os.path.exists(os.path.join(REPO, 'raw', y, slug + '.layout.txt')) else []
                ptoks = E.TOK.findall(pos.lower()); pix = E.index(ptoks)
                ctoks = E.TOK.findall(ci.lower()); cix = E.index(ctoks)
                for p in sorted(tabs[slug]):
                    if p - 1 >= len(lay): continue
                    for row in E.layout_rows(lay[p - 1]):
                        D['positions'].append(E.row_found(row, ptoks, pix) >= 0.9)
                        D['inspector'].append(E.row_found(row, ctoks, cix) >= 0.9)
        res = {'totals': dict(T), 'examples': dict(ex), 'tables': {k: (round(sum(v) / len(v), 3), len(v)) for k, v in D.items() if v}}
        json.dump(res, open(os.path.join(REPO, 'reports', 'eval', 'positions_%s.json' % y), 'w', encoding='utf-8'), indent=1)
        pct = lambda a, b: 100.0 * T[a] / max(1, T[b])
        print('== %s' % y)
        print('   words intact  inspector %.1f%%  positions %.1f%%  +vocab %.1f%% | numbers intact inspector %.1f%%  positions %.1f%%  +vocab %.1f%%' % (
            pct('inspector words', 'ref words'), pct('positions words', 'ref words'), pct('positions+vocab words', 'ref words'),
            pct('inspector nums', 'ref nums'), pct('positions nums', 'ref nums'), pct('positions+vocab nums', 'ref nums')))
        print('   notices inspector %d  positions %d | positions misses %d, extra %d | category same %d different %d' % (
            T['notices inspector'], T['notices positions'], T['positions misses (inspector has)'], T['positions extra (inspector lacks)'],
            T['category same'], T['category different']))
        for c in ('Court_Legal', 'Land_Property'):
            print('   %-13s of %4d: inspector %.1f%%  positions %.1f%%  +vocab %.1f%%' % (
                c, T[c + ' eligible'], pct(c + ' inspector ok', c + ' eligible'), pct(c + ' positions ok', c + ' eligible'),
                pct(c + ' positions+vocab ok', c + ' eligible')))
        print('   probate blocks: inspector %.1f%%  positions %.1f%%  +vocab %.1f%%' % (
            pct('blocks ok inspector', 'blocks inspector'), pct('blocks ok positions', 'blocks positions'),
            pct('blocks ok positions+vocab', 'blocks positions+vocab')))
        print('   table rows complete:', res['tables'])
        print('   examples:', {k: v[:4] for k, v in ex.items()})


if __name__ == '__main__':
    main(sys.argv[1:] or ['2023'])
