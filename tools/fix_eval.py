"""
Before/after scorecard for cleaner and repair changes - measurement only.

Runs the real categoriser and templates on the fix-1 positions text through
several variants and compares them per year:
  base    baseline cleaner (frozen copy: --baseline-cleaner PATH), no repair
  fix2    baseline cleaner + vocabulary repair, corpus dictionary (tools/gazette_vocab.txt)
  fix2p   baseline cleaner + vocabulary repair, per-gazette pdftotext dictionary
  fix3    current tools/gazette_clean.py, no repair
  fix23   current cleaner + vocabulary repair, corpus dictionary
Truth for notice detection = the pdftotext headers (three-way check agrees on
born-digital gazettes).

Usage: python tools/fix_eval.py --baseline-cleaner PATH 2022 2023 2024 2025 2026
Writes reports/eval/fix_eval.json and prints a table.
"""
import argparse, glob, importlib.util, json, os, re, sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import corpus_report as R
import extractor_eval as E
import ocr_check as OC
import vocab_repair as VR
import gazette_clean as NEW


def load_module(path, name):
    s = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m


def notices(G, text, V=None):
    c = G.clean(text)
    if V is not None:
        c = VR.repair(c, V)
    c = G.apply_ascending_lock(c)
    return c, {int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1)): n
               for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', c) if n.startswith('GAZETTE NOTICE NO.')}


def score(T, k, text, ns, truth, ref_w, ref_n):
    T[k + ' notices'] += len(ns)
    T[k + ' missed'] += len(truth - set(ns))
    T[k + ' extra'] += len(set(ns) - truth)
    T[k + ' words'] += sum((ref_w & E.words(text)).values())
    T[k + ' nums'] += sum((ref_n & E.nums(text)).values())
    T[k + ' year deleted'] += len(re.findall(r'CAUSE NO\.\s*\S+\s+OF\s+By\b', text))
    for n in ns.values():
        cat = R.CEN.categorise(n)
        if cat not in ('court_legal', 'land_property', 'Corrigenda'):
            continue
        c = R.canon(cat)
        T['%s %s eligible' % (k, c)] += 1
        T['%s %s ok' % (k, c)] += R.PL.route_notice(n)[3] == 'template'
        if cat == 'court_legal':
            for b in R.P.split_causes(n):
                T[k + ' blocks'] += 1
                r = R.P.extract(b, n)
                if r:
                    T[k + ' blocks ok'] += 1
                    T[k + ' f relationship'] += bool(r.get('petitioner_relationship'))
                    T[k + ' f advocate'] += bool(r.get('advocate_firm'))
                    T[k + ' f place'] += bool(r.get('place_of_death'))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--baseline-cleaner', required=True)
    ap.add_argument('--only-current', action='store_true',
                    help='score only the current pipeline (cleaner + corpus repair), e.g. before/after a template change')
    ap.add_argument('--out', default='fix_eval.json')
    ap.add_argument('years', nargs='+')
    a = ap.parse_args()
    OLD = load_module(a.baseline_cleaner, 'gazette_clean_baseline')
    VC = VR.load_corpus_vocab()
    variants = [('base', OLD, None), ('fix2', OLD, 'corpus'), ('fix2p', OLD, 'ptt'), ('fix3', NEW, None), ('fix23', NEW, 'corpus')]
    if a.only_current:
        variants = [('current', NEW, 'corpus')]
    allres = {}
    for y in a.years:
        T = Counter()
        for sj in sorted(glob.glob(os.path.join(R.REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            if s['kind'] != 'born-digital':
                continue
            pos_path = os.path.join(R.REPO, 'raw', y, s['slug'] + '.positions.txt')
            if not os.path.exists(pos_path):
                E_pdf = os.path.join(R.REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5', y, s['file'])
                import positions_eval
                positions_eval.positions_text(y, s['slug'], E_pdf)
            pos = R.read_text(pos_path)
            ptt = R.read_text(os.path.join(R.REPO, 'raw', y, s['slug'] + '.pdftotext.txt'))
            page_of, _ = OC.headers_by_page(ptt.split('\f'), OC.HDR_TEXT)
            truth = set(page_of)
            ref_w, ref_n = E.words(ptt), E.nums(ptt)
            Vp = VR.build_vocab(ptt)
            for k, G, vsrc in variants:
                V = VC if vsrc == 'corpus' else (Vp if vsrc == 'ptt' else None)
                text, ns = notices(G, pos, V)
                score(T, k, text, ns, truth, ref_w, ref_n)
            T['truth'] += len(truth); T['ref words'] += sum(ref_w.values()); T['ref nums'] += sum(ref_n.values())
        allres[y] = dict(T)
        pct = lambda a_, b_: 100.0 * T[a_] / max(1, T[b_])
        print('== %s (truth notices %d)' % (y, T['truth']))
        print('   %-6s %8s %6s %6s %9s %9s %9s %9s %8s %7s %7s %7s %7s %7s' % (
            'var', 'notices', 'missed', 'extra', 'probate', 'blocks', 'land', 'corrig', 'words', 'nums', 'relat', 'advoc', 'place', 'yrdel'))
        for k, _, _ in variants:
            print('   %-6s %8d %6d %6d %8.1f%% %8.1f%% %8.1f%% %8.1f%% %7.1f%% %6.1f%% %6.1f%% %6.1f%% %6.1f%% %7d' % (
                k, T[k + ' notices'], T[k + ' missed'], T[k + ' extra'],
                pct(k + ' Court_Legal ok', k + ' Court_Legal eligible'), pct(k + ' blocks ok', k + ' blocks'),
                pct(k + ' Land_Property ok', k + ' Land_Property eligible'), pct(k + ' Corrigenda ok', k + ' Corrigenda eligible'),
                pct(k + ' words', 'ref words'), pct(k + ' nums', 'ref nums'),
                pct(k + ' f relationship', k + ' blocks ok'), pct(k + ' f advocate', k + ' blocks ok'),
                pct(k + ' f place', k + ' blocks ok'), T[k + ' year deleted']), flush=True)
    os.makedirs(os.path.join(R.REPO, 'reports', 'eval'), exist_ok=True)
    json.dump(allres, open(os.path.join(R.REPO, 'reports', 'eval', a.out), 'w', encoding='utf-8'), indent=1)


if __name__ == '__main__':
    main()
