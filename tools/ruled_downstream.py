"""
Ruled tables (fix 7b) - downstream check: does the new extraction change
anything after the extractor? For every born-digital gazette, both texts
(raw/<year>/<slug>.positions.txt = before, .ruled.txt = after) go through
the same cleaner, segmentation, categoriser, templates and table extractor:

  - notices found (set of notice numbers)        must not get worse
  - category of each notice                       changes listed
  - template success (probate / land / others)    must not get worse
  - tables: rows, rows matching their table's width (fix 8 measure)

Usage: python tools/ruled_downstream.py 2022 2023 2024 2025 2026
"""
import collections, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import gazette_clean as G
import vocab_repair as VR
import corpus_report as R
import category_census as C
import table_extract as T


def notices(text):
    return {int(m.group(1)): n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text)
            for m in [re.match(r'GAZETTE NOTICE NO\. (\d+)', n)] if m}


def measure(raw, V):
    ns = notices(G.apply_ascending_lock(VR.repair(G.clean(raw), V)))
    out = {}
    for num, n in ns.items():
        cat = C.categorise(n)
        ok = bool(C.template_for(cat, n))
        tabs = T.tables(n)
        rows = sum(len(t['rows']) for t in tabs)
        # a row is at the table's width when it has as many cells as the
        # header (or, without a header, as the table's most common row)
        width_ok = 0
        for t in tabs:
            ws = t.get('raw_widths', [])
            if not ws:
                continue
            target = len(t['columns']) if t['columns'] else collections.Counter(ws).most_common(1)[0][0]
            width_ok += sum(1 for w in ws if w == target)
        out[num] = (cat, ok, rows, width_ok, len(tabs))
    return out


def main(years, b_suf='positions', a_suf='ruled'):
    V = VR.load_corpus_vocab()
    tot = collections.defaultdict(collections.Counter)
    changes = []
    for y in years:
        d = os.path.join(REPO, 'raw', y)
        for f in sorted(os.listdir(d)):
            if not f.endswith('.%s.txt' % a_suf):
                continue
            slug = f[:-len('.%s.txt' % a_suf)]
            if not os.path.exists(os.path.join(d, slug + '.%s.txt' % b_suf)):
                continue
            before = measure(R.read_text(os.path.join(d, slug + '.%s.txt' % b_suf)), V)
            after = measure(R.read_text(os.path.join(d, f)), V)
            s = tot[y]
            s['gazettes'] += 1
            s['notices before'] += len(before)
            s['notices after'] += len(after)
            s['notices lost'] += len(set(before) - set(after))
            s['notices gained'] += len(set(after) - set(before))
            for num in set(before) & set(after):
                b, a = before[num], after[num]
                if b[0] != a[0]:
                    s['category changes'] += 1
                    changes.append((y, slug, num, b[0], a[0]))
                s['templated before'] += b[1]
                s['templated after'] += a[1]
                if b[1] and not a[1]:
                    s['template lost'] += 1
                    changes.append((y, slug, num, 'template lost', a[0]))
                if a[1] and not b[1]:
                    s['template gained'] += 1
                s['table rows before'] += b[2]
                s['table rows after'] += a[2]
                s['rows at table width before'] += b[3]
                s['rows at table width after'] += a[3]
    for y in years:
        print('==', y, dict(tot[y]))
    print('== all', dict(sum(tot.values(), collections.Counter())))
    for c in changes[:40]:
        print('  change:', c)


if __name__ == '__main__':
    # python tools/ruled_downstream.py [--before positions] [--after ruled] years...
    a = sys.argv[1:]
    kw = {}
    for opt, key in (('--before', 'b_suf'), ('--after', 'a_suf')):
        if opt in a:
            i = a.index(opt); kw[key] = a[i + 1]; del a[i:i + 2]
    main(a, **kw)
