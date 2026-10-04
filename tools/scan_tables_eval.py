"""
Scan tables scorecard (docs/specs/scan-tables.md) - measurement.

For every scanned page (21 issues, 2022-2025): the page image's ruling lines
(cached as reports/ocr/<year>/<slug>/pNNNN.rules.json), the new page text
(pNNNN.tab.txt) and, against Tesseract's own text (pNNNN.txt):
  - letters and digits identical, except "I"s that replaced a stray "|"
  - lines with cells outside a table (must be 0)
  - ruled tables / rows, unruled rows
  - time per page for the rules (render excluded: the Java lane has the image)

Usage: python tools/scan_tables_eval.py [--jobs 4] 2022 2023 2024 2025
"""
import argparse, collections, concurrent.futures as cf, csv, json, os, re, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import scan_tables as ST

CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')
ALNUM = re.compile(r'[A-Za-z0-9]')


def pdf_map(year):
    out = {}
    for f in os.listdir(os.path.join(CORPUS, year)):
        m = re.search(r'No\s*(\d+)(\s*\(1\))?\.pdf$', f)
        if m:
            out.setdefault('no%03d' % int(m.group(1)) + ('_b' if m.group(2) else ''), os.path.join(CORPUS, year, f))
    return out


def rules_for(pdf, page, cache):
    if os.path.exists(cache):
        R = json.load(open(cache, encoding='utf-8'))
        return R, 0.0
    img = ST.render(pdf, page)
    t0 = time.time()
    R = ST.image_rules(img, 2)
    dt = time.time() - t0
    R = {'h': [list(map(float, h)) for h in R['h']], 'v': [list(map(float, v)) for v in R['v']], 'W': R['W'], 'H': R['H']}
    json.dump(R, open(cache, 'w', encoding='utf-8'))
    return R, dt


def one(job):
    year, slug, pdf, page = job
    d = os.path.join(REPO, 'reports', 'ocr', year, slug)
    tsv = os.path.join(d, 'p%04d.tsv.gz' % page)
    if not os.path.exists(tsv):
        return None
    R, dt = rules_for(pdf, page, os.path.join(d, 'p%04d.rules.json' % page))
    R = {'h': [tuple(h) for h in R['h']], 'v': [tuple(v) for v in R['v']], 'W': R['W'], 'H': R['H']}
    st = {}
    text = ST.page_text(ST.read_words(tsv), R, st)
    open(os.path.join(d, 'p%04d.tab.txt' % page), 'w', encoding='utf-8').write(text)
    before = open(os.path.join(d, 'p%04d.txt' % page), encoding='utf-8', errors='replace').read()
    a, b = collections.Counter(ALNUM.findall(before)), collections.Counter(ALNUM.findall(text))
    extra_i = b['I'] - a['I']
    b['I'] -= max(0, extra_i)
    return year, slug, page, a == b, max(0, extra_i), st, dt, (a - b, b - a) if a != b else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--jobs', type=int, default=4)
    ap.add_argument('years', nargs='+')
    a = ap.parse_args()
    jobs = []
    for y in a.years:
        pdfs = pdf_map(y)
        for r in csv.DictReader(open(os.path.join(REPO, 'reports', 'ocr', y, 'gazettes.csv'), encoding='utf-8')):
            if r['kind'] in ('born-digital', 'mixed') or r['slug'] not in pdfs:
                continue
            jobs += [(y, r['slug'], pdfs[r['slug']], p) for p in range(1, int(r['pages']) + 1)]
    tot = collections.defaultdict(collections.Counter)
    times, bad = [], []
    with cf.ProcessPoolExecutor(a.jobs) as ex:
        for res in ex.map(one, jobs, chunksize=4):
            if not res:
                continue
            y, slug, page, same, extra_i, st, dt, diff = res
            s = tot[y]
            s['pages'] += 1
            s['pages with letters/digits identical'] += same
            s['"|" read as "I"'] += extra_i
            for k, v in st.items():
                s[k] += v
            s['pages with a table'] += bool(st.get('ruled rows') or st.get('unruled rows'))
            if dt:
                times.append(dt)
            if not same:
                bad.append((y, slug, page, dict(list(diff[0].items())[:6]), dict(list(diff[1].items())[:6])))
    for y in sorted(tot):
        print('==', y, dict(tot[y]))
    print('== all', dict(sum(tot.values(), collections.Counter())))
    if times:
        times.sort()
        print('rules per page: median %.2fs, max %.2fs (render excluded)' % (times[len(times) // 2], times[-1]))
    for b in bad[:12]:
        print('  letters differ:', b)


if __name__ == '__main__':
    main()
