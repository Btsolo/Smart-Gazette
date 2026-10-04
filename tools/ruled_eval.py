"""
Ruled-table scorecard (fix 7b, docs/specs/ruled-tables.md) - measurement.

For every born-digital gazette of the given years, compares the extractor's
new output (raw/<year>/<slug>.ruled.txt, written here with the ruled-table
pass) with the cached output from before the fix (raw/<year>/<slug>.positions.txt):

  - pages without a ruled table must be byte-identical;
  - pages with one must hold exactly the same words (multiset of word tokens):
    the pass may move, split and join text, never drop or invent it;
  - on changed pages: text lines before -> after, and cell rows after.

Usage: python tools/ruled_eval.py [--jobs 4] [--limit N] 2022 2023 2024 2025 2026
"""
import argparse, collections, concurrent.futures as cf, os, re, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')
WORD = re.compile(r'\w+')


def pdfs(year):
    out = {}
    for f in os.listdir(os.path.join(CORPUS, year)):
        m = re.search(r'No\s*(\d+)\.pdf$', f)
        if m:
            out['no%03d' % int(m.group(1))] = os.path.join(CORPUS, year, f)
    return out


def run(job):
    year, slug, pdf = job
    dst = os.path.join(REPO, 'raw', year, slug + '.ruled.txt')
    t0 = time.time()
    if not os.path.exists(dst):
        r = subprocess.run(['node', os.path.join(HERE, 'inspect_positions.js'), pdf], capture_output=True)
        if r.returncode != 0:
            return year, slug, None, time.time() - t0
        with open(dst, 'wb') as f:
            f.write(r.stdout)
    return year, slug, dst, time.time() - t0


def compare(before, after):
    b, a = before.split('\f'), after.split('\f')
    s = collections.Counter()
    if len(b) != len(a):
        s['page count differs'] += 1
        return s, []
    changed = []
    for k, (x, y) in enumerate(zip(b, a)):
        s['pages'] += 1
        if x == y:
            continue
        s['pages changed'] += 1
        wx, wy = collections.Counter(WORD.findall(x)), collections.Counter(WORD.findall(y))
        if wx != wy:
            s['pages with word differences'] += 1
            s['words lost'] += sum((wx - wy).values())
            s['words added'] += sum((wy - wx).values())
            changed.append((k + 1, sorted((wx - wy).items())[:5], sorted((wy - wx).items())[:5]))
        lx = [l for l in x.split('\n') if l.strip()]
        ly = [l for l in y.split('\n') if l.strip() and not re.match(r'^-{3}( \| -{3})+$', l)]
        s['lines before (changed pages)'] += len(lx)
        s['lines after (changed pages)'] += len(ly)
        s['cell rows before (changed pages)'] += sum(1 for l in lx if ' | ' in l)
        s['cell rows after (changed pages)'] += sum(1 for l in ly if ' | ' in l)
    return s, changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--jobs', type=int, default=4)
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('years', nargs='+')
    a = ap.parse_args()
    jobs = []
    for y in a.years:
        ps = pdfs(y)
        slugs = sorted(f[:-len('.positions.txt')] for f in os.listdir(os.path.join(REPO, 'raw', y)) if f.endswith('.positions.txt'))
        slugs = [s for s in slugs if s in ps]
        if a.limit:
            slugs = slugs[:a.limit]
        jobs += [(y, s, ps[s]) for s in slugs]
    tot = collections.defaultdict(collections.Counter)
    times, worst = [], []
    with cf.ThreadPoolExecutor(a.jobs) as ex:
        for year, slug, dst, dt in ex.map(run, jobs):
            if not dst:
                tot[year]['extraction failed'] += 1
                continue
            times.append(dt)
            before = open(os.path.join(REPO, 'raw', year, slug + '.positions.txt'), encoding='utf-8', errors='replace').read()
            after = open(dst, encoding='utf-8', errors='replace').read()
            s, ch = compare(before, after)
            tot[year] += s
            tot[year]['gazettes'] += 1
            worst += [(year, slug) + c for c in ch]
    for y in sorted(tot):
        print('==', y, dict(tot[y]))
    allc = sum(tot.values(), collections.Counter())
    print('== all', dict(allc))
    if times:
        times.sort()
        print('extraction time per gazette (new runs only): median %.1fs, max %.1fs' % (times[len(times) // 2], times[-1]))
    for w in worst[:15]:
        print('  word difference:', w)


if __name__ == '__main__':
    main()
