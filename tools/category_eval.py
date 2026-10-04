"""
Category routing scorecard - measurement only (fix 6).

For every notice in the positions text of the given years it records:
  python   tools/category_census.categorise (the Python categoriser)
  java     corpus_report.java_prefilter (exact port of GazetteService.keywordPreFilter;
           None = the notice goes to the AI triage call)
  heading  the home its heading block points to (misc_review.RULES on Act + subject)
The heading is the referee: it is structural, printed once per notice, and
independent of the body keywords being tested (same idea as lesson 12).

Cleaned text is cached in cleaned/<year>/<slug>.txt (gitignored) so a rule
change can be re-scored in seconds; pass --rebuild after a cleaner change.

Usage: python tools/category_eval.py [--rebuild] [--out NAME] 2022 2023 2024 2025 2026
Writes reports/eval/category_<NAME>.json and prints a table.
"""
import argparse, glob, json, os, re, sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import corpus_report as R
import misc_review as M
import vocab_repair as VR


def cleaned_notices(year, rebuild=False):
    out_dir = os.path.join(REPO, 'cleaned', year)
    os.makedirs(out_dir, exist_ok=True)
    V = None
    for p in sorted(glob.glob(os.path.join(REPO, 'raw', year, '*.positions.txt'))):
        slug = os.path.basename(p)[:-len('.positions.txt')]
        cache = os.path.join(out_dir, slug + '.txt')
        if rebuild or not os.path.exists(cache):
            V = V or VR.load_corpus_vocab()
            c = R.G.apply_ascending_lock(VR.repair(R.G.clean(R.read_text(p)), V))
            open(cache, 'w', encoding='utf-8').write(c)
        c = open(cache, encoding='utf-8').read()
        for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', c):
            m = re.match(r'GAZETTE NOTICE NO\. (\d+)', n)
            if m:
                yield slug, int(m.group(1)), n


def score(years, rebuild=False):
    rows = []
    for y in years:
        for slug, num, n in cleaned_notices(y, rebuild):
            act, subj = R.heading(n)
            h, _ = M.home({'act': act, 'subject': subj})
            rows.append({'year': y, 'slug': slug, 'notice': num,
                         'python': R.canon(R.CEN.categorise(n)),
                         'java': R.java_prefilter(re.sub(r'\s+', '', n.lower()), n) or '(AI triage)',
                         'heading': h or '', 'act': act, 'subject': subj})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('years', nargs='+')
    ap.add_argument('--rebuild', action='store_true')
    ap.add_argument('--out', default='current')
    a = ap.parse_args()
    rows = score(a.years, a.rebuild)

    print('notices: %d\n' % len(rows))
    for side in ('python', 'java'):
        print('%s categories by year' % side)
        per = defaultdict(Counter)
        for r in rows:
            per[r[side]][r['year']] += 1
        print('%-24s' % '' + ''.join('%7s' % y for y in a.years) + '   total')
        for cat in sorted(per, key=lambda c: -sum(per[c].values())):
            print('%-24s' % cat + ''.join('%7d' % per[cat][y] for y in a.years) + '%8d' % sum(per[cat].values()))
        print()

    # agreement with the heading, for each heading home
    print('%-38s %6s  %-34s  %-34s' % ('heading home', 'n', 'python files them as (top 3)', 'java files them as (top 3)'))
    by_home = defaultdict(list)
    for r in rows:
        if r['heading']:
            by_home[r['heading']].append(r)
    for h in sorted(by_home, key=lambda k: -len(by_home[k])):
        rs = by_home[h]
        top = lambda side: ', '.join('%s %d' % kv for kv in Counter(r[side] for r in rs).most_common(3))
        print('%-38s %6d  %-34s  %-34s' % (h[:38], len(rs), top('python')[:34], top('java')[:34]))

    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    path = os.path.join(REPO, 'reports', 'eval', 'category_%s.json' % a.out)
    json.dump(rows, open(path, 'w', encoding='utf-8'))
    print('\nwrote', path)


if __name__ == '__main__':
    main()
