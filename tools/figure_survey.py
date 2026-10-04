"""
Survey (Phase 4c): what does the Gazette print besides text and tables?

For every gazette (2022-2026): every embedded image (pdfimages -list) with
its page, printed size and colour, sorted into kinds by printed size and
place; plus, per page, how much vector drawing there is that is not a table
rule (curves = maps, charts, logos drawn as vectors) - measured on a sample.

Usage: python tools/figure_survey.py > reports/corpus/figures.txt
"""
import collections, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')


def images(pdf):
    out = []
    lst = subprocess.run(['pdfimages', '-list', pdf], capture_output=True, text=True, errors='replace').stdout.splitlines()[2:]
    for ln in lst:
        f = ln.split()
        try:
            page, w, h, color, xppi, yppi = int(f[0]), int(f[3]), int(f[4]), f[5], float(f[12]), float(f[13])
            typ = f[2]
        except (IndexError, ValueError):
            continue
        if typ != 'image' or xppi <= 0 or yppi <= 0:
            continue                       # masks / smasks are not separate pictures
        out.append({'page': page, 'w_in': w / xppi, 'h_in': h / yppi, 'color': color, 'px': (w, h)})
    return out


def kind(im):
    a = im['w_in'] * im['h_in']
    if im['w_in'] >= 6.5 and im['h_in'] >= 9.0:
        return 'page-sized (scan or full-page figure)'
    if a >= 12:
        return 'large figure (>= 12 sq in: maps, charts, plans)'
    if a >= 2:
        return 'medium (2-12 sq in: photos, small maps, stamps)'
    if a >= 0.25:
        return 'small (0.25-2 sq in: logos, coat of arms, signatures)'
    return 'tiny (< 0.25 sq in: marks, bullets)'


def main():
    by_kind = collections.Counter()
    by_kind_issue = collections.defaultdict(set)
    color = collections.Counter()
    per_year = collections.defaultdict(collections.Counter)
    examples = collections.defaultdict(list)
    for y in ['2022', '2023', '2024', '2025', '2026']:
        for f in sorted(os.listdir(os.path.join(CORPUS, y))):
            if not f.lower().endswith('.pdf'):
                continue
            ims = images(os.path.join(CORPUS, y, f))
            for im in ims:
                k = kind(im)
                by_kind[k] += 1
                by_kind_issue[k].add((y, f))
                color[(k, im['color'])] += 1
                per_year[y][k] += 1
                if len(examples[k]) < 400:
                    examples[k].append((y, f, im['page'], round(im['w_in'], 1), round(im['h_in'], 1), im['color']))
    print('== images by kind (count | issues)')
    for k, v in by_kind.most_common():
        print('  %-55s %6d | %3d issues' % (k, v, len(by_kind_issue[k])))
    print('\n== by year')
    for y in sorted(per_year):
        print('  ', y, dict(per_year[y]))
    print('\n== colour')
    for (k, c), v in sorted(color.items()):
        print('   %-55s %-6s %d' % (k, c, v))
    print('\n== examples (year file page width height colour)')
    for k in examples:
        print('  #', k)
        for e in examples[k][:400:40]:
            print('     ', e)


if __name__ == '__main__':
    main()
