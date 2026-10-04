"""
Ruled tables (fix 7b): the strict no-loss check. On every page the ruled
pass changed, the characters (ignoring spaces, cell bars and "---" header
marks) must be exactly those of the page before: text may move into rows and
cells and word breaks may change, but nothing is dropped or invented.

Usage: python tools/ruled_chars.py 2022 2023 2024 2025 2026
"""
import collections, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
STRIP = re.compile(r'[\s|]|-{3}')


def main(years):
    pages = bad = 0
    for y in years:
        d = os.path.join(REPO, 'raw', y)
        for f in sorted(os.listdir(d)):
            if not f.endswith('.ruled.txt'):
                continue
            b = open(os.path.join(d, f.replace('.ruled.', '.positions.')), encoding='utf-8', errors='replace').read().split('\f')
            a = open(os.path.join(d, f), encoding='utf-8', errors='replace').read().split('\f')
            for k, (x, z) in enumerate(zip(b, a)):
                if x == z:
                    continue
                pages += 1
                cx, cz = collections.Counter(STRIP.sub('', x)), collections.Counter(STRIP.sub('', z))
                if cx != cz:
                    bad += 1
                    if bad <= 10:
                        print('CHAR DIFF', y, f, 'p%d' % (k + 1), 'lost', dict(cx - cz), 'added', dict(cz - cx))
    print('changed pages %d | pages whose characters differ: %d' % (pages, bad))


if __name__ == '__main__':
    main(sys.argv[1:])
