"""
Table lane scorecard (fix 7) - measurement only.

Reference: pdftotext -layout keeps every printed table row on one line
(two-column pages split at the gutter: extractor_eval.layout_rows).
A row is KEPT when ONE line of the cleaned pipeline text holds >= 90% of the
row's tokens in order and not much more (<= 1.5x the row + 3 tokens) -
stricter than the old "tokens in order anywhere"
score, because a table is only usable when its rows survive as rows.
A kept row has CELLS when that line carries the " | " cell separator.

Also checked, so the table lane cannot quietly hurt prose: notices found,
probate cause blocks and land notices templated.

Usage: python tools/table_eval.py [--positions-suffix positions] [--out NAME] 2022 2023 2024 2025 2026
  --positions-suffix: raw/<year>/<slug>.<suffix>.txt to score (default: positions)
Table pages come from reports/corpus/<year>/tables.csv (born-digital gazettes).
"""
import argparse, csv, json, os, re, subprocess, sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import extractor_eval as E
import corpus_report as R
import vocab_repair as VR
import gazette_clean as G
import category_census as CEN
import probate_template as P
import land_template as L

CORPUS = os.path.join(REPO, '..', 'Testing', 'Material', 'Atricle raw material', 'M5')


def layout(y, slug, pdf):
    p = os.path.join(REPO, 'raw', y, slug + '.layout.txt')
    if not os.path.exists(p):
        subprocess.run(['pdftotext', '-layout', '-enc', 'UTF-8', pdf, p], capture_output=True)
    return open(p, encoding='utf-8', errors='replace').read().split('\f')


def kept(row, lines, line_ix, share=0.9):
    """Index of a line holding >= 90% of the row's tokens in order, or -1."""
    anchors = [t for t in row if len(t) >= 3 and t in line_ix] or [t for t in row if t in line_ix]
    if not anchors:
        return -1
    a = min(anchors, key=lambda t: len(line_ix[t]))
    need = share * len(row)
    for li in line_ix[a][:200]:
        toks, j, got = lines[li], 0, 0
        # the row must stand on its own line: a paragraph that swallowed the
        # whole table also "contains" every row, but the rows are lost in it
        if len(toks) > 1.5 * len(row) + 3:
            continue
        for t in row:
            k = j
            while k < len(toks) and toks[k] != t:
                k += 1
            if k < len(toks):
                got += 1; j = k + 1
        if got >= need:
            return li
    return -1


def tesseract_rows(y, slug, page):
    """Table rows read from the page IMAGE (Tesseract word boxes, cells split
    at gaps > 2.5% of the page width): [(tokens, n_cells)]. Second referee:
    pdftotext -layout misaligns some tables by a row (2024 No 89 p28 pairs
    each parcel with the next row's owner), the image cannot."""
    p = os.path.join(REPO, 'reports', 'ocr', y, slug, 'p%04d.tsv.gz' % page)
    if not os.path.exists(p):
        return []
    lines, width = R.page_lines(p)
    out = []
    for ws in lines:
        cs = R.cells(ws, 0.025 * width)
        if len(cs) >= 3 and any(R.NUMERIC.match(''.join(w[3] for w in c)) for c in cs):
            t = E.TOK.findall(' '.join(w[3] for w in ws).lower())
            if len(t) >= 3:
                out.append((t, len(cs)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('years', nargs='+')
    ap.add_argument('--positions-suffix', default='positions')
    ap.add_argument('--out', default='current')
    ap.add_argument('--ref', default='layout', choices=['layout', 'tesseract'],
                    help='reference rows: pdftotext -layout, or Tesseract word boxes (image)')
    a = ap.parse_args()
    V = VR.load_corpus_vocab()
    report = {}
    for y in a.years:
        files = {}
        for r in csv.DictReader(open(os.path.join(REPO, 'reports', 'ocr', y, 'gazettes.csv'), encoding='utf-8')):
            if r['kind'] == 'born-digital':
                files[r['slug']] = r['file']
        pages = defaultdict(dict)
        for r in csv.DictReader(open(os.path.join(REPO, 'reports', 'corpus', y, 'tables.csv'), encoding='utf-8')):
            if r['slug'] in files:
                pages[r['slug']][int(r['page'])] = r['kind']
        T = defaultdict(int)
        by_kind = defaultdict(lambda: [0, 0, 0])
        for slug in sorted(pages):
            src = os.path.join(REPO, 'raw', y, '%s.%s.txt' % (slug, a.positions_suffix))
            if not os.path.exists(src):
                continue
            lay = layout(y, slug, os.path.join(CORPUS, y, files[slug]))
            text = G.apply_ascending_lock(VR.repair(G.clean(R.read_text(src)), V))
            out_lines = text.split('\n')
            toks = [E.TOK.findall(l.lower()) for l in out_lines]
            ix = defaultdict(list)
            for i, ts in enumerate(toks):
                for t in set(ts):
                    ix[t].append(i)
            for p, kind in pages[slug].items():
                if a.ref == 'tesseract':
                    refrows = tesseract_rows(y, slug, p)
                else:
                    refrows = [(r, None) for r in E.layout_rows(lay[p - 1])] if p - 1 < len(lay) else []
                for row, ncell in refrows:
                    # OCR misreads digits: the image referee allows 80%
                    li = kept(row, toks, ix, 0.8 if a.ref == 'tesseract' else 0.9)
                    T['rows'] += 1
                    by_kind[kind][0] += 1
                    if li >= 0:
                        T['kept'] += 1; by_kind[kind][1] += 1
                        if ' | ' in out_lines[li]:
                            T['cells'] += 1; by_kind[kind][2] += 1
                            if ncell is not None and out_lines[li].count(' | ') + 1 == ncell:
                                T['cells exact'] += 1
            # prose guard: notices and templates on the whole gazette
            for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text):
                if not n.startswith('GAZETTE NOTICE NO.'):
                    continue
                T['notices'] += 1
                c = CEN.categorise(n)
                if c == 'court_legal':
                    for b in P.split_causes(n):
                        T['probate blocks'] += 1; T['probate ok'] += bool(P.extract(b, n))
                elif c == 'land_property':
                    T['land'] += 1; T['land ok'] += bool(L.extract(n))
        pct = lambda k, d: 100.0 * T[k] / max(1, T[d])
        print('%s: table rows %d | kept on one line %.1f%% | with cells %.1f%% | cell count = image %.1f%% | notices %d | probate blocks %.1f%% | land %.1f%%'
              % (y, T['rows'], pct('kept', 'rows'), pct('cells', 'rows'), pct('cells exact', 'rows'), T['notices'],
                 pct('probate ok', 'probate blocks'), pct('land ok', 'land')))
        for k, (n, kp, c) in sorted(by_kind.items(), key=lambda kv: -kv[1][0]):
            print('      %-22s rows %5d  kept %5.1f%%  cells %5.1f%%' % (k, n, 100.0 * kp / n, 100.0 * c / n))
        report[y] = {'totals': dict(T), 'by_kind': {k: v for k, v in by_kind.items()}}
    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    json.dump(report, open(os.path.join(REPO, 'reports', 'eval', 'tables_%s.json' % a.out), 'w'), indent=1)


if __name__ == '__main__':
    main()
