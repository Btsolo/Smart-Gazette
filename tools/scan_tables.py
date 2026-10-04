"""
Tables in scanned gazettes (docs/specs/scan-tables.md).

The scan lane reads a page as Tesseract's word boxes (TSV, 300 dpi). Without
tables, page_text() gives exactly Tesseract's own text (checked on 90 pages).
With them:
  - ruled tables: the ruling lines found in the page image (image_rules)
    give rows and cells, as fix 7b does with a PDF's vector lines; the rules
    Tesseract misread as characters ("1319]", "__|") are dropped
  - unruled tables: runs of lines whose words form >= 3 groups separated by
    wide gaps; each group is a cell
  - elsewhere no cells: a standalone "|" is a misread "I" before a lowercase
    word ("| shall issue"), otherwise a speck, and goes
Java port: ScanTableReader (keep in sync).
"""
import gzip, os, re, subprocess, tempfile

import numpy as np
from PIL import Image

TMP = tempfile.gettempdir()
RULE_CHARS = set('|[]{}_')


# ---------------------------------------------------------------- word boxes
def read_words(tsv_path):
    """Tesseract TSV (gz) -> words in reading order: dict(key=(block, par, line), x, y, w, h, text)."""
    out = []
    with gzip.open(tsv_path, 'rt', encoding='utf-8', errors='replace') as f:
        next(f)
        for ln in f:
            c = ln.rstrip('\n').split('\t')
            if len(c) < 12 or c[0] != '5' or not c[11].strip():
                continue
            out.append({'key': (int(c[2]), int(c[3]), int(c[4])), 'x': int(c[6]), 'y': int(c[7]),
                        'w': int(c[8]), 'h': int(c[9]), 'text': c[11]})
    return out


def lines_of(words):
    """[(key, [words])] in reading order."""
    order, d = [], {}
    for w in words:
        if w['key'] not in d:
            d[w['key']] = []
            order.append(w['key'])
        d[w['key']].append(w)
    return [(k, d[k]) for k in order]


# ------------------------------------------------------------ ruling lines
def _runs(row, min_len):
    """(start, end) of runs of True of length >= min_len in a 1-D bool array"""
    p = np.concatenate(([False], row, [False]))
    d = np.flatnonzero(p[1:] != p[:-1])
    s, e = d[0::2], d[1::2]
    keep = (e - s) >= min_len
    return list(zip(s[keep], e[keep]))


def _join(segs, along_tol, across_tol=2):
    """segments (c, a0, a1) at cross position c from a0 to a1 -> lines; a
    segment continues a line when it is within 2 px across and touches along
    (tolerates a slight skew: the line's position follows its last segment)"""
    lines = []
    for c, a0, a1 in sorted(segs):
        for l in lines:
            if abs(l['c_end'] - c) <= across_tol and a0 <= l['a1'] + along_tol and a1 >= l['a0'] - along_tol:
                l['a0'], l['a1'] = min(l['a0'], a0), max(l['a1'], a1)
                l['cs'].append(c)
                l['c_end'] = c
                break
        else:
            lines.append({'a0': a0, 'a1': a1, 'cs': [c], 'c_end': c})
    return [(float(np.mean(l['cs'])), l['a0'], l['a1']) for l in lines]


def image_rules(img, scale, dpi=150):
    """ruling lines of a grey page image (dpi: the image's resolution) ->
    {'h': [(y, x0, x1)], 'v': [(x, y0, y1)]} in the coordinates of the word
    boxes (pixel * scale). Pixel sizes are given at 150 dpi and scale with dpi."""
    k = dpi // 150
    dark = img < 185                      # thin rules render as light grey
    H, W = dark.shape
    hs = [(y, s, e) for y in range(H) for s, e in _runs(dark[y], 25 * k)]
    vs = [(x, s, e) for x in range(W) for s, e in _runs(dark[:, x], 25 * k)]
    # pieces of a rule broken by the rendering join across small gaps; a rule
    # is then long (text strokes are not)
    h = [(c * scale, a0 * scale, a1 * scale) for c, a0, a1 in _join(hs, 12 * k, 2 * k) if a1 - a0 >= 0.06 * W]
    v = [(c * scale, a0 * scale, a1 * scale) for c, a0, a1 in _join(vs, 12 * k, 2 * k) if a1 - a0 >= 0.03 * H]
    return {'h': h, 'v': v, 'W': W * scale, 'H': H * scale}


def render(pdf, page, dpi=150):
    base = os.path.join(TMP, 'sct_%d_%d' % (os.getpid(), page))
    subprocess.run(['pdftoppm', '-r', str(dpi), '-gray', '-png', '-singlefile', '-f', str(page), '-l', str(page), pdf, base],
                   capture_output=True)
    try:
        return np.asarray(Image.open(base + '.png').convert('L'))
    finally:
        if os.path.exists(base + '.png'):
            os.remove(base + '.png')


def _uniq(vals, tol):
    out = []
    for v in sorted(vals):
        if not out or v - out[-1] > tol:
            out.append(v)
    return out


def lattices(R):
    """as tools/pdf_rules.js lattices(): >= 3 walls whose spans overlap, >= 3
    rules between the outer walls; the rule between the two page columns
    (long, near the centre, crossed by no rule) is not a wall"""
    W, H = R['W'], R['H']
    def gutter(v):
        x, y0, y1 = v
        return (y1 - y0 >= 0.4 * H and abs(x - W / 2) <= 0.04 * W
                and not any(y0 + 4 < hy < y1 - 4 and hx0 < x - 4 and hx1 > x + 4 for hy, hx0, hx1 in R['h']))
    walls = sorted((v for v in R['v'] if not gutter(v)), key=lambda v: v[1])
    groups = []
    for w in walls:
        for g in groups:
            if w[1] < g['y1'] - 8 and w[2] > g['y0'] + 8:
                g['walls'].append(w)
                g['y0'], g['y1'] = min(g['y0'], w[1]), max(g['y1'], w[2])
                break
        else:
            groups.append({'walls': [w], 'y0': w[1], 'y1': w[2]})
    out = []
    for g in groups:
        xs = _uniq([w[0] for w in g['walls']], 8)
        if len(xs) < 3:
            continue
        ys = _uniq([hy for hy, hx0, hx1 in R['h'] if g['y0'] - 6 <= hy <= g['y1'] + 6
                    and hx0 <= xs[1] + 4 and hx1 >= xs[-2] - 4], 6)
        if len(ys) < 3:
            continue
        out.append({'xs': xs, 'ys': ys, 'walls': g['walls']})
    return out


# ---------------------------------------------------------------- tables
def _clean_cell_word(w, walls_x):
    """a rule Tesseract read as a character: alone ("|", "__|") it goes; at
    the end / start of a word that touches a wall it is cut ("1319]")"""
    t = w['text']
    if set(t) <= RULE_CHARS:
        return ''
    tol = 1.5 * max(w['h'], 10)
    if t[-1] in '|]}' and any(abs(w['x'] + w['w'] - x) <= tol for x in walls_x):
        t = t.rstrip('|]}_')
    if t and t[0] in '|[{_' and any(abs(w['x'] - x) <= tol for x in walls_x):
        t = t.lstrip('|[{_')
    return t


def _split_at_bar(w, xs):
    """a word Tesseract read across a wall, with the rule inside it
    ("Elgeyo/Marakwet_|__130"): cut at the bar, each piece placed by its share
    of the word's width"""
    t = w['text']
    i = t.find('|')
    if i <= 0 or i >= len(t) - 1 or not any(w['x'] + 4 < x < w['x'] + w['w'] - 4 for x in xs):
        return [w]
    a, b = t[:i].rstrip('_'), t[i + 1:].lstrip('_')
    if not a or not b:
        return [w]
    cut = w['x'] + w['w'] * i / len(t)
    return [dict(w, text=a, w=max(1, int(cut - w['x']))), dict(w, text=b, x=int(cut), w=max(1, int(w['x'] + w['w'] - cut)))]


def _ruled_rows(L, words, R):
    """rows of one lattice: [(top, [cell texts])] and the words it took"""
    xs, ys, walls = L['xs'], L['ys'], L['walls']
    inside = [w for w in words if xs[0] <= w['x'] + w['w'] / 2 <= xs[-1] and ys[0] <= w['y'] + w['h'] / 2 <= ys[-1]]
    if len(inside) < 6:
        return None, []
    pieces = [p for w in inside for p in _split_at_bar(w, xs)]
    cols = list(zip(xs[:-1], xs[1:]))
    crosses = lambda h, c: h[1] <= c[0] + 6 and h[2] >= c[1] - 6
    rules = [h for h in R['h'] if ys[0] - 3 <= h[0] <= ys[-1] + 3 and any(crosses(h, c) for c in cols)]
    all_y = _uniq(ys + [h[0] for h in rules], 6)
    # each word goes to the band its centre is in (on a scan a rule piece can
    # be lost in one column, so no per-column rows here)
    grid = {}
    for w in pieces:
        cx, cy = w['x'] + w['w'] / 2, w['y'] + w['h'] / 2
        k = next((i for i, (a, b) in enumerate(cols) if a <= cx < b), 0 if cx < xs[0] else len(cols) - 1)
        r = min(max(0, len([y for y in all_y if y <= cy]) - 1), len(all_y) - 2)
        grid.setdefault(r, [[] for _ in cols])[k].append(w)
    walls_x = [v[0] for v in walls]
    # A band can hold several rows when the rules between them are too faint
    # to find (2022 No 169: five counties in one band). Its text lines that
    # start with something in the first column each open a row; a wrapped
    # cell's extra lines leave the first column empty.
    # the key column: the leftmost column holding text in most bands (a table
    # can have an empty margin column at its left edge)
    filled = [sum(1 for r in grid if grid[r][k]) for k in range(len(cols))]
    key = next(k for k in range(len(cols)) if filled[k] >= 0.5 * max(filled)) if grid else 0
    bands = []
    for r in sorted(grid):
        by_col = grid[r]
        # the band's printed lines as Tesseract grouped them (it keeps a code
        # centred in a tall row on the line it sits beside)
        by_key = {}
        for ps in by_col:
            for w in ps:
                by_key.setdefault(w['key'], []).append(w)
        lines = sorted(({'cy': min(w['y'] for w in ws), 'ws': ws} for ws in by_key.values()), key=lambda l: l['cy'])
        first_col = {id(w) for w in by_col[key]}
        starts = [i for i, l in enumerate(lines) if i == 0 or any(id(w) in first_col for w in l['ws'])]
        if len(starts) <= 1:
            bands.append((r, by_col))
            continue
        # rules were lost here, and which lines form a row is not printed (a
        # code centred in a two-line row sits between its lines): one row per
        # printed line, cells from the walls, nothing joined - never a guess
        for l in lines:
            ids = {id(w) for w in l['ws']}
            bands.append((r, [[w for w in ps if id(w) in ids] for ps in by_col]))
    rows = []
    for r, by_col in bands:
        bm = (all_y[r] + all_y[r + 1]) / 2
        ws = [v[0] for v in walls if v[1] < bm < v[2]]
        cells = []
        for k, ps in enumerate(by_col):
            if k > 0 and not any(abs(x - xs[k]) <= 8 for x in ws):
                cells[-1].extend(ps)
            else:
                cells.append(list(ps))
        texts = []
        for c in cells:
            # Tesseract's own lines, top to bottom, each left to right
            tops = {}
            for w in c:
                tops[w['key']] = min(tops.get(w['key'], w['y']), w['y'])
            c.sort(key=lambda w: (tops[w['key']], w['key'], w['x']))
            texts.append(' '.join(t for t in (_clean_cell_word(w, walls_x) for w in c) if t))
        if any(texts):
            rows.append((all_y[r], texts))
    if len(rows) < 2 or sum(1 for _, t in rows if sum(1 for x in t if x) >= 2) * 2 < len(rows):
        return None, []
    return rows, inside


def _groups(line_words):
    """a line's words split at wide gaps (> 2x the word height) and at a
    standalone bar"""
    ws = sorted(line_words, key=lambda w: w['x'])
    h = sorted(w['h'] for w in ws)[len(ws) // 2]
    groups, cur, end = [], [], None
    for w in ws:
        if set(w['text']) <= RULE_CHARS:
            if cur:
                groups.append(cur)
            cur, end = [], None
            continue
        if cur and w['x'] - end > 2 * h:
            groups.append(cur)
            cur = []
        cur.append(w)
        end = w['x'] + w['w']
    if cur:
        groups.append(cur)
    return groups


def _row_text(groups):
    return ' | '.join(' '.join(w['text'] for w in g) for g in groups)


def _stray_bars(ws):
    """no cells outside a table: "|" before a lowercase word is a misread "I",
    any other standalone bar is a speck"""
    out, glue = [], False
    for i, w in enumerate(ws):
        if w['text'] == '|':
            nxt = ws[i + 1]['text'] if i + 1 < len(ws) else ''
            if nxt[:1].islower():
                out.append('I')
            elif nxt[:1].isdigit():
                # a "1" read as a bar ("GAZETTE NOTICE NO. | 2057" = 12057): kept,
                # glued to its number, for the header / number repair
                glue = True
            continue
        out.append(('|' if glue else '') + w['text'])
        glue = False
    return ' '.join(out)


def page_text(words, R=None, stats=None):
    """the page as text: Tesseract's own text, with tables as " | " rows.
    stats (a dict, optional) counts ruled / unruled table rows and the
    lines written with cells, for the scorecard"""
    st = stats if stats is not None else {}
    lines = lines_of(words)
    # ruled tables
    taken, rows_at = set(), {}
    for L in (lattices(R) if R else []):
        rows, inside = _ruled_rows(L, [w for w in words if id(w) not in taken], R)
        if not rows:
            continue
        ids = {id(w) for w in inside}
        taken |= ids
        first = next(i for i, (k, ws) in enumerate(lines) if any(id(w) in ids for w in ws))
        # an empty cell is written "a | | c" (single spaces, as both cleaners keep it)
        rows_at.setdefault(first, []).extend(
            re.sub(' {2,}', ' ', ' | '.join(t) + (' |' if len(t) == 1 else '')).strip() for _, t in rows)
        st['ruled tables'] = st.get('ruled tables', 0) + 1
        st['ruled rows'] = st.get('ruled rows', 0) + len(rows)
    # unruled tables: runs of >= 3 consecutive lines with >= 3 groups
    groups = [(_groups([w for w in ws if id(w) not in taken]) if any(id(w) not in taken for w in ws) else [])
              for k, ws in lines]
    # a line belongs to an unruled table when it has >= 3 groups and shares
    # >= 2 group starts (besides the first) with a neighbouring such line - a
    # masthead ("date | THE KENYA GAZETTE | page") next to a table does not
    # every group must be cell-sized: a line that carries a column of prose
    # beside a table (2024 No 6 p51: Tesseract reads straight across both)
    # is not a table row
    page_w = max((w['x'] + w['w'] for w in words), default=1) - min((w['x'] for w in words), default=0)
    cellish = [all(g[-1]['x'] + g[-1]['w'] - g[0]['x'] <= 0.34 * page_w for g in gs) for gs in groups]
    groups = [gs if ok else [] for gs, ok in zip(groups, cellish)]
    starts = [[g[0]['x'] for g in gs] for gs in groups]
    hgt = [sorted(w['h'] for w in ws)[len(ws) // 2] for k, ws in lines]
    def shared(i, j):
        tol = 2 * max(hgt[i], hgt[j])
        return sum(1 for a in starts[i][1:] if any(abs(a - b) <= tol for b in starts[j][1:])) >= 2
    multi = [len(g) >= 3 and any(0 <= j < len(lines) and len(groups[j]) >= 3 and shared(i, j) for j in (i - 1, i + 1))
             for i, g in enumerate(groups)]
    in_table = [False] * len(lines)
    i = 0
    while i < len(lines):
        j = i
        while j < len(lines) and multi[j]:
            j += 1
        if j - i >= 3:
            for k in range(i, j):
                in_table[k] = True
        i = max(j, i + 1)
    out, prev = [], None
    for i, (k, ws) in enumerate(lines):
        if prev is not None and (k[0], k[1]) != (prev[0], prev[1]):
            out.append('')
        prev = k
        if i in rows_at:
            out.extend(rows_at[i])
        rest = [w for w in ws if id(w) not in taken]
        if not rest:
            continue
        if in_table[i]:
            out.append(_row_text(groups[i]))
            st['unruled rows'] = st.get('unruled rows', 0) + 1
        else:
            line = _stray_bars(rest)
            if ' | ' in line:
                st['cells outside tables'] = st.get('cells outside tables', 0) + 1
            out.append(line)
    return '\n'.join(out)
