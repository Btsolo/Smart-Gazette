"""
Tables of a notice as data (fix 8) - built on the table lane's " | " cells.

  tables(notice) -> [{'columns': [...] or None, 'rows': [[cell, ...], ...]}]

- A table is a run of cell rows (lines containing " | ").
- A short line between two rows (<= 8 words, no sentence end) is a wrapped
  cell ("Doe", "(Example Coffee Nursery)"); it is appended to the cell
  with the most letters in the row above, instead of breaking the table.
- The header is the first row when it holds no digit; a second digit-free
  row with the same number of cells is a two-level header and is merged
  into it ("Area" + "Acquire (Ha)" -> "Area Acquire (Ha)").
- Every row is padded / joined to the header width, so each row reads as
  column -> value. A row with more cells than the header keeps its extras
  joined into the last column (rare; counted by the scorecard).

Java port: TableExtractor (keep in sync).

Usage: python tools/table_extract.py 2022 2023 2024 2025 2026   (scorecard)
"""
import os, re, sys
from collections import Counter

# cell separator: a bar with whitespace (or the line start) before it and
# whitespace (or the line end) after it - so empty cells survive the cleaner's
# squashing: "a | | c", "text | |" (fix 7b); "a|b" inside text is not split
# a line holding only figure markers (docs/specs/figures.md)
FIG_LINE = re.compile(r'^(?:\[\[FIGURE:\d+\.\d+\]\]\s*)+$')
CELL_SEP = re.compile(r'(?:^|(?<=\s))\|(?=\s|$)')

HERE = os.path.dirname(os.path.abspath(__file__))
SENT_END = re.compile(r'[.;:]\s*$')


def _letters(s):
    return sum(ch.isalpha() for ch in s)


DASH_ROW = re.compile(r'^-{3}(?: \| -{3})+$')
# words that open a new section of a schedule (acquisition corrigenda: Deletion / Corrigenda / Addendum)
SECTION = re.compile(r'\b(?:deletion|addendum|corrigend\w*|amendment|schedule|part|annex|appendix)\b', re.I)


def _caption(lines):
    """The label above a table: the last short line(s) before it ("SCHEDULE
    Deletion", "Addendum"), or None."""
    t = ' '.join(lines).strip()
    return t if t and len(t.split()) <= 12 else None


def tables(notice):
    # cur: rows of the open table; pre[i]: short lines seen just before row i
    # (decided later: a wrapped cell of row i-1, or - when row i turns out to
    # be a header - the new table's caption, e.g. "Corrigenda" between two
    # tables of an acquisition corrigendum)
    out, cur, pre, pending, header, caption, last_text = [], None, [], [], None, None, []

    def close():
        nonlocal cur, pre, header, caption
        if cur and (len(cur) >= 2 or (header and cur)):
            for i in range(1, len(cur)):
                if pre[i]:                     # wrapped text of the row above
                    tgt = max(range(len(cur[i - 1])), key=lambda k: _letters(cur[i - 1][k]))
                    cur[i - 1][tgt] = (cur[i - 1][tgt] + ' ' + ' '.join(pre[i])).strip()
            t = _shape(cur, header)
            t['caption'] = caption
            out.append(t)
        cur, pre, header, caption = None, [], None, None

    for line in notice.split('\n'):
        s = line.strip()
        if FIG_LINE.match(s):
            continue                  # a figure marker between rows is not a wrapped cell (figures)
        if DASH_ROW.match(s):
            # the row above is the header (inspect_positions.js marks it from
            # the header's own typeface); rows before it in the same run are
            # either upper header levels (no digits) or a previous table
            if cur:
                h, hpre = cur.pop(), pre.pop()
                cap = _caption(hpre) or caption
                if any(re.search(r'\d', ' '.join(r)) for r in cur):
                    close()
                cur, pre, header, caption, pending = [], [], h, cap, []
            continue
        if ' | ' in s or s.endswith(' |'):        # last cell empty: "text |" (fix 7b)
            # an empty cell arrives as "a | | c" (the cleaner squashes "a |  | c")
            cells = [c.strip() for c in CELL_SEP.split(s)]
            if cur is None:
                cur, pre, caption = [], [], _caption(last_text[-1:])
            cur.append(cells); pre.append(pending)
            pending = []
        elif cur is not None and s and len(s.split()) <= 8 and not SENT_END.search(s) \
                and not s.startswith('GAZETTE NOTICE NO.') and len(pending) < 3:
            pending.append(s)
        else:
            close(); pending = []
            if s:
                last_text = [s]
    close()
    # A headerless piece directly after a table of the same width is that
    # table continued (a page break, "SCHEDULE-(Contd.)", a long wrapped cell
    # split it): its rows join the table above, under the same header.
    merged = []
    for t in out:
        prev = merged[-1] if merged else None
        # ... unless it carries its own caption ("Addendum" after "Deletion"):
        # that is a new section, not a continuation - except "(Contd.)"
        # (only a section word counts: other short lines above a piece are
        # wrapped names or meeting notes, and merged pieces drop them)
        own = t['caption'] and SECTION.search(t['caption']) and not re.search(r'cont(?:d|inued)', t['caption'], re.I)
        if prev and t['columns'] is None and not own and t['rows'] \
                and len(t['rows'][0]) == len(prev['rows'][0] if prev['rows'] else prev['columns'] or []):
            prev['rows'] += t['rows']; prev['raw_widths'] += t['raw_widths']
        else:
            merged.append(t)
    return merged


_HW = None
_STOP = {'of', 'in', 'and', 'the', 'to', 'for', 'a', 'by', 'or', 'on', 'at'}


def header_words():
    """Words seen in >= 3 headers that the extractor marked by typeface
    (tools/table_header_words.txt, learnt from the 2022-2026 corpus)."""
    global _HW
    if _HW is None:
        p = os.path.join(HERE, 'table_header_words.txt')
        _HW = {l.strip() for l in open(p, encoding='utf-8') if l.strip() and not l.startswith('#')}
    return _HW


def looks_like_header(row):
    """A header set in the body font ("Name | Position", "Lot No. | Vessel |
    Manifest No."): no digits, short cells, and >= 70% of its content words
    are words seen in typeface-marked headers. "Kipkelion | Kericho" is data."""
    if re.search(r'\d', ' '.join(row)) or any(len(c.split()) > 5 for c in row):
        return False
    words = [w for w in re.findall(r'[a-z]+', ' '.join(row).lower()) if w not in _STOP]
    return len(words) >= 2 and sum(w in header_words() for w in words) >= 0.7 * len(words)


def _shape(rows, cols=None):
    """cols = the header row marked by the extractor; otherwise the first row
    if it looks like a header by its words. (A digit-free first row is not
    enough: "Proposed Court | Supervising High Court" was merged with the
    data row "Kipkelion | Kericho" when that was the rule.)"""
    if not rows:
        rows = [[]]
    if cols is None and len(rows) > 1 and looks_like_header(rows[0]):
        cols, rows = rows[0], rows[1:]
    width = len(cols) if cols else Counter(len(r) for r in rows).most_common(1)[0][0]
    shaped = []
    split = []
    for r in rows:
        # two records side by side on one line (the page's two columns read
        # as one row): exactly twice the header width -> two rows
        if cols and width >= 2 and len(r) == 2 * width:
            split += [r[:width], r[width:]]
        else:
            split.append(r)
    rows = split
    for r in rows:
        if len(r) < width:
            r = r + [''] * (width - len(r))
        elif len(r) > width:
            r = r[:width - 1] + [' '.join(r[width - 1:])]
        shaped.append(r)
    return {'columns': cols, 'rows': shaped, 'raw_widths': [len(r) for r in rows]}


def main(years):
    sys.path.insert(0, HERE)
    import category_eval as E
    T = Counter()
    for y in years:
        for slug, num, n in E.cleaned_notices(y):
            ts = tables(n)
            if not ts:
                continue
            T['notices with tables'] += 1
            for t in ts:
                T['tables'] += 1
                T['tables with header'] += t['columns'] is not None
                w = len(t['columns']) if t['columns'] else None
                for rw in t['raw_widths']:
                    T['rows'] += 1
                    T['rows under header'] += t['columns'] is not None
                    T['rows exact width'] += (w is not None and rw == w) or (w is None)
    pct = lambda a, b: 100.0 * T[a] / max(1, T[b])
    print('notices with tables %d | tables %d, with a header %.1f%% | rows %d, under a header %.1f%%, matching the table width %.1f%%'
          % (T['notices with tables'], T['tables'], pct('tables with header', 'tables'), T['rows'],
             pct('rows under header', 'rows'), pct('rows exact width', 'rows')))


if __name__ == '__main__':
    main(sys.argv[1:] or ['2022', '2023', '2024', '2025', '2026'])
