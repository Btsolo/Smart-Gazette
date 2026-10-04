// PROTOTYPE (measurement): rebuild gazette text from pdf-inspector's
// extractTextWithPositions instead of extractText.
//
// extractText breaks words and numbers apart ("a dministration", "20 21").
// extractTextWithPositions returns whole lines with x/y/width/font, so we can
// (1) join pieces on the same baseline by their real gap, and
// (2) order the page as the Gazette prints it: two columns, top to bottom,
//     with full-width blocks (mastheads, wide tables) breaking the flow.
//
// Usage: node tools/inspect_positions.js "<gazette.pdf>" > out.txt
// Pages are separated by form feeds (\f) like pdftotext.
// As a module: { lines, layout, pageText, itemsByPage } (tools/ruled_probe.js).
const { readFileSync } = require('fs');
const { extractTextWithPositions, classifyPdf } = require('@firecrawl/pdf-inspector');
const { rulesForDoc, lattices } = require('./pdf_rules');

function lines(its) {
  // group into baselines: same y within 40% of the font size
  its.sort((a, b) => b.y - a.y || a.x - b.x);
  const out = [];
  for (const it of its) {
    const last = out[out.length - 1];
    if (last && Math.abs(last.y - it.y) <= 0.4 * Math.max(it.fontSize, last.fs)) {
      last.parts.push(it);
    } else {
      out.push({ y: it.y, fs: it.fontSize, parts: [it] });
    }
  }
  // table rows (fix 7): wide gaps (> 1.5 em) between pieces separate cells.
  // A line with >= 2 wide gaps (>= 3 cells) is a row. A line with ONE wide
  // gap is a row only inside a run of >= 3 consecutive such lines whose cells
  // start at the same x (an aligned 2-column table such as "1-6 | 50"); a
  // lone two-part line is a signature ("MR/3883998   Governor") and keeps
  // plain spaces, so template patterns over it are unchanged.
  for (const l of out) {
    l.parts.sort((a, b) => a.x - b.x);
    l.cellXs = [];
    const gaps = [];
    for (let k = 1; k < l.parts.length; k++) {
      const p = l.parts[k], q = l.parts[k - 1];
      const g = p.x - (q.x + q.width);
      if (g > 0.2 * p.fontSize) gaps.push(g);
      if (g > 1.5 * p.fontSize) l.cellXs.push(p.x);
    }
    // A justified prose line holding few words (the next word, a long parcel
    // number, did not fit) is stretched: EVERY gap is wide and they are all
    // about equal ("district | of | Kirinyaga, | registered" - 21 land notices
    // in 2023 lost their template). Table cells sit at uneven distances.
    if (gaps.length >= 2 && l.cellXs.length === gaps.length
        && Math.max(...gaps) - Math.min(...gaps) < 0.6 * l.fs) l.cellXs = [];
    l.cells = l.cellXs.length >= 2;
  }
  const aligned = (a, b) => a.cellXs.some(x => b.cellXs.some(y => Math.abs(x - y) < 3));
  for (let i = 0; i < out.length;) {
    let j = i;
    while (j + 1 < out.length && out[j + 1].cellXs.length >= 1 && out[j].cellXs.length >= 1
           && out[j].y - out[j + 1].y <= 2.5 * out[j].fs && aligned(out[j], out[j + 1])) j++;
    if (out[i].cellXs.length >= 1 && j - i + 1 >= 3) for (let k = i; k <= j; k++) out[k].cells = true;
    i = j + 1;
  }
  // Column starts of each table: in a narrow column a long value ("Location
  // 11/Gaitega/467") leaves less than 1.5 em before the next cell, so the
  // wide-gap rule misses that boundary. The columns are learnt from the rows
  // where the gap IS wide (x positions seen in >= 2 rows of this page column
  // - a wrapped cell can split one table into several runs), then every row
  // is split at those x positions; neighbouring lines whose pieces start on
  // a learnt column join the table.
  const runs = [];
  const xs = [];
  for (const l of out) if (l.cells) xs.push(...l.cellXs);
  const cols = xs.filter(x => xs.filter(y => Math.abs(x - y) < 3).length >= 2);
  for (let i = 0; i < out.length;) {
    if (!out[i].cells) { i++; continue; }
    let j = i;
    while (j + 1 < out.length && out[j + 1].cells && out[j].y - out[j + 1].y <= 3 * out[j].fs) j++;
    const onCol = l => l.parts.slice(1).some(p => cols.some(c => Math.abs(p.x - c) < 3));
    let a = i, b = j;
    while (a - 1 >= 0 && !out[a - 1].cells && out[a - 1].y - out[a].y <= 2.5 * out[a].fs && onCol(out[a - 1])) a--;
    while (b + 1 < out.length && !out[b + 1].cells && out[b].y - out[b + 1].y <= 2.5 * out[b].fs && onCol(out[b + 1])) b++;
    for (let k = a; k <= b; k++) { out[k].cells = true; out[k].cols = cols; }
    runs.push([a, b]);
    // Header rows (fix 8): a table's header is set in a different typeface
    // (bold is embedded as its own font: header "Parcel No." TT4, data TT1;
    // isBold itself is never set). Body font = the font of most cell pieces;
    // the first rows of the table whose pieces are ALL in another font are
    // the header, and get a "--- | ---" separator line after them.
    const fc = new Map();
    for (let k = a; k <= b; k++) for (const p of out[k].parts) fc.set(p.font, (fc.get(p.font) || 0) + 1);
    const body = [...fc.entries()].sort((x, y) => y[1] - x[1])[0][0];
    let k = a;
    while (k <= b && k < a + 3 && out[k].parts.length >= 2 && out[k].parts.every(p => p.font !== body)) k++;
    if (k > a && k <= b) out[k - 1].headerEnd = true;
    i = b + 1;
  }
  for (const l of out) {
    let s = '', end = null;
    const cols = l.cols || [];
    for (const p of l.parts) {
      const g = end === null ? 0 : p.x - end;
      const boundary = l.cells && end !== null
        && (g > 1.5 * p.fontSize || (g > 0.2 * p.fontSize && cols.some(c => Math.abs(p.x - c) < 3)));
      // a real gap (> 0.2 em) is a space; touching pieces are one word
      if (boundary) s = s.replace(/\s+$/, '') + ' | ';
      else if (end !== null && g > 0.2 * p.fontSize && !s.endsWith(' ')) s += ' ';
      s += p.text;
      end = p.x + p.width;
    }
    l.text = s.replace(/[ \t]+/g, ' ').trim();
    if (l.headerEnd) {
      const n = l.text.split(' | ').length;
      l.text += '\n' + Array(Math.max(2, n)).fill('---').join(' | ');
    }
    l.x0 = l.parts[0].x;
    l.x1 = Math.max(...l.parts.map(p => p.x + p.width));
  }
  out.runs = runs;        // table runs [first, last] line index (fix 7b)
  return out;
}

// One page: split the items into left column / right column / spanning
// (full-width) and read each part as lines.
function layout(its) {
  const minX = Math.min(...its.map(i => i.x));
  const maxX = Math.max(...its.map(i => i.x + i.width));
  const mid = (minX + maxX) / 2;
  // column start margins: left = leftmost x; right = most common x right of mid
  const rx = new Map();
  for (const it of its) if (it.x > mid) { const k = Math.round(it.x); rx.set(k, (rx.get(k) || 0) + 1); }
  const rightStart = rx.size ? [...rx.entries()].sort((a, b) => b[1] - a[1])[0][0] : mid;
  const nearMargin = x => Math.abs(x - minX) < 12 || Math.abs(x - rightStart) < 12;
  // table rows: a baseline with >= 2 wide gaps (> 1.5 em) between pieces,
  // i.e. >= 3 cells, at least one not starting at a column margin. Two-column
  // prose has one wide gap (the gutter); small-caps and bold pieces touch.
  const rows = new Map();
  for (const it of its) {
    const k = Math.round(it.y);
    const key = [k - 1, k, k + 1].find(kk => rows.has(kk));
    if (key !== undefined) rows.get(key).push(it); else rows.set(k, [it]);
  }
  const tableItem = new Set();
  const flagged = [];
  for (const r of rows.values()) {
    if (r.length < 3) continue;
    r.sort((a, b) => a.x - b.x);
    let gaps = 0, cellStarts = [r[0]];
    for (let k = 1; k < r.length; k++) {
      if (r[k].x - (r[k - 1].x + r[k - 1].width) > 1.5 * r[k].fontSize) { gaps++; cellStarts.push(r[k]); }
    }
    // full-width table: off-margin cells on BOTH sides of the gutter. A
    // signature block ("MR/3883998   Governor, ...") has an indented cell on
    // one side only and must stay in its column.
    // ...and >= 2 cells on EACH side of the gutter (or a cell crossing it):
    // an in-column table beside indented prose (2023 No 2 p33) must stay in
    // its column.
    const offL = cellStarts.some(it => it.x < mid && !nearMargin(it.x));
    const offR = cellStarts.some(it => it.x >= mid && !nearMargin(it.x));
    const nL = cellStarts.filter(it => it.x < mid).length, nR = cellStarts.length - nL;
    const crosses = r.some(it => it.x < mid - 5 && it.x + it.width > mid + 5);
    if (gaps >= 2 && offL && offR && ((nL >= 2 && nR >= 2) || crosses)) flagged.push(r);
  }
  // A single such line is usually a coincidence (a centred heading next to a
  // signature or a short list). A table has several rows: keep only runs of
  // >= 3 flagged lines with <= 2.5 line heights between neighbours.
  flagged.sort((a, b) => b[0].y - a[0].y);
  let run = [];
  const flush = () => { if (run.length >= 3) run.forEach(r => r.forEach(it => tableItem.add(it))); run = []; };
  for (const r of flagged) {
    const prev = run[run.length - 1];
    if (prev && prev[0].y - r[0].y > 2.5 * r[0].fontSize) flush();
    run.push(r);
  }
  flush();
  // split items into left / right / spanning (spanning = crosses the gutter or table row)
  const left = [], right = [], span = [];
  for (const it of its) {
    if (tableItem.has(it) || (it.x < mid - 5 && it.x + it.width > mid + 5)) span.push(it);
    else if (it.x < mid) left.push(it);
    else right.push(it);
  }
  return { left: lines(left), right: lines(right), span: lines(span).sort((a, b) => b.y - a.y), mid };
}

// ---- Ruled tables (fix 7b, docs/specs/ruled-tables.md) ----------------
// The printed ruling lines (pdf_rules.js) say which text belongs to which
// row and cell, including cells that wrap onto several lines. Coordinates of
// the rules are top-left; text items have their baseline y from the bottom,
// so an item's middle is at H - y - 0.35 em from the top.
const midY = (i, H) => H - i.y - 0.35 * i.fontSize;

// A text item running across a wall that exists at its height holds two
// cells ("13/12/2022 2022JKAHI263", 2024 No 116): split it at the space
// nearest the wall. No space near the wall: the item stays whole.
function splitAtWalls(i, walls, H) {
  const cy = midY(i, H);
  const ws = walls.filter(w => w.y0 < cy && w.y1 > cy && w.x > i.x + 2 && w.x < i.x + i.width - 2)
    .map(w => w.x).sort((a, b) => a - b);
  if (!ws.length) return [i];
  const t = i.text, n = t.length, cuts = [];
  // where each character starts, from typical Times widths (capitals are
  // wide, spaces and punctuation narrow): counting characters as equal put
  // the cut a word late in a line of capitals ("REFERRAL ... Kenyatta 19th",
  // 2023 No 2 p27)
  const cw = ch => /[A-Z]/.test(ch) ? 0.68 : /[mwMW]/.test(ch) ? 0.75 : /[a-z0-9]/.test(ch) ? 0.48
    : ch === ' ' ? 0.25 : 0.3;
  const at = [0];
  for (let k = 0; k < n; k++) at.push(at[k] + cw(t[k]));
  const scale = i.width / at[n];
  const indexAt = x => { let k = 0; while (k < n && i.x + at[k + 1] * scale < x) k++; return k; };
  for (const x of ws) {
    const est = indexAt(x);
    let best = -1;
    for (let d = 0; d <= Math.max(3, Math.round(0.3 * n)); d++) {
      if (t[est - d] === ' ') { best = est - d; break; }
      if (t[est + d] === ' ') { best = est + d; break; }
    }
    if (best > 0 && best < n - 1 && !cuts.includes(best)) cuts.push(best);
  }
  if (!cuts.length) return [i];
  cuts.sort((a, b) => a - b);
  const out = [];
  let s = 0;
  for (const c of [...cuts, n]) {
    // a piece keeps the space it was cut at (and the item its own trailing
    // space): positions of pieces are estimates, so the space - not the
    // gap - says where a word ends ("Gender Year " + "of" -> "Year of")
    const piece = t.slice(s, c < n ? c + 1 : n);
    if (piece.trim()) out.push({ ...i, text: piece, x: i.x + at[s] * scale, width: (at[c] - at[s]) * scale });
    s = c + 1;
  }
  return out;
}

// text of one cell: its baselines top to bottom (grouped like lines(): a
// raised ordinal "rd" belongs to the baseline of its "3"), each read left to
// right; pieces that touch are one word
function cellText(ps) {
  ps.sort((a, b) => b.y - a.y || a.x - b.x);
  const bl = [];
  for (const p of ps) {
    const last = bl[bl.length - 1];
    if (last && Math.abs(last.y - p.y) <= 0.4 * Math.max(p.fontSize, last.fs)) last.parts.push(p);
    else bl.push({ y: p.y, fs: p.fontSize, parts: [p] });
  }
  const out = [];
  for (const l of bl) {
    l.parts.sort((a, b) => a.x - b.x);
    let s = '', end = null;
    for (const p of l.parts) {
      // a gap is a word break; so is a strong overlap - overlapping text
      // runs are not one word ("21--100" over "50", 2024 No 213 p25)
      const g = end === null ? 0 : p.x - end;
      if (end !== null && (g > 0.2 * p.fontSize || g < -0.3 * p.fontSize) && !s.endsWith(' ')) s += ' ';
      s += p.text;
      end = p.x + p.width;
    }
    out.push(s);
  }
  return out.join(' ').replace(/\s+/g, ' ').trim();
}

// The ruled tables of one page: the items they hold, and one line per row
// for the page's left column, right column or full width.
function ruledTables(its, R, H, mid) {
  const used = new Set();
  const out = { left: [], right: [], span: [] };
  for (const L of lattices(R, H * 595 / 842, H)) {
    const { xs, ys, walls } = L;
    const inside = its.filter(i => {
      const cx = i.x + i.width / 2, cy = midY(i, H);
      return !used.has(i) && cx >= xs[0] && cx <= xs[xs.length - 1] && cy >= ys[0] && cy <= ys[ys.length - 1];
    });
    if (!inside.length) continue;
    const pieces = inside.flatMap(i => splitAtWalls(i, walls, H));
    // Rows: every rule that fully crosses at least one column (an underline
    // never does). A column's own rules bound its cells, so a cell merged
    // downwards (2026 No 103 p50: "A. Regional Master meters" beside three
    // sub-rows) belongs to the row where it starts, and the sub-rows stay rows.
    const cols = xs.slice(0, -1).map((x, k) => [x, xs[k + 1]]);
    const crosses = (l, [c0, c1]) => l.x0 <= c0 + 2 && l.x1 >= c1 - 2;
    const rules = R.h.filter(l => l.y >= ys[0] - 1 && l.y <= ys[ys.length - 1] + 1 && cols.some(c => crosses(l, c)));
    const allY = [...new Set([...ys, ...rules.map(l => l.y)].map(y => Math.round(y * 2) / 2))].sort((p, q) => p - q);
    const colY = cols.map(c => [...new Set([ys[0], ys[ys.length - 1], ...rules.filter(l => crosses(l, c)).map(l => l.y)]
      .map(y => Math.round(y * 2) / 2))].sort((p, q) => p - q));
    const grid = new Map();                        // row index -> column index -> pieces
    for (const p of pieces) {
      const cx = p.x + p.width / 2, cy = midY(p, H);
      let k = cols.findIndex(([c0, c1]) => cx >= c0 && cx < c1);
      if (k < 0) k = cx < xs[0] ? 0 : cols.length - 1;
      const tops = colY[k].filter(y => y <= cy);
      const top = tops.length ? tops[tops.length - 1] : colY[k][0];
      let r = allY.indexOf(top);
      if (r < 0) r = Math.max(0, allY.filter(y => y <= top).length - 1);
      if (r >= allY.length - 1) r = allY.length - 2;
      if (!grid.has(r)) grid.set(r, cols.map(() => []));
      grid.get(r)[k].push(p);
    }
    const rows = [];
    for (const r of [...grid.keys()].sort((p, q) => p - q)) {
      const byCol = grid.get(r);
      // the walls of THIS row: a cell merged sideways has no wall through it
      const bm = (allY[r] + allY[r + 1]) / 2;
      const ws = walls.filter(w => w.y0 < bm && w.y1 > bm).map(w => w.x);
      const cells = [];
      byCol.forEach((ps, k) => {
        if (k > 0 && !ws.some(x => Math.abs(x - xs[k]) <= 2)) cells[cells.length - 1].push(...ps);
        else cells.push([...ps]);
      });
      const texts = cells.map(c => cellText(c));
      if (texts.every(t => !t)) continue;
      const ps = byCol.flat();
      rows.push({ texts, ps, y: Math.max(...ps.map(p => p.y)), fs: Math.max(...ps.map(p => p.fontSize)) });
    }
    if (rows.length < 2) continue;                 // not a table: leave the text to the normal layout
    // prose under walls that are not a table fills one cell per row; a table
    // fills several in most rows
    if (rows.filter(r => r.texts.filter(Boolean).length >= 2).length * 2 < rows.length) continue;
    inside.forEach(i => used.add(i));
    // header (fix 8): the first rows set entirely in another font than the
    // body. The body font is counted below the first row: in a table with one
    // data row the header's font would otherwise tie (2026 No 103 p35)
    // A label row merged across the table (a venue / date) can sit above the
    // header (2025 No 164 notice 10358): the header starts after such rows
    let s = 0;
    while (s < rows.length - 1 && rows[s].texts.length === 1) s++;
    const fc = new Map();
    for (const r of rows.slice(s + 1)) for (const p of r.ps) fc.set(p.font, (fc.get(p.font) || 0) + 1);
    const body = fc.size ? [...fc.entries()].sort((p, q) => q[1] - p[1])[0][0] : null;
    let h = s;
    while (h < rows.length && h < 3 && rows[h].texts.filter(Boolean).length >= 2 && rows[h].ps.every(p => p.font !== body)) h++;
    const place = xs[0] < mid - 5 && xs[xs.length - 1] > mid + 5 ? 'span' : (xs[0] + xs[xs.length - 1]) / 2 < mid ? 'left' : 'right';
    rows.forEach((r, k) => {
      // a row merged across the whole table (a date / venue / section label)
      // is still a row of the table: "label |" keeps the table together downstream
      // an empty cell is written "a | | c" (single spaces, as the Python and
      // Java cleaners both keep it)
      let text = (r.texts.length === 1 ? r.texts[0] + ' |' : r.texts.join(' | ')).replace(/ {2,}/g, ' ').trim();
      if (h > s && h < rows.length && k === h - 1) text += '\n' + Array(Math.max(2, r.texts.length)).fill('---').join(' | ');
      out[place].push({ y: r.y, fs: r.fs, text, x0: xs[0], x1: xs[xs.length - 1], cells: true, ruled: true });
    });
  }
  return { used, out };
}

const HDR = /^G\s*A\s*Z\s*E\s*T\s*T?\s*E\s+N\s*O\s*T\s*I\s*C\s*E\s+N\s*O/;

// Figures (docs/specs/figures.md): each image placement gets a marker
// "[[FIGURE:p.k]]". Inside a ruled table it is a pseudo word in the cell the
// figure sits in (a party symbol stays with its candidate); elsewhere it is
// placed AFTER the layout, into its page column at its height - a marker never
// takes part in layout decisions (a wide chart taken as a full-width line cut
// the page into bands and reordered the text: 2023 No 175 p3). No space
// inside it: a marker is never split at a cell wall.
function figureItems(R, H, pg) {
  return ((R && R.figures) || []).map((f, k) => ({
    text: '[[FIGURE:' + pg + '.' + (k + 1) + ']]', x: f.x0, width: Math.max(1, f.x1 - f.x0),
    y: H - f.y0 - 8, height: 8, fontSize: 8, font: '__FIG', page: pg, figure: true,
  }));
}

// One page's text in reading order. R / H: the page's ruling lines and the
// page height (fix 7b); without them the page reads exactly as before.
// pg: the page number, for the figure markers.
function pageText(its, R, H, pg) {
  const figs = figureItems(R, H, pg);
  if (!its.length && !figs.length) return '';
  const mid = its.length ? (Math.min(...its.map(i => i.x)) + Math.max(...its.map(i => i.x + i.width))) / 2 : H * 595 / 842 / 2;
  let ruled = null;
  if (its.length && R && H && R.v.length >= 3) {
    ruled = ruledTables(its.concat(figs), R, H, mid);
    if (![...ruled.used].some(i => !i.figure)) ruled = null;
  }
  const rest = ruled ? its.filter(i => !ruled.used.has(i)) : its;
  const lay = rest.length ? layout(rest) : { left: [], right: [], span: [] };
  if (ruled) {
    for (const k of ['left', 'right', 'span']) lay[k] = lay[k].concat(ruled.out[k]).sort((a, b) => b.y - a.y);
  }
  // the other figures' markers: into their page column at their height
  for (const f of figs) {
    if (ruled && ruled.used.has(f)) continue;
    const line = { y: f.y + 8, fs: 8, text: f.text, x0: f.x, x1: f.x + f.width };
    (f.x + f.width / 2 < mid ? lay.left : lay.right).push(line);
  }
  lay.left.sort((a, b) => b.y - a.y);
  lay.right.sort((a, b) => b.y - a.y);
  const { left: L, right: Rr, span: spanLines } = lay;
  // spanning lines cut the page into horizontal bands; inside a band read
  // the left column, then the right column
  const cuts = spanLines.map(l => l.y);
  const bandOf = y => { let k = 0; while (k < cuts.length && y < cuts[k]) k++; return k; };
  const out = [];
  for (let band = 0; band <= cuts.length; band++) {
    const lb = L.filter(l => bandOf(l.y) === band);
    const rb = Rr.filter(l => bandOf(l.y) === band);
    // A notice header stranded at the foot of the left column, below the
    // last right-column line, introduces the full-width block that follows
    // (2022 No 120 p21: 7432 sits under 7430 but starts after 7431).
    // Also when the left column simply ENDS with a header (+ <= 3 heading
    // lines) and the right column opens with its own new notice: then the
    // header's text cannot continue in the right column, so it belongs to the
    // full-width block below (2023 No 44 p64: 2640). In normal flow a column
    // that ends with a header continues at the top of the next column, which
    // then does NOT start with a header - left untouched.
    // Notice numbers ascend through an issue (lesson 14), so a moved header
    // must also be numbered ABOVE every header in the right column - this
    // keeps unbalanced columns (2023 No 2 p33: 118, 119 | 120, 121) intact.
    let tail = [];
    if (rb.length && lb.length && band < cuts.length) {
      const num = s => { const m = s.match(/N\s*O\s*\.?\s*([\d\s]+)/); return m ? parseInt(m[1].replace(/\s/g, ''), 10) : NaN; };
      const rNums = rb.filter(l => HDR.test(l.text)).map(l => num(l.text)).filter(n => !isNaN(n));
      const after = l => !rNums.length || num(l.text) > Math.max(...rNums);
      const lowR = Math.min(...rb.map(l => l.y));
      const t = lb.filter(l => l.y < lowR - 0.5 * l.fs);
      if (t.length && t.length <= 4 && HDR.test(t[0].text) && after(t[0])) tail = t;
      else {
        let h = -1;
        for (let k = lb.length - 1; k >= Math.max(0, lb.length - 4); k--) if (HDR.test(lb[k].text)) { h = k; break; }
        if (h >= 0 && rNums.length && after(lb[h])) tail = lb.slice(h);
      }
    }
    for (const l of lb) if (!tail.includes(l)) out.push(l.text);
    for (const l of rb) out.push(l.text);
    for (const l of tail) out.push(l.text);
    if (band < spanLines.length) out.push(spanLines[band].text);
  }
  return out.join('\n');
}

// The text items of a PDF, by page (1-based).
function itemsByPage(buf) {
  const byPage = new Map();
  for (const it of extractTextWithPositions(buf)) {
    if (it.itemType !== 'Text' || !it.text || !it.text.trim()) continue;
    if (!byPage.has(it.page)) byPage.set(it.page, []);
    byPage.get(it.page).push(it);
  }
  return byPage;
}

module.exports = { lines, layout, pageText, itemsByPage, ruledTables };

// RULED=0 turns the ruled-table pass off (A/B measurement only)
if (require.main === module) {
  const buf = readFileSync(process.argv[2]);
  const byPage = itemsByPage(buf);
  // every PDF page, including trailing pages without a text layer (scanned
  // maps: 2025 No 163 came out as 8 pages of 115) - they are OCR'd downstream
  // (docs/specs/mixed-pages.md)
  let pageCount = 0;
  try { pageCount = classifyPdf(buf).pageCount; } catch (e) { pageCount = 0; }
  const maxPage = Math.max(0, pageCount, ...byPage.keys());
  const doc = process.env.RULED === '0' ? { height: null, pages: new Map() } : rulesForDoc(process.argv[2], maxPage);
  const H = doc.height || 842;
  const pagesOut = [];
  for (let pg = 1; pg <= maxPage; pg++) pagesOut.push(pageText(byPage.get(pg) || [], doc.pages.get(pg), H, pg));
  // the figure list beside the text: --figures <file.json>
  const fi = process.argv.indexOf('--figures');
  if (fi > 0 && process.argv[fi + 1]) {
    const figs = [];
    for (let pg = 1; pg <= maxPage; pg++) {
      ((doc.pages.get(pg) || {}).figures || []).forEach((f, k) => figs.push({
        id: pg + '.' + (k + 1), page: pg, x0: f.x0, y0: f.y0, x1: f.x1, y1: f.y1, src: f.src, px: f.px, pageHeight: H,
      }));
    }
    require('fs').writeFileSync(process.argv[fi + 1], JSON.stringify(figs));
  }
  process.stdout.write(pagesOut.join('\n\f'));
}
