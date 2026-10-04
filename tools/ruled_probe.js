// Measurement (fix 7b): ruled tables from the printed ruling lines.
//
// Lattice first: the ruling lines of the page (tools/pdf_rules.js) are
// grouped into lattices - >= 3 vertical walls whose spans overlap, with the
// horizontal rules between the outer walls. Each lattice is scored against
// the page's text:
//   rows      - rows (between consecutive horizontal rules) that hold text
//   cols      - columns (between walls) that hold text
//   items     - text items inside the lattice
//   straddle  - items cut by a horizontal rule (glyph top above it, baseline
//               below) - 0 in a real ruled table
//   vcross    - items running across a vertical wall by > 2 pt
//   inTable   - items the extractor already reads as table cells (a lattice
//               with none is a frame or box, not a table)
//   baselines - text lines in the lattice; baselines - rows = wrapped lines
//               the lattice joins to their row
// Pages without any table run are the control: a lattice there is suspect.
//
// Usage: node tools/ruled_probe.js < list   (lines "pdf<TAB>page<TAB>kind")
// stdout TSV: pdf page kind walls hrules rows cols items straddle vcross inTable baselines
const { readFileSync } = require('fs');
const { layout, itemsByPage } = require('./inspect_positions');
const { rules } = require('./pdf_rules');

function pageHeight(buf) {
  const s = buf.toString('latin1');
  const re = /\/MediaBox\s*\[\s*([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s*\]/g;
  const c = new Map();
  let m;
  while ((m = re.exec(s))) { const h = +(parseFloat(m[4]) - parseFloat(m[2])).toFixed(1); c.set(h, (c.get(h) || 0) + 1); }
  return c.size ? [...c.entries()].sort((a, b) => b[1] - a[1])[0][0] : 842;
}

const uniq = (vals, tol) => {
  const out = [];
  for (const v of vals.slice().sort((a, b) => a - b)) if (!out.length || v - out[out.length - 1] > tol) out.push(v);
  return out;
};

// lattices: vertical walls grouped by overlapping y spans
function lattices(R) {
  const walls = R.v.filter(l => l.y1 - l.y0 >= 12).sort((a, b) => a.y0 - b.y0);
  const groups = [];
  for (const w of walls) {
    const g = groups.find(g => w.y0 < g.y1 - 4 && w.y1 > g.y0 + 4);
    if (g) { g.walls.push(w); g.y0 = Math.min(g.y0, w.y0); g.y1 = Math.max(g.y1, w.y1); }
    else groups.push({ walls: [w], y0: w.y0, y1: w.y1 });
  }
  const out = [];
  for (const g of groups) {
    const xs = uniq(g.walls.map(w => w.x), 2);
    if (xs.length < 3) continue;
    const ys = uniq(R.h.filter(l => l.y >= g.y0 - 2 && l.y <= g.y1 + 2 && l.x0 <= xs[1] && l.x1 >= xs[xs.length - 2]).map(l => l.y), 1);
    if (ys.length < 3) continue;
    out.push({ xs, ys, walls: g.walls });
  }
  return out;
}

function score(L, H, its, tableItems) {
  const { xs, ys, walls } = L;
  let items = 0, straddle = 0, vcross = 0, inTable = 0;
  const rows = new Set(), cols = new Set(), base = new Set();
  for (const i of its) {
    const t = H - i.y - 0.7 * i.fontSize, b = H - i.y, cy = (t + b) / 2, cx = i.x + i.width / 2;
    if (cx < xs[0] || cx > xs[xs.length - 1] || cy < ys[0] || cy > ys[ys.length - 1]) continue;
    items++;
    base.add(Math.round(i.y));
    if (tableItems.has(i)) inTable++;
    const k = ys.findIndex((y, n) => n + 1 < ys.length && cy >= y && cy <= ys[n + 1]);
    if (k >= 0) rows.add(k);
    const c = xs.findIndex((x, n) => n + 1 < xs.length && cx >= x && cx <= xs[n + 1]);
    if (c >= 0) cols.add(c);
    if (ys.some(y => y > t + 1 && y < b - 1)) straddle++;
    // only a wall that exists at this height (a header cell spanning two
    // columns has no wall through it)
    if (walls.some(w => w.x > i.x + 2 && w.x < i.x + i.width - 2 && w.y0 < cy && w.y1 > cy)) vcross++;
  }
  return { rows: rows.size, cols: cols.size, items, straddle, vcross, inTable, baselines: base.size };
}

const bufs = new Map();
const list = readFileSync(0, 'utf8').split('\n').map(s => s.trim()).filter(Boolean).map(s => s.split('\t'));
for (const [pdf, p, kind] of list) {
  const pg = parseInt(p, 10);
  if (!bufs.has(pdf)) {
    bufs.clear();
    const buf = readFileSync(pdf);
    bufs.set(pdf, { H: pageHeight(buf), byPage: itemsByPage(buf) });
  }
  const { H, byPage } = bufs.get(pdf);
  const its = byPage.get(pg);
  if (!its) continue;
  const parts = layout(its.slice());
  const tableItems = new Set();
  for (const part of ['left', 'right', 'span']) for (const l of parts[part]) if (l.cells) l.parts.forEach(i => tableItems.add(i));
  const R = rules(pdf, pg);
  const Ls = lattices(R);
  const name = pdf.split(/[\\/]/).slice(-2).join('/');
  if (!Ls.length) process.stdout.write([name, pg, kind, 0, 0, 0, 0, 0, 0, 0, 0, 0].join('\t') + '\n');
  for (const L of Ls) {
    const s = score(L, H, its, tableItems);
    process.stdout.write([name, pg, kind, L.xs.length, L.ys.length, s.rows, s.cols, s.items, s.straddle, s.vcross,
      s.inTable, s.baselines].join('\t') + '\n');
  }
}
