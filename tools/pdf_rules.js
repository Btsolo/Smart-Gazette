// Ruling lines and ruled-table lattices of PDF pages (fix 7b,
// docs/specs/ruled-tables.md).
//
// The lines the printer drew, read from the pages' vector drawing commands
// via poppler's `pdftocairo -svg` (glyphs are <symbol>s in <defs> and are
// skipped; rules are stroked lines or thin filled rectangles in the page
// body). Prose has none.
//
// pdf-inspector's detectVectorGridInRegion was tried first: it finds a grid
// only on a whole page, its column walls are not the printed rules, and its
// rows follow text lines (one "row" per line of prose) - it cannot tell a
// ruled table from prose.
//
// Coordinates: PDF points, origin top-left (y grows downwards).
//   rules(pdf, page)          -> { h: [{ y, x0, x1 }], v: [{ x, y0, y1 }] }
//   rulesForDoc(pdf, nPages)  -> { height, pages: Map(page -> rules) }
//   lattices(rules)           -> [{ xs, ys, walls }]
const { spawnSync } = require('child_process');

const BLOCK = 50;          // pages per pdftocairo run (bounds the SVG size)

const MUL = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1],
  m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3],
  m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
const ID = [1, 0, 0, 1, 0, 0];

function parseTransform(attrs) {
  const t = /transform="([^"]*)"/.exec(attrs);
  if (!t) return ID;
  let m = ID;
  const re = /(matrix|translate|scale)\(([^)]*)\)/g;
  let x;
  while ((x = re.exec(t[1]))) {
    const v = x[2].split(/[\s,]+/).filter(Boolean).map(Number);
    if (x[1] === 'matrix') m = MUL(m, v);
    else if (x[1] === 'translate') m = MUL(m, [1, 0, 0, 1, v[0] || 0, v[1] || 0]);
    else m = MUL(m, [v[0], 0, 0, v.length > 1 ? v[1] : v[0], 0, 0]);
  }
  return m;
}

// straight segments of an SVG path; a path with curves is not a rule
function segments(d) {
  const toks = d.match(/[MLHVCZmlhvcz]|-?\d*\.?\d+(?:e-?\d+)?/g) || [];
  const segs = [];
  let i = 0, cmd = '', x = 0, y = 0, sx = 0, sy = 0, curved = false;
  const num = () => parseFloat(toks[i++]);
  while (i < toks.length) {
    if (/[A-Za-z]/.test(toks[i])) cmd = toks[i++];
    if (cmd === 'M') { x = num(); y = num(); sx = x; sy = y; cmd = 'L'; }
    else if (cmd === 'L') { const nx = num(), ny = num(); segs.push([x, y, nx, ny]); x = nx; y = ny; }
    else if (cmd === 'H') { const nx = num(); segs.push([x, y, nx, y]); x = nx; }
    else if (cmd === 'V') { const ny = num(); segs.push([x, y, x, ny]); y = ny; }
    else if (cmd === 'C') { num(); num(); num(); num(); x = num(); y = num(); curved = true; }
    else if (cmd === 'Z' || cmd === 'z') { if (x !== sx || y !== sy) segs.push([x, y, sx, sy]); x = sx; y = sy; cmd = ''; }
    else { i++; }                      // relative commands: not produced by pdftocairo
  }
  return { segs, curved };
}

// image sizes from the SVG's <defs>: id -> [width, height] in pixels
function imageSizes(svg) {
  const sizes = new Map();
  const re = /<image\b([^>]*)>/g;
  let m;
  while ((m = re.exec(svg))) {
    const id = /\sid="([^"]+)"/.exec(m[1]), w = /\swidth="([\d.]+)"/.exec(m[1]), hh = /\sheight="([\d.]+)"/.exec(m[1]);
    if (id && w && hh) sizes.set(id[1], [parseFloat(w[1]), parseFloat(hh[1])]);
  }
  return sizes;
}

// rules and image placements in one page's SVG markup (defs excluded by the
// caller or skipped here). An image is defined once in <defs> and placed with
// <use href="#id" transform=...>; a placement with the color-to-alpha filter
// is a transparency mask and is skipped (docs/specs/figures.md).
function parsePage(svg, sizes) {
  sizes = sizes || imageSizes(svg);
  const h = [], v = [], figures = [];
  const stack = [ID];
  let inDefs = 0;
  const tagRe = /<(\/?)(\w+)([^>]*?)(\/?)>/g;
  let t;
  while ((t = tagRe.exec(svg))) {
    const [, close, name, attrs, self] = t;
    if (name === 'defs') { inDefs += close ? -1 : (self ? 0 : 1); continue; }
    if (inDefs) continue;
    if (name === 'g') {
      if (close) stack.pop();
      else if (!self) stack.push(MUL(stack[stack.length - 1], parseTransform(attrs)));
      continue;
    }
    if (name === 'use' && !close) {
      const href = /href="#([^"]+)"/.exec(attrs);
      if (href && sizes.has(href[1]) && !/color-to-alpha/.test(attrs)) {
        const [iw, ih] = sizes.get(href[1]);
        const m = MUL(stack[stack.length - 1], parseTransform(attrs));
        const P = (px, py) => [m[0] * px + m[2] * py + m[4], m[1] * px + m[3] * py + m[5]];
        const c = [P(0, 0), P(iw, 0), P(0, ih), P(iw, ih)];
        const xs = c.map(p => p[0]), ys = c.map(p => p[1]);
        const f = { x0: Math.min(...xs), y0: Math.min(...ys), x1: Math.max(...xs), y1: Math.max(...ys), src: href[1], px: [iw, ih] };
        if (!figures.some(g => Math.abs(g.x0 - f.x0) < 0.5 && Math.abs(g.y0 - f.y0) < 0.5 && Math.abs(g.x1 - f.x1) < 0.5 && Math.abs(g.y1 - f.y1) < 0.5)) figures.push(f);
      }
      continue;
    }
    if (name !== 'path' || close) continue;
    const d = /\sd="([^"]*)"/.exec(attrs);
    if (!d) continue;
    const m = MUL(stack[stack.length - 1], parseTransform(attrs));
    const P = (px, py) => [m[0] * px + m[2] * py + m[4], m[1] * px + m[3] * py + m[5]];
    const { segs, curved } = segments(d[1]);
    if (curved || !segs.length) continue;
    const pts = segs.flatMap(s => [P(s[0], s[1]), P(s[2], s[3])]);
    const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]);
    const bx0 = Math.min(...xs), bx1 = Math.max(...xs), by0 = Math.min(...ys), by1 = Math.max(...ys);
    const stroked = /stroke:\s*rgb|stroke="(?!none)/.test(attrs) && !/stroke:\s*none/.test(attrs);
    if (!stroked) {
      // a filled thin rectangle is a rule
      if (by1 - by0 <= 2 && bx1 - bx0 >= 4) h.push({ y: (by0 + by1) / 2, x0: bx0, x1: bx1 });
      else if (bx1 - bx0 <= 2 && by1 - by0 >= 4) v.push({ x: (bx0 + bx1) / 2, y0: by0, y1: by1 });
      continue;
    }
    for (const s of segs) {
      const [ax, ay] = P(s[0], s[1]), [bx, by] = P(s[2], s[3]);
      if (Math.abs(ay - by) <= 0.8 && Math.abs(ax - bx) >= 4) h.push({ y: (ay + by) / 2, x0: Math.min(ax, bx), x1: Math.max(ax, bx) });
      else if (Math.abs(ax - bx) <= 0.8 && Math.abs(ay - by) >= 4) v.push({ x: (ax + bx) / 2, y0: Math.min(ay, by), y1: Math.max(ay, by) });
    }
  }
  return { h: joinH(h), v: joinV(v), figures };
}

// Printers draw a wall one cell at a time (2024 No 203 p48: 324 vertical
// pieces for 5 walls): join collinear pieces that touch (gap <= 3 pt).
function joinV(v) {
  const out = [];
  for (const l of v.slice().sort((a, b) => a.x - b.x || a.y0 - b.y0)) {
    const last = out.find(o => Math.abs(o.x - l.x) <= 1 && l.y0 <= o.y1 + 3 && l.y1 >= o.y0 - 3);
    if (last) { last.y0 = Math.min(last.y0, l.y0); last.y1 = Math.max(last.y1, l.y1); }
    else out.push({ ...l });
  }
  return out;
}

function joinH(h) {
  const out = [];
  for (const l of h.slice().sort((a, b) => a.y - b.y || a.x0 - b.x0)) {
    const last = out.find(o => Math.abs(o.y - l.y) <= 1 && l.x0 <= o.x1 + 3 && l.x1 >= o.x0 - 3);
    if (last) { last.x0 = Math.min(last.x0, l.x0); last.x1 = Math.max(last.x1, l.x1); }
    else out.push({ ...l });
  }
  return out;
}

function svgOf(pdf, first, last) {
  const r = spawnSync('pdftocairo', ['-svg', '-f', String(first), '-l', String(last), pdf, '-'],
    { maxBuffer: 1024 * 1024 * 1024, encoding: 'utf8' });
  return r.status === 0 && r.stdout ? r.stdout : null;
}

function rules(pdf, page) {
  const svg = svgOf(pdf, page, page);
  return svg ? parsePage(svg) : { h: [], v: [], figures: [] };
}

// all pages, in blocks; a block that fails (no poppler, broken page) has no
// rules - those pages are then extracted exactly as without this fix
function rulesForDoc(pdf, nPages) {
  const pages = new Map();
  let height = null;
  for (let first = 1; first <= nPages; first += BLOCK) {
    const last = Math.min(nPages, first + BLOCK - 1);
    const svg = svgOf(pdf, first, last);
    if (!svg) continue;
    if (height === null) { const m = /<svg[^>]*\sheight="([\d.]+)pt"/.exec(svg); if (m) height = parseFloat(m[1]); }
    const sizes = imageSizes(svg);
    const body = svg.replace(/<defs>[\s\S]*?<\/defs>/g, '');
    // a one-page SVG has no <page> wrapper: the whole body is the page
    const parts = body.includes('<page>') ? body.split('<page>').slice(1) : [body];
    parts.forEach((p, k) => pages.set(first + k, parsePage(p.split('</page>')[0], sizes)));
  }
  return { height, pages };
}

const uniq = (vals, tol) => {
  const out = [];
  for (const v of vals.slice().sort((a, b) => a - b)) if (!out.length || v - out[out.length - 1] > tol) out.push(v);
  return out;
};

// Ruled-table lattices: >= 3 vertical walls (>= 2 columns) whose spans
// overlap, and >= 3 horizontal rules (>= 2 rows) between the outer walls.
// A one-cell frame around a boxed notice is not a lattice.
function lattices(R, pageW = 595, pageH = 842) {
  // The rule some issues print between the two page columns is not a table
  // wall: long, near the page centre, and crossed by no horizontal rule
  // (table walls are crossed by row rules). Grouped with a small table's
  // walls it made the rules between notices into "rows" (2025 No 192 p43).
  const gutter = l => l.y1 - l.y0 >= 0.4 * pageH && Math.abs(l.x - pageW / 2) <= 25
    && !R.h.some(h => h.y > l.y0 + 2 && h.y < l.y1 - 2 && h.x0 < l.x - 2 && h.x1 > l.x + 2);
  // >= 6 pt: the walls of a one-row group are ~10 pt tall (2025 No 93 p29)
  const walls = R.v.filter(l => l.y1 - l.y0 >= 6 && !gutter(l)).sort((a, b) => a.y0 - b.y0);
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

module.exports = { rules, rulesForDoc, lattices, parsePage };

if (require.main === module) {
  const r = rules(process.argv[2], parseInt(process.argv[3], 10));
  console.log('horizontal', r.h.length, 'vertical', r.v.length);
  console.log('h y:', [...new Set(r.h.map(l => l.y.toFixed(1)))].slice(0, 15).join(' '));
  console.log('v x:', [...new Set(r.v.map(l => l.x.toFixed(1)))].join(' '));
  console.log('lattices:', lattices(r).map(L => `${L.xs.length - 1} cols x ${L.ys.length - 1} rows`).join(', '));
}
