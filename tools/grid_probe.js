// Measurement probe (fix 7): ruled tables from the PDF's own vector lines.
// detectVectorGridInRegion returns cell boxes (render pixels at `dpi`) from
// drawn ruling lines; each cell is filled with the positioned text items whose
// centre lies inside it. Usage: node tools/grid_probe.js <pdf> <page1based> [left|right|full]
const { readFileSync } = require('fs');
const pi = require('@firecrawl/pdf-inspector');
const buf = readFileSync(process.argv[2]);
const page = parseInt(process.argv[3], 10);
const part = process.argv[4] || 'full';
const DPI = 150;
const items = pi.extractTextWithPositions(buf).filter(i => i.page === page && i.itemType === 'Text' && i.text.trim());
// page size from items is approximate; A4 = 595 x 842 pt
const W = 595, H = 842;
const region = part === 'left' ? [0, 0, W / 2, H] : part === 'right' ? [W / 2, 0, W, H] : [0, 0, W, H];
const g = pi.detectVectorGridInRegion(buf, page - 1, region, DPI);
if (!g) { console.log('no grid'); process.exit(0); }
const k = 72 / DPI;
const boxes = g.cellBboxes.map(b => [region[0] + b[0] * k, region[1] + b[1] * k, region[0] + b[2] * k, region[1] + b[3] * k]);
// rows from structure tokens
const rows = []; let cur = null, ci = 0;
for (const t of g.structureTokens) {
  if (t === '<tr>') cur = [];
  else if (t === '</tr>') { rows.push(cur); cur = null; }
  else if (t.startsWith('<td')) { cur.push(boxes[ci++]); }
}
// item y in extractTextWithPositions: baseline from the BOTTOM? detect: compare to page height
const ys = items.map(i => i.y);
const fromBottom = true;
const cellText = b => items.filter(i => {
  const cx = i.x + i.width / 2;
  const cy = fromBottom ? H - i.y - i.height / 2 : i.y + i.height / 2;
  return cx >= b[0] && cx <= b[2] && cy >= b[1] && cy <= b[3];
}).sort((a, c) => (c.y - a.y) || (a.x - c.x)).map(i => i.text.trim()).join(' ');
console.log('grid rows', rows.length, 'cols', Math.max(...rows.map(r => r.length)));
for (const r of rows.slice(0, 14)) console.log(r.map(cellText).join(' | '));
