// Regression cases for the ruled-table pass (fix 7b), run by tools/audit.py.
// Synthetic pages: text items (baseline y from the bottom, as pdf-inspector
// gives them) and ruling lines (top-left origin, as pdf_rules.js gives them).
// Prints one "ok|ERROR<TAB>name<TAB>detail" line per case.
const { pageText } = require('./inspect_positions');

const H = 842, FS = 8;
const item = (text, x, top, font = 'TT1') => ({ text, x, y: H - top, width: text.length * 4, height: FS, fontSize: FS, font, page: 1 });
// a 3-column lattice: walls at x 50 / 150 / 300 / 400, rules between rows
const wall = x => ({ x, y0: 100, y1: 160 });
const RULES = {
  v: [wall(50), wall(150), wall(300), wall(400)],
  h: [100, 112, 136, 148, 160].map(y => ({ y, x0: 50, x1: 400 })),
};
const cases = [];
const check = (name, ok, detail) => cases.push([ok ? 'ok' : 'ERROR', name, ok ? '' : detail]);

// header row (bold font), a row with a wrapped owner cell, a row with an empty cell,
// a row whose PDF text item holds two cells
const its = [
  item('Parcel', 55, 109, 'TT4'), item('Owner', 155, 109, 'TT4'), item('Area', 305, 109, 'TT4'),
  item('X/1', 55, 121), item('John Doe and', 155, 121), item('0.10', 305, 121), item('Jane Doe', 155, 132),
  item('X/2', 55, 145), item('0.20', 305, 145),
  item('X/3 John Roe', 55, 157), item('0.30', 305, 157),
];
let out = pageText(its.map(i => ({ ...i })), RULES, H).split('\n');
check('ruled: header marked by its typeface', out[0] === 'Parcel | Owner | Area' && out[1] === '--- | --- | ---', out.slice(0, 2).join(' / '));
check('ruled: wrapped cell joined to its row', out.includes('X/1 | John Doe and Jane Doe | 0.10'), out.join(' / '));
check('ruled: empty cell kept in place', out.includes('X/2 | | 0.20'), out.join(' / '));
// "X/3 John Roe" is 48 pt wide from x 55: it does not reach the wall at 150, so it stays one cell
check('ruled: text inside one cell is not split', out.includes('X/3 John Roe | | 0.30'), out.join(' / '));
// an item running across the wall at 150 holds two cells
const its2 = its.map(i => ({ ...i }));
its2[9] = item('X/3                       John Roe', 55, 157);
out = pageText(its2, RULES, H).split('\n');
check('ruled: an item across a wall is split at the space', out.includes('X/3 | John Roe | 0.30'), out.join(' / '));
// words in = words out
const words = s => s.split(/\W+/).filter(Boolean).sort().join(' ');
check('ruled: no word lost or added', words(pageText(its.map(i => ({ ...i })), RULES, H)) === words(its.map(i => i.text).join(' ')), '');
// a raised ordinal ("3" + small "rd" a little higher) stays one word
const its3 = its.map(i => ({ ...i }));
its3[5] = item('3', 305, 121);
its3.push({ ...item('rd', 309, 119.5), fontSize: 5 });
out = pageText(its3, RULES, H).split('\n');
check('ruled: a raised ordinal stays one word', out.includes('X/1 | John Doe and Jane Doe | 3rd'), out.join(' / '));
// an item split at a wall keeps its word break: "Year " + "of" on the same baseline
const its4 = its.map(i => ({ ...i }));
its4[1] = { ...item('Owner Year ', 120, 109, 'TT4'), width: 44 };
its4.push(item('of', 160, 109, 'TT4'));
out = pageText(its4, RULES, H).split('\n');
check('ruled: a split piece keeps its space ("Year of")', out[0] === 'Parcel Owner | Year of | Area', out[0]);
// a merged cell: no wall at 150 in the last row -> one cell spanning two columns
const merged = { v: [wall(50), { x: 150, y0: 100, y1: 148 }, wall(300), wall(400)], h: RULES.h };
out = pageText(its.map(i => ({ ...i })), merged, H).split('\n');
check('ruled: a merged cell is one cell', out.includes('X/3 John Roe | 0.30'), out.join(' / '));
// a cell merged downwards: the rule between rows 2 and 3 crosses only columns
// 2 and 3, so "X/1" (column 1) spans both; the sub-rows must stay separate rows
const rowspan = { v: RULES.v, h: [100, 112, 136, 160].map(y => ({ y, x0: 50, x1: 400 })).concat([{ y: 148, x0: 150, x1: 400 }]) };
const its5 = [
  item('Parcel', 55, 109, 'TT4'), item('Owner', 155, 109, 'TT4'), item('Area', 305, 109, 'TT4'),
  item('X/1', 55, 121), item('John Doe', 155, 121), item('0.10', 305, 121),
  item('X/2', 55, 145), item('Jane Doe', 155, 145), item('0.20', 305, 145),
  item('Jim Doe', 155, 157), item('0.30', 305, 157),
];
out = pageText(its5, rowspan, H).split('\n');
check('ruled: a cell merged downwards keeps the sub-rows apart',
  out.includes('X/2 | Jane Doe | 0.20') && out.includes('| Jim Doe | 0.30'), out.join(' / '));
// an underline under a heading inside a cell is not a row rule
const underl = { v: RULES.v, h: RULES.h.concat([{ y: 123, x0: 160, x1: 200 }]) };
out = pageText(its.map(i => ({ ...i })), underl, H).split('\n');
check('ruled: an underline inside a cell does not split the row', out.includes('X/1 | John Doe and Jane Doe | 0.10'), out.join(' / '));
// no rules: exactly the text without the pass
check('ruled: a page without rules is unchanged',
  pageText(its.map(i => ({ ...i })), { v: [], h: [] }, H) === pageText(its.map(i => ({ ...i }))), '');
// a frame (2 walls) is not a table
const frame = { v: [wall(50), wall(400)], h: RULES.h };
check('ruled: a one-column frame is not a table',
  pageText(its.map(i => ({ ...i })), frame, H) === pageText(its.map(i => ({ ...i }))), '');
// a venue label merged across the table above the bold header (2025 No 164
// notice 10358): the header after the label is still marked
const inner = x => ({ x, y0: 112, y1: 148 });
const labelled = { v: [wall(50), inner(150), inner(300), wall(400)], h: [100, 112, 124, 136, 148].map(y => ({ y, x0: 50, x1: 400 })) };
const its6 = [
  item('Venue on Monday', 55, 109),
  item('Parcel', 55, 121, 'TT4'), item('Owner', 155, 121, 'TT4'), item('Area', 305, 121, 'TT4'),
  item('X/1', 55, 133), item('John Doe', 155, 133), item('0.10', 305, 133),
  item('X/2', 55, 145), item('Jane Doe', 155, 145), item('0.20', 305, 145),
];
out = pageText(its6, labelled, H).split('\n');
check('ruled: a label row above the header keeps the header marked',
  out[0] === 'Venue on Monday |' && out[1] === 'Parcel | Owner | Area' && out[2] === '--- | --- | ---', out.join(' / '));

for (const c of cases) console.log(c.join('\t'));
