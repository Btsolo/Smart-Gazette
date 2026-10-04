// Measurement (fix 7): how many table pages carry a vector grid (ruled table)?
// stdin: lines "pdf\tpage" ; stdout: "pdf\tpage\trows\tcols\tms"
const { readFileSync } = require('fs');
const pi = require('@firecrawl/pdf-inspector');
const lines = readFileSync(0, 'utf8').split('\n').filter(Boolean);
const cache = new Map();
for (const l of lines) {
  const [pdf, page] = l.split('\t');
  if (!cache.has(pdf)) { cache.clear(); cache.set(pdf, readFileSync(pdf)); }
  const t0 = Date.now();
  let rows = 0, cols = 0;
  try {
    const g = pi.detectVectorGridInRegion(cache.get(pdf), parseInt(page, 10) - 1, [0, 0, 595, 842], 150);
    if (g) {
      let cur = 0;
      for (const t of g.structureTokens) {
        if (t === '<tr>') { rows++; cur = 0; } else if (t.startsWith('<td')) { cur++; cols = Math.max(cols, cur); }
      }
    }
  } catch (e) { rows = -1; }
  process.stdout.write([pdf, page, rows, cols, Date.now() - t0].join('\t') + '\n');
}
