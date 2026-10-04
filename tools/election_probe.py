"""
Election notices in a year - measurement only.

Finds notices whose heading block carries election markers (Elections Act,
IEBC, Political Parties Act, returning officers, petitions ...), in the
pipeline text and in the OCR text of scanned gazettes, then shows how the
Python categoriser and the Java pre-filter file them, their structure, and
clusters by Act + subject. Writes reports/corpus/<year>/election_notices.json.

Usage: python tools/election_probe.py 2022   (needs ocr_check + corpus_report run first)
"""
import glob, json, os, re, sys
from collections import Counter, defaultdict
sys.path.insert(0, 'tools')
import corpus_report as R

Y = sys.argv[1] if len(sys.argv) > 1 else '2022'
ELEC = re.compile(r'ELECTIONS?\s*ACT|ELECTORAL|RETURNING\s*OFFICER|POLLING|BY-?\s*ELECTION|ELECTION\s*PETITION|'
                  r'POLITICAL\s*PART|NOMINATION\s*OF\s*(?:CANDIDATE|PARTY)|ELECTED\s*MEMBERS?|IEBC|'
                  r'GENERAL\s*ELECTION|ELECTION\s*OFFENCES|CAMPAIGN\s*FINANCING|TALLYING|PARTY\s*LIST', re.I)

rows = []
for sj in sorted(glob.glob('reports/ocr/%s/*/summary.json' % Y)):
    s = json.load(open(sj, encoding='utf-8'))['summary']
    slug = s['slug']
    if s['kind'] in ('scanned', 'scan+ocr-layer'):
        pages = [open(p, encoding='utf-8', errors='replace').read() for p in sorted(glob.glob('reports/ocr/%s/%s/p*.txt' % (Y, slug)))]
        text, src = '\n'.join(pages), 'ocr (scan)'
    else:
        text, src = R.read_text('raw/%s/%s.inspector.txt' % (Y, slug)), 'pipeline'
    ne = [l for l in text.split('\n') if l.strip()]
    if len(ne) > 20000 and sum(len(l.strip()) for l in ne) / len(ne) < 3:
        continue
    c = R.G.apply_ascending_lock(R.G.clean(text))
    for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', c):
        if not n.startswith('GAZETTE NOTICE NO.'):
            continue
        head = n[:700]
        if not ELEC.search(head):
            continue
        num = int(re.match(r'GAZETTE NOTICE NO\. (\d+)', n).group(1))
        sq = re.sub(r'\s+', '', n.lower())
        act, subj = R.heading(n)
        rows.append({'slug': slug, 'issue': s['issue'], 'num': num, 'src': src, 'words': len(n.split()),
                     'py': R.canon(R.CEN.categorise(n)), 'java': R.java_prefilter(sq, n) or '(AI triage)',
                     'truth': R.canon(R.KP.truth_of(n)) or '', 'act': act, 'subj': subj,
                     'shapes': R.structure(n, 'x')['shapes'], 'text': n})

print('== %s election-related notices: %d (pipeline %d, hidden in scans %d)' % (
    Y, len(rows), sum(1 for r in rows if r['src'] == 'pipeline'), sum(1 for r in rows if r['src'] != 'pipeline')))
print('by issue:', dict(Counter(r['issue'] for r in rows).most_common(12)))
print('python cat:', dict(Counter(r['py'] for r in rows).most_common()))
print('java cat  :', dict(Counter(r['java'] for r in rows).most_common()))
print('truth     :', dict(Counter(r['truth'] or '-' for r in rows).most_common()))
print('shapes    :', dict(Counter(s for r in rows for s in r['shapes']).most_common()))
print('words: median %d, max %d' % (sorted(r['words'] for r in rows)[len(rows) // 2], max(r['words'] for r in rows)))

# cluster by subject heading with digits / place names collapsed
def key(r):
    s = re.sub(r'#|\b[A-Z][a-z]+\b', '', r['subj']).strip()
    return (r['act'][:45], re.sub(r'\s+', ' ', s)[:70])
cl = defaultdict(list)
for r in rows:
    cl[key(r)].append(r)
print('\n== clusters (Act | subject)')
for k, v in sorted(cl.items(), key=lambda kv: -len(kv[1]))[:30]:
    print('%4d  %s | %s  [py %s | java %s]  e.g. %s' % (
        len(v), k[0] or '-', k[1] or '-', Counter(x['py'] for x in v).most_common(1)[0][0],
        Counter(x['java'] for x in v).most_common(1)[0][0], ', '.join('%s/%d' % (x['slug'], x['num']) for x in v[:4])))

json.dump([{k: v for k, v in r.items() if k != 'text'} | {'text': r['text'][:3000]} for r in rows],
          open('reports/corpus/%s/election_notices.json' % Y, 'w', encoding='utf-8'), indent=1)
