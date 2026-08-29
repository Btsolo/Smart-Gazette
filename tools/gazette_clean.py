"""
Gazette text cleaner.

Input : raw output from pdf-inspector's extractText (via inspect.js)
Output: cleaned, joined, header-stripped text ready for notice segmentation

Pipeline:
  Stage 0 - fix mojibake, canonicalize fragmented small-caps markers
  Stage 1 - strip running headers/footers (context-aware, so years survive)
  Stage 2 - join fragmented lines using a word-completeness test
"""
import re, sys

# Words used to decide glue-vs-space when joining a fragmented line.
# If the previous line's trailing token is a complete word we add a space;
# if it is a cut-off fragment (e.g. "Nairob") we glue with no space.
COMMON = set('''a an and are as at be been by for from had has have he her his if in into is it
its of on or that the their there they this to was were which who will with within would
court estate grant letters administration intestate testate probate notice cause deceased
died late who widow son sons daughter children advocates through messrs
gazette kenya publication days thirty date same issue unless shown contrary appearance
respect entered proceed application applications having made this order box po
registrar district deputy senior principal magistrate chief high resident county
act cap constitution government public service board members chairperson secretary
notified pursuant provisions section reference period appointment following persons'''.split())

def is_word(tok):
    t = tok.strip(".,;:()'\"-").lower()
    return t in COMMON or len(t) > 6      # long tokens are usually complete

# --- Stage 1 patterns ------------------------------------------------------
P_GAZ   = re.compile(r'^\s*THE KENYA GAZETTE\s*$', re.I)
P_DATE  = re.compile(r'^\s*\d{1,2}(st|nd|rd|th)\s+\w+,?\s+\d{4}\s*$', re.I)
P_NUM   = re.compile(r'^\s*\[?(\d{1,5})\]?\s*$')
P_PRINT = re.compile(r'PRINTED AND|GOVERNMENT PRINTER', re.I)

def looks_like_year(n):
    return 1900 <= n <= 2100

def clean(text):
    # ---- Stage 0: encoding + canonicalize fragmented markers --------------
    t = text
    for a, b in [('ÔÇÖ', "'"), ('ÔÇô', '-'), ('ÔÇö', '—'),
                 ('ÔÇ£', '"'), ('ÔÇØ', '"'), ('ÔÇ¥', '"')]:
        t = t.replace(a, b)

    # Note: 'AZET T? E' tolerates the source typo "GAZETE" (one T) seen in
    # Vol. CXXVII No. 266 notice 19127.
    t = re.sub(r'G\s*A\s*Z\s*E\s*T\s*T?\s*E\s*N\s*O\s*T\s*I\s*C\s*E\s*N\s*O\s*\.\s*([\d\s]*\d)(?=\s*\n\s*[A-Z])',
               lambda m: '\n@@HDR@@' + re.sub(r'\s', '', m.group(1)) + '\n', t, flags=re.I)
    t = re.sub(r'C\s*A\s*U\s*S\s*E\s*N\s*O\s*\.\s*', '\n@@CAUSE@@ ', t, flags=re.I)
    t = re.sub(r'T\s*AKE\s+N\s*OTICE', 'TAKE NOTICE', t, flags=re.I)
    t = re.sub(r'P\s*ROBATE\s+AND\s+A\s*DMINISTRATION', 'PROBATE AND ADMINISTRATION', t, flags=re.I)
    t = re.sub(r'\bPR\s*\n\s*INCIPAL', 'PRINCIPAL', t)
    t = re.sub(r'\bPR\s*\n\s*OBATE', 'PROBATE', t)

    # ---- Stage 1: context-aware header/footer stripping -------------------
    # A bare number is only a page number if a running header appeared just
    # before it. Otherwise it is content (most importantly, a year such as
    # "2025" that got split onto its own line by the extractor).
    kept, since_header = [], 99
    for ln in t.split('\n'):
        s = ln.strip()
        if not s:
            kept.append(ln); since_header += 1; continue
        if P_GAZ.match(s) or P_DATE.match(s):
            since_header = 0; continue
        if P_PRINT.search(s):
            continue
        m = P_NUM.match(s)
        if m:
            n = int(m.group(1))
            if since_header <= 6 and not looks_like_year(n):
                continue                      # page number right after header
            if since_header <= 6 and looks_like_year(n) and n > 2030:
                continue                      # implausible year => page number
        since_header += 1
        kept.append(ln)

    # ---- Stage 2: join fragments ------------------------------------------
    out, buf = [], ''
    def flush():
        nonlocal buf
        if buf.strip():
            out.append(re.sub(r'\s{2,}', ' ', buf).strip())
        buf = ''

    for ln in kept:
        s = ln.strip()
        if not s:
            continue
        if s.startswith('@@HDR@@'):
            flush(); out.append('GAZETTE NOTICE NO. ' + s.replace('@@HDR@@', '').strip()); continue
        if s.startswith('@@CAUSE@@'):
            flush(); buf = 'CAUSE NO. ' + s.replace('@@CAUSE@@', '').strip(); continue
        if re.match(r'^(\d+\.|\([a-z]\)|\([ivx]+\))\s', s):
            flush(); buf = s; continue
        if not buf:
            buf = s
        else:
            prev, nxt = buf[-1], s[0]
            last_tok = buf.split()[-1] if buf.split() else ''
            if nxt in '.,;:)]':
                buf += s
            elif prev in '([':
                buf += s
            elif prev.isalpha() and nxt.isalpha() and not is_word(last_tok):
                buf += s                       # previous token is a fragment
            else:
                buf += ' ' + s
        if re.search(r'[.;:]$', s) and len(s) > 2:
            flush()
    flush()

    res = re.sub(r'\s{2,}', ' ', '\n'.join(out))
    return re.sub(r'\n{3,}', '\n\n', res)


def apply_ascending_lock(res):
    """
    Gazette notice numbers ascend monotonically through an issue, but the text
    also contains cross-references to earlier notices ("IN Gazette Notice No.
    14410 of 2023, amend..."). Those look identical to real headers.

    A single-pass "must exceed the previous" rule fails when a cross-reference
    appears BEFORE the first real notice - as in issues that open with a
    CORRIGENDA section - because the high reference number then rejects every
    genuine header that follows.

    So we keep the largest strictly-ascending subset of candidates (a longest
    increasing subsequence in document order). Real headers form one long
    ascending run; cross-references are isolated outliers that fall outside it.
    Each candidate may also be considered in a repaired form, for headers whose
    trailing digits were split onto the following line ("NO. 1900" + "3 HIGH").
    """
    lines = res.split('\n')

    # ---- collect candidates, each with one or two possible values ----------
    cands = []          # dict: line, variants [(value, absorb_next, tail_text)]
    for i, ln in enumerate(lines):
        m = re.match(r'^GAZETTE NOTICE NO\. (\d+)\s*(.*)$', ln)
        if not m:
            continue
        num_s, rest = m.group(1), m.group(2)
        variants = [(int(num_s), False, rest)]
        nxt = rest if rest else (lines[i + 1] if i + 1 < len(lines) else '')
        dm = re.match(r'^(\d+)\s+(.*)$', nxt.strip())
        if dm:
            variants.append((int(num_s + dm.group(1)), not rest, dm.group(2)))
        cands.append({'line': i, 'variants': variants})

    if not cands:
        return res

    # ---- longest strictly-increasing subsequence over (candidate, variant) --
    flat = [(ci, vi, v[0]) for ci, c in enumerate(cands)
            for vi, v in enumerate(c['variants'])]
    n = len(flat)
    best = [1] * n
    prev = [-1] * n
    for k in range(n):
        ck, _, vk = flat[k]
        for j in range(k):
            cj, _, vj = flat[j]
            if cj < ck and vj < vk and best[j] + 1 > best[k]:
                best[k] = best[j] + 1
                prev[k] = j
    end_k = max(range(n), key=lambda k: best[k])

    keep = {}
    k = end_k
    while k != -1:
        ci, vi, _ = flat[k]
        keep[cands[ci]['line']] = cands[ci]['variants'][vi]
        k = prev[k]

    # ---- rewrite ------------------------------------------------------------
    out, skip = [], set()
    for i, ln in enumerate(lines):
        if i in skip:
            continue
        m = re.match(r'^GAZETTE NOTICE NO\. (\d+)\s*(.*)$', ln)
        if not m:
            out.append(ln)
            continue
        if i in keep:
            value, absorb, tail = keep[i]
            out.append(('GAZETTE NOTICE NO. %d' % value).rstrip())
            if absorb:
                skip.add(i + 1)
                if tail:
                    out.append(tail)
            elif tail:
                out.append(tail)
        else:
            out.append(('Gazette Notice No. %s %s' % (m.group(1), m.group(2))).rstrip())
    return '\n'.join(out)

if __name__ == '__main__':
    data = open(sys.argv[1], 'rb').read()
    try:
        raw = data.decode('utf-16')
    except UnicodeError:
        raw = data.decode('utf-8', errors='replace')

    res = apply_ascending_lock(clean(raw))
    open(sys.argv[2], 'w', encoding='utf-8').write(res)

    h = re.findall(r'GAZETTE NOTICE NO\. (\d+)', res)
    if not h:
        print('WARNING: no notice headers found — check the input file'); sys.exit(0)
    ints = [int(x) for x in h]
    bad = [(ints[i], ints[i+1]) for i in range(len(ints)-1) if ints[i] >= ints[i+1]]
    print('headers:', len(h), '|', h[0], '->', h[-1])
    print('non-ascending (should be 0 after lock):', len(bad), bad[:5])
    print('CAUSE NO:', len(re.findall(r'CAUSE NO\.', res)))
    print('hdr leaks:', len(re.findall(r'THE KENYA GAZETTE', res)))
    print('dropped-year check (OF By):', len(re.findall(r'OF By', res)))
