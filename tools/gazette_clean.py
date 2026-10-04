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
notified pursuant provisions section reference period appointment following persons
amend printed read land title parcel situate proprietor lost replacement objection'''.split())
# (last line: present in the Java port but missing here until fix 5 - found
# when scan text glued "land"+"contaiming" and "read"+"Kiambu" in Python only)

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

# A date line ending a sentence of content ("... who died on" / "Dated the")
# rather than sitting in a running header (lesson 22).
P_CONTENT_BEFORE_DATE = re.compile(r'(?:\bon|\bthe|\bof|\bdated|\bdied|,)\s*$', re.I)

def one_char_per_line(text):
    """pdf-inspector output with ~1 character per line (some scans, lesson 20):
    unreadable, and Stage 2 would never flush its buffer."""
    lines = [l for l in text.split('\n') if l.strip()]
    return len(lines) > 20000 and sum(len(l.strip()) for l in lines) / len(lines) < 3

def clean(text):
    if one_char_per_line(text):
        return ''                       # no usable text layer -> scan lane

    # ---- Stage 0: encoding + canonicalize fragmented markers --------------
    t = text.replace('\x00', '')        # NUL after a header hid it (lesson 21)
    for a, b in [('ÔÇÖ', "'"), ('ÔÇô', '-'), ('ÔÇö', '—'),
                 ('ÔÇ£', '"'), ('ÔÇØ', '"'), ('ÔÇ¥', '"')]:
        t = t.replace(a, b)
    # Real Unicode punctuation (UTF-8 input) as well as the mojibake above:
    # the templates match ASCII quotes and apostrophes (lesson 25).
    for a, b in [('’', "'"), ('‘', "'"), ('“', '"'), ('”', '"'),
                 ('–', '-'), ('—', '-')]:
        t = t.replace(a, b)

    # Note: 'AZET T? E' tolerates the source typo "GAZETE" (one T) seen in
    # Vol. CXXVII No. 266 notice 19127.
    # Case-sensitive on purpose. Real headers are set in small caps and come
    # out of the extractor as all-uppercase fragments ("G AZETTE N OTICE N O.").
    # Cross-references inside corrigenda are ordinary mixed case ("IN Gazette
    # Notice No. 5520 of 2026, amend ..."), so requiring uppercase keeps them
    # out of the candidate set entirely.
    # The number is followed by the title either on the next line or on the
    # same line ("NO. 7653 THE PUBLIC HOLIDAYS ACT", lesson 21) - but never by
    # "OF <year>": that is an uppercase cross-reference ("NO. 6865 OF 2017",
    # "NO. 2690 / OF 2016", lesson 29).
    t = re.sub(r'G\s*A\s*Z\s*E\s*T\s*T?\s*E\s*N\s*O\s*T\s*I\s*C\s*E\s*N\s*O\s*\.\s*([\d\s]*\d)'
               r'(?!\s*OF\s+\d{4})(?=\s*\n\s*[A-Z]|[ \t]+[A-Z]{2})',
               lambda m: '\n@@HDR@@' + re.sub(r'\s', '', m.group(1)) + '\n', t)
    t = re.sub(r'C\s*A\s*U\s*S\s*E\s*N\s*O\s*\.\s*', '\n@@CAUSE@@ ', t, flags=re.I)
    t = re.sub(r'T\s*AKE\s+N\s*OTICE', 'TAKE NOTICE', t, flags=re.I)
    t = re.sub(r'P\s*ROBATE\s+AND\s+A\s*DMINISTRATION', 'PROBATE AND ADMINISTRATION', t, flags=re.I)
    t = re.sub(r'\bPR\s*\n\s*INCIPAL', 'PRINCIPAL', t)
    t = re.sub(r'\bPR\s*\n\s*OBATE', 'PROBATE', t)

    # ---- Stage 1: context-aware header/footer stripping -------------------
    # A bare number is only a page number if a running header appeared just
    # before it. Otherwise it is content (most importantly, a year such as
    # "2025" that got split onto its own line by the extractor).
    lines = t.split('\n')
    near_gaz = set()                    # line indices within 3 lines of THE KENYA GAZETTE
    for i, ln in enumerate(lines):
        if P_GAZ.match(ln.strip()):
            near_gaz.update(range(i - 3, i + 4))
    kept, since_header, prev = [], 99, ''
    for i, ln in enumerate(lines):
        s = ln.strip()
        if not s:
            kept.append(ln); since_header += 1; continue
        if P_GAZ.match(s):
            # the running head marks a page break; the marker lets Stage 2
            # recognise a table header repeated at the top of the next page
            since_header = 0; kept.append('@@PAGE@@'); continue
        # A date line is a running-header dateline only next to THE KENYA
        # GAZETTE and not when it completes a sentence ("who died on" / "Dated
        # the"). Otherwise it is content - a date of death or signature date
        # (lesson 22: ~480 a year were deleted, and the reset then let the
        # following split year be deleted as a page number).
        if P_DATE.match(s) and i in near_gaz and not P_CONTENT_BEFORE_DATE.search(prev):
            since_header = 0; continue
        prev = s
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
    page_top, page_first = False, set()   # indices of the first table row on a page
    def flush():
        nonlocal buf
        if buf.strip():
            out.append(re.sub(r'\s{2,}', ' ', buf).strip())
        buf = ''

    for ln in kept:
        s = ln.strip()
        if not s:
            continue
        if s == '@@PAGE@@':
            page_top = True; continue
        if s.startswith('@@HDR@@'):
            page_top = False
            flush(); out.append('GAZETTE NOTICE NO. ' + s.replace('@@HDR@@', '').strip()); continue
        if s.startswith('@@CAUSE@@'):
            flush(); buf = 'CAUSE NO. ' + s.replace('@@CAUSE@@', '').strip(); continue
        # a table row (fix 7: cells marked " | " by the extractor) stands on
        # its own line - joined into a paragraph its row boundary is lost.
        # Before the numbered-item rule: rows often start "1. | 233426 | ..."
        # (a row whose last cell is empty ends " |" once trailing spaces go, fix 7b)
        if ' | ' in s or s.endswith(' |'):
            flush()
            if page_top:
                page_first.add(len(out)); page_top = False
            out.append(s); continue
        # top-of-page text other than a short caption ("SCHEDULE-(Contd.)")
        # means the page does not open with a continued table
        if page_top and len(s.split()) > 6:
            page_top = False
        if re.match(r'^(\d+\.|\([a-z]\)|\([ivx]+\))\s', s):
            flush(); buf = s; continue
        if not buf:
            buf = s
        else:
            prev, nxt = buf[-1], s[0]
            # last word only: re-splitting the whole buffer per line was
            # quadratic and hung for hours on long unpunctuated text (lesson 20)
            last_tok = buf.rsplit(None, 1)[-1] if buf.strip() else ''
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

    res = re.sub(r'\s{2,}', ' ', '\n'.join(drop_repeated_table_headers(out, page_first)))
    return re.sub(r'\n{3,}', '\n\n', res)


# the separator line inspect_positions.js writes under a table header row
DASH_ROW = re.compile(r'^-{3}(?: \| -{3})+$')


def drop_repeated_table_headers(lines, page_first):
    """A table continued on the next page repeats its header row ("Parcel No. |
    Registered Owner (s) | Area") as the FIRST row of the new page. Such a row
    - no digits, identical to an earlier cell row of the same notice - is
    dropped, so the continued table reads as one (fix 7). Only page-first rows:
    data rows without digits repeat legitimately ("Ward Administrator |
    Ex-Officio Member" once per ward - 899 rows were dropped when any repeat
    counted)."""
    seen, out, dropped = set(), [], False
    for i, l in enumerate(lines):
        if dropped and DASH_ROW.match(l):       # its "--- | ---" separator (fix 8)
            dropped = False
            continue
        dropped = False
        if l.startswith('GAZETTE NOTICE NO.'):
            seen = set()
        elif ' | ' in l and not re.search(r'\d', l) and not DASH_ROW.match(l):
            key = re.sub(r'\s+', ' ', l).strip().lower()
            if key in seen and i in page_first:
                dropped = True
                continue
            seen.add(key)
        out.append(l)
    return out


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
