"""
Kenya Law Act PDFs -> law reference JSON (docs/specs/law-library.md).

Kenya Law's "Download PDF" (Laws.Africa, Apache FOP) has a fixed shape:
  - cover + licence page: "Legislation as at 31 December 2022", "FRBR URI: /akn/ke/act/2012/3/eng@2022-12-31"
  - Contents: "Part I - PRELIMINARY ....... 1", "1. Short title ....... 1", "SCHEDULE ....... 39"
  - body pages, each with a running header ("Land Registration Act (Cap. 300)", "Kenya")
    and a page number; "Part II - ..." headings; each section starts on its own line
    "33. Lost or destroyed certificates and registers".

The Contents list gives every section number and title in order, so a section
heading is found by looking for the NEXT expected one - never by guessing from
any line that starts with a number ("(1)", "13A." and cross references are in
the text). Output: the same structure tools/build_law_reference.py writes from
the HTML pages (parts, provisions keyed by number with text and clauses,
schedules), plus title / citation / version date / source read from the PDF.

  parse(pdf_path) -> dict
"""
import datetime, difflib, re, subprocess

LIGATURES = {'ﬀ': 'ff', 'ﬁ': 'fi', 'ﬂ': 'fl', 'ﬃ': 'ffi', 'ﬄ': 'ffl', '­': ''}
MONTHS = {m: i for i, m in enumerate(['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
                                      'september', 'october', 'november', 'december'], 1)}
DASH = r'\s*[—–-]+\s*'
RE_PART = re.compile(r'^(?:PART|Part)\s+([IVXLC]+[A-Z]?)' + DASH + r'(.+)$')
RE_ENTRY = re.compile(r'(.+?)\s*\.{4,}\s*(\d+)\s*')            # a Contents line: text ..... page
RE_SECTION_ENTRY = re.compile(r'^(\d+[A-Z]{0,3})\.\s*(.*)$')
RE_CLAUSE = re.compile(r'^\((\d+[A-Z]{0,2})\)\s*(.*)$')
RE_LETTER = re.compile(r'^\(([a-z]{1,4})\)\s')


def text_of(pdf_path):
    out = subprocess.run(['pdftotext', '-enc', 'UTF-8', pdf_path, '-'], capture_output=True).stdout.decode('utf-8', 'replace')
    for a, b in LIGATURES.items():
        out = out.replace(a, b)
    return out


def tidy(s):
    return re.sub(r'\s+', ' ', s or '').strip()


def norm(s):
    """for comparing headings: lower case, one kind of dash, single spaces"""
    return tidy(re.sub(r'[—–]', '-', s)).lower()


def iso_date(text):
    m = re.match(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', text)
    if not m or m.group(2).lower() not in MONTHS:
        return None
    return datetime.date(int(m.group(3)), MONTHS[m.group(2).lower()], int(m.group(1))).isoformat()


def contents_entries(text):
    """[(kind, num, title)] from the Contents pages, in order; kind = part | section | schedule"""
    flat = tidy(text)
    flat = flat[flat.find('Contents') + len('Contents'):] if 'Contents' in flat else flat
    out = []
    for m in RE_ENTRY.finditer(flat):
        e = tidy(m.group(1))
        p = RE_PART.match(e)
        s = RE_SECTION_ENTRY.match(e)
        if p:
            out.append(('part', p.group(1), tidy(p.group(2))))
        elif s:
            out.append(('section', s.group(1), tidy(s.group(2))))
        elif re.search(r'schedule|appendix|annex|form\b', e, re.I):
            out.append(('schedule', None, e))
        else:
            out.append(('other', None, e))
    return out


def body_lines(pages, header):
    """body lines without the running header, the 'Kenya' line and page numbers"""
    out = []
    for pg in pages:
        for line in pg.split('\n'):
            t = line.strip()
            if not t or t == header or t == 'Kenya' or re.fullmatch(r'\d{1,4}', t):
                continue
            out.append(t)
    return out


def join_wrapped(lines):
    """pdftotext breaks long lines; a line continues the previous one unless the
    previous one ended a sentence / list item or this one starts a numbered unit"""
    out = []
    for t in lines:
        starts_unit = bool(RE_CLAUSE.match(t) or RE_LETTER.match(t) or re.match(r'^\([ivxlc]+\)\s', t)
                           or t.startswith('[') or t.startswith('"'))
        if out and not starts_unit and not re.search(r'[.;:—\]]$|--$', out[-1]):
            out[-1] = out[-1] + ' ' + t
        else:
            out.append(t)
    return out


def render(lines):
    """the section text: numbered subsections at the margin, lettered paragraphs
    indented; a run "(a) ...; (b) ..." on one line is split at its markers"""
    text = []
    for t in join_wrapped(lines):
        # split runs of lettered paragraphs that pdftotext put on one line
        parts = re.split(r'(?<=[;:—])\s+(?=\([a-z]{1,4}\)\s)', t)
        for k, p in enumerate(parts):
            p = tidy(p)
            if not p:
                continue
            text.append(('  ' if RE_LETTER.match(p) or re.match(r'^\([ivxlc]+\)\s', p) else '') + p)
    return '\n'.join(text)


def parse(pdf_path):
    raw = text_of(pdf_path)
    pages = raw.split('\f')
    head = '\n'.join(pages[:3])
    # the running header is the most common first line of the body pages
    firsts = [p.strip().split('\n')[0].strip() for p in pages if p.strip()]
    header = max(set(firsts), key=firsts.count)
    m = re.match(r'^(.*?)\s*\(([^()]*)\)\s*$', header)
    title, citation = (tidy(m.group(1)), tidy(m.group(2))) if m else (header, None)
    version = re.search(r'Legislation as at\s+(\d{1,2}\s+\w+\s+\d{4})', head)
    frbr = re.search(r'FRBR URI:\s*(\S+)', head)
    # the body starts on the page with the Act's own heading / assent line
    start = next((i for i, p in enumerate(pages)
                  if i > 0 and re.search(r'Assented to on|Commenced on|AN ACT of Parliament|^\s*1\.\s', p, re.M)
                  and not re.search(r'\.{6,}', p)), 2)
    entries = contents_entries('\n'.join(pages[1:start]))
    expected = [e for e in entries if e[0] == 'section']
    sched_titles = [e[2] for e in entries if e[0] == 'schedule']
    lines = body_lines(pages[start:], header)

    parts, provisions, schedules, preamble = [], {}, [], []
    cur_part, cur = None, None
    i = 0                                   # next expected section
    in_schedules = False

    def heading_of(line):
        """the expected section (index) this line opens, or None; looks ahead 3
        entries so one missed heading does not stop the rest"""
        for j in range(i, min(i + 4, len(expected))):
            num, ttl = expected[j][1], expected[j][2]
            if not line.startswith(num + '.'):
                continue
            rest = line[len(num) + 1:].strip()
            if not ttl or norm(rest).startswith(norm(ttl)[:40]) or \
                    difflib.SequenceMatcher(None, norm(rest)[:60], norm(ttl)[:60]).ratio() > 0.8:
                return j
        return None

    def close():
        if cur is None:
            return
        body = cur.pop('lines')
        cur['text'] = render(body)
        cur['clauses'] = [{'ref': '(' + c.group(1) + ')', 'text': tidy(c.group(2))}
                          for c in (RE_CLAUSE.match(l) for l in join_wrapped(body)) if c]
        provisions[cur['num']] = cur

    for line in lines:
        if not in_schedules:
            j = heading_of(line)
            if j is not None:
                close()
                num, ttl = expected[j][1], expected[j][2]
                rest = line[len(num) + 1:].strip()
                cur = {'num': num, 'title': ttl, 'chapter': None, 'chapter_title': None,
                       'part': cur_part['num'] if cur_part else None,
                       'part_title': cur_part['title'] if cur_part else None, 'lines': []}
                # a deleted / repealed section keeps its note as its text
                if norm(rest) != norm(ttl) and len(rest) > len(ttl) + 3:
                    cur['lines'].append(rest[len(ttl):].strip() if norm(rest).startswith(norm(ttl)) else rest)
                if cur_part:
                    cur_part['provisions'].append(num)
                i = j + 1
                continue
            p = RE_PART.match(line)
            if p:
                close()
                cur = None
                cur_part = {'id': 'part_' + p.group(1), 'num': p.group(1), 'title': tidy(p.group(2)), 'provisions': []}
                parts.append(cur_part)
                continue
            if i >= len(expected) and any(norm(line).startswith(norm(s)[:20]) for s in sched_titles):
                close()
                cur = None
                in_schedules = True
        if in_schedules:
            if any(norm(line).startswith(norm(s)[:20]) for s in sched_titles) and (not schedules or schedules[-1]['lines']):
                schedules.append({'id': 'att_%d' % (len(schedules) + 1), 'title': line, 'subtitle': None, 'lines': []})
            else:
                schedules[-1]['lines'].append(line)
            continue
        if cur is None:
            preamble.append(line)
        else:
            cur['lines'].append(line)
    close()
    for s in schedules:
        body = s.pop('lines')
        if body and body[0].isupper() and len(body[0]) < 120:
            s['subtitle'], body = body[0], body[1:]
        s['text'] = render(body)
    return {
        'title': title,
        'citation': citation,
        'source': 'Kenya Law (National Council for Law Reporting) and Laws.Africa',
        'source_url': ('https://new.kenyalaw.org' + frbr.group(1)) if frbr else None,
        'version_date': iso_date(version.group(1)) if version else None,
        'retrieved': datetime.date.today().isoformat(),
        'note': ('Official text as published by Kenya Law (PDF), converted to JSON by '
                 'tools/build_law_reference.py. There is no copyright on the legislative content '
                 '(Kenya Law; Copyright Act, Cap. 130, s. 2); the PDF copy is CC BY-NC-SA 4.0 - '
                 'credit Kenya Law and Laws.Africa. Check source_url for amendments after version_date.'),
        'provision_label': 'section',
        'preamble': render(preamble),
        'parts': parts,
        'provisions': provisions,
        'schedules': schedules,
        'check': {'sections_in_contents': len(expected), 'sections_found': len(provisions),
                  'missing': [e[1] for e in expected if e[1] not in provisions]},
    }


# ------------------------------------------------------------- older layout
# Government Printer editions (a Kenya Gazette supplement, scanned revised
# editions): "ARRANGEMENT OF SECTIONS" lists "1―Short title." (or several
# "1. Short title. 2. Interpretation." on one line); in the body the number
# leads straight into the text ("1. This Act may be cited as ...") and the
# title is a margin note ("Short title. Interpretation.").

RE_ARR_ENTRY = re.compile(r'(?:(?<=\s)|^)(\d+[A-Z]{0,2})\s*[\u2015\u2014\u2013.]\s*([A-Z][^\u2015]*?\.)(?=\s+\d+[A-Z]{0,2}\s*[\u2015\u2014\u2013.]\s*[A-Z]|\s*$)')
MONTH_WORDS = r'(\d{1,2})(?:st|nd|rd|th)?\s+([A-Z][a-z]+),?\s+(\d{4})'


def arrangement_entries(lines):
    """[(num, title)] from the ARRANGEMENT OF SECTIONS lines (they may run on one line)"""
    out, seen = [], set()
    for line in lines:
        for m in RE_ARR_ENTRY.finditer(line):
            num, title = m.group(1), tidy(m.group(2)).rstrip('.')
            if num not in seen and (not out or _after(num, out[-1][0])):
                out.append((num, title))
                seen.add(num)
    return out


def _key(num):
    m = re.match(r'(\d+)([A-Z]*)', num)
    return int(m.group(1)), m.group(2)


def _after(num, prev):
    return _key(num) > _key(prev)


def running_lines(pages):
    """short lines printed on many pages (running headers / footers)"""
    from collections import Counter
    c = Counter()
    for pg in pages:
        c.update({l.strip() for l in pg.split('\n') if 0 < len(l.strip()) < 70})
    return {l for l, n in c.items() if n >= max(3, len(pages) // 4)}


def parse_arrangement(pdf_path, raw=None):
    raw = raw if raw is not None else text_of(pdf_path)
    pages = raw.split('\f')
    run = running_lines(pages)
    lines = []
    for pg in pages:
        for l in pg.split('\n'):
            t = l.strip()
            if not t or t in run or re.fullmatch(r'\d{1,4}', t) or re.search(r'\[Rev\.\s*\d{4}', t):
                continue
            lines.append(t)
    flat = '\n'.join(lines)
    a0 = flat.find('ARRANGEMENT OF SECTIONS')
    # the arrangement ends where the Act itself begins
    body_at = re.search(r'\n(?:AN ACT|An Act)\b', flat[a0:])
    arr_text = flat[a0:a0 + body_at.start()] if body_at else flat[a0:]
    expected = arrangement_entries(arr_text.split('\n'))
    body = flat[a0 + body_at.start():].split('\n') if body_at else []
    title_line = re.search(r'THE\s+([A-Z][A-Z ,()\-]+ACT(?:,\s*\d{4})?)', flat)
    title = tidy(title_line.group(1)).title().replace(' Of ', ' of ').replace(' And ', ' and ') if title_line else None
    cit = re.search(r'No\.\s*\d+\s+of\s+\d{4}', flat)
    ver = re.search(r'amendments up to\s+' + MONTH_WORDS, flat) or re.search(r'Date of Commencement:\s*' + MONTH_WORDS, flat) \
        or re.search(r'Date of Assent:\s*' + MONTH_WORDS, flat)
    version = iso_date('%s %s %s' % ver.groups()) if ver else None
    titles = {n: t for n, t in expected}
    provisions, schedules, preamble = {}, [], []
    cur, i = None, 0
    in_sched = False
    margin = {norm(t).rstrip('.') for t in titles.values()}

    def is_margin(t):
        """a margin note: one or more section titles and nothing else"""
        frags = [norm(f).rstrip('.') for f in re.split(r'(?<=\.)\s+', t) if f.strip()]
        return frags and all(f in margin for f in frags)

    def close():
        if cur is not None:
            body_lines_ = cur.pop('lines')
            cur['text'] = render(body_lines_)
            cur['clauses'] = [{'ref': '(' + c.group(1) + ')', 'text': tidy(c.group(2))}
                              for c in (RE_CLAUSE.match(l) for l in join_wrapped(body_lines_)) if c]
            provisions[cur['num']] = cur

    for t in body:
        if is_margin(t):
            continue
        if not in_sched and i < len(expected):
            # the next expected section (looking 3 ahead, so one missed heading does
            # not stop the rest); a scanned margin note can run into the line
            # ("Examination 41. Goods entered ...")
            hit = None
            for j in range(i, min(i + 4, len(expected))):
                num = expected[j][0]
                m = re.match(r'^(?:[A-Z][a-z]+(?:\s+[a-z]+){0,4}\s+)?' + re.escape(num) + r'\.\s*(.*)$', t)
                if m:
                    hit = (j, num, m)
                    break
            if hit:
                j, num, m = hit
                close()
                cur = {'num': num, 'title': titles[num], 'chapter': None, 'chapter_title': None,
                       'part': None, 'part_title': None, 'lines': [m.group(1)] if m.group(1) else []}
                i = j + 1
                continue
        if i >= len(expected) and re.match(r'^(?:FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH|EIGHTH|NINTH|TENTH)?\s*SCHEDULE\b', t):
            close()
            cur = None
            in_sched = True
            schedules.append({'id': 'att_%d' % (len(schedules) + 1), 'title': t, 'subtitle': None, 'lines': []})
            continue
        if in_sched:
            schedules[-1]['lines'].append(t)
        elif cur is None:
            preamble.append(t)
        else:
            cur['lines'].append(t)
    close()
    for s in schedules:
        s['text'] = render(s.pop('lines'))
    return {
        'title': title, 'citation': cit.group(0) if cit else None,
        'source': 'Government Printer edition (saved from Kenya Law)', 'source_url': None,
        'version_date': version, 'retrieved': datetime.date.today().isoformat(),
        'note': ('Text of a Government Printer edition (PDF), converted to JSON by tools/build_law_reference.py; '
                 'scanned editions carry OCR errors - check against the PDF. There is no copyright on the '
                 'legislative content (Copyright Act, Cap. 130, s. 2).'),
        'provision_label': 'section', 'preamble': render(preamble), 'parts': [],
        'provisions': provisions, 'schedules': schedules,
        'check': {'sections_in_contents': len(expected), 'sections_found': len(provisions),
                  'missing': [n for n, _ in expected if n not in provisions]},
    }


def parse_any(pdf_path):
    """Kenya Law edition first; the older Government Printer layout when it has no Contents"""
    d = parse(pdf_path)
    if d['check']['sections_in_contents'] == 0:
        d = parse_arrangement(pdf_path)
    return d
