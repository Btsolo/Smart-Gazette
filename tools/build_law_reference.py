"""
Builds the law reference files the app shows next to a notice.

Input: a Kenya Law page (new.kenyalaw.org, Akoma Ntoso HTML) saved under
raw/law/ (gitignored - the source page, not our data). Output: one JSON file
in src/main/resources/reference/, beside geography.json:

  {
    "title", "citation", "source_url", "version_date", "retrieved", "note",
    "chapters" | "parts": [{"id", "num", "title", "provisions": ["1", "2", ...]}],
    "provisions": {"179": {"num", "title", "chapter", "chapter_title", "part",
                           "part_title", "text", "clauses": [{"ref", "text"}]}},
    "schedules": [{"id", "title", "subtitle", "text"}]
  }

"provisions" are Articles for the Constitution and sections for an Act, keyed
by number so a citation found in a notice ("Article 179 (4) of the
Constitution", "section 45 of the County Governments Act") can be looked up
directly. The text is kept exactly as published; only whitespace is tidied.

Usage:
  python tools/build_law_reference.py constitution raw/law/constitution.html URL VERSION_DATE
  python tools/build_law_reference.py county_governments_act raw/law/county_governments_act.html URL VERSION_DATE
"""
import datetime, json, os, re, sys
from html.parser import HTMLParser

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
OUT_DIR = os.path.join(REPO, 'src', 'main', 'resources', 'reference')

# depth of a numbered unit inside a provision -> indentation in "text"
LEVEL = {'akn-subsection': 0, 'akn-paragraph': 1, 'akn-subparagraph': 2, 'akn-item': 3}
ONLY_NUM = re.compile(r'^\s*(?:\(\w+\)|\w+\.)?\s*$')     # empty, or just "(2) " / "7. "
VOID = {'br', 'img', 'input', 'meta', 'link', 'hr', 'wbr', 'source', 'col', 'area', 'base'}


class AknParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []                 # (tag, class, eid)
        self.chapters, self.parts, self.provisions, self.schedules = [], [], {}, []
        self.cur_ch = self.cur_part = self.cur_prov = self.cur_sched = None
        self.heading = None             # ('chapter'|'part'|'prov'|'sched'|'subsched', buffer)
        self.lines = None               # body lines of the current provision / schedule
        self.in_num = False
        self.skip = 0                   # inside a block we do not render (none yet)

    # -- helpers --
    def _cls(self, attrs):
        return dict(attrs).get('class', '') or ''

    def _newline(self, indent=0):
        if self.lines is not None:
            self.lines.append(['  ' * indent, ''])

    def _emit(self, text):
        if self.heading is not None:
            self.heading[1].append(text)
        elif self.lines is not None:
            if not self.lines:
                self.lines.append(['', ''])
            self.lines[-1][1] += text

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = a.get('class', '') or ''
        eid = a.get('data-eid', '')
        if tag == 'br' and self.heading is not None:
            self.heading[1].append('\n')
        if tag in VOID:
            return
        self.stack.append((tag, cls, eid))
        if cls == 'akn-chapter':
            self.cur_ch = {'id': eid, 'num': None, 'title': None, 'provisions': []}
            self.chapters.append(self.cur_ch)
            self.cur_part = None
        elif cls == 'akn-part':
            self.cur_part = {'id': eid, 'num': None, 'title': None, 'provisions': []}
            self.parts.append(self.cur_part)
        elif cls == 'akn-section':
            self.cur_prov = {'eid': eid, 'heading': '', 'lines': []}
            self.lines = self.cur_prov['lines']
        elif cls == 'akn-attachment':
            self.cur_sched = {'id': eid, 'title': '', 'subtitle': '', 'lines': []}
            self.schedules.append(self.cur_sched)
            self.lines = self.cur_sched['lines']
            self.cur_prov = None
        elif cls in LEVEL and self.lines is not None:
            self._newline(LEVEL[cls])
        elif cls == 'akn-tr' and self.lines is not None:
            self._newline(1)
        elif cls == 'akn-td' and self.lines is not None and self.lines and self.lines[-1][1]:
            self._emit(' | ')
        elif cls == 'akn-num':
            self.in_num = True
        elif cls == 'akn-p' and self.lines is not None and self.lines \
                and not ONLY_NUM.match(self.lines[-1][1]) \
                and not self.in_num and self.stack[-2][1] not in ('akn-content',):
            # a second paragraph inside an intro / wrap-up starts a new line
            # (but the intro right after "(2) " stays on the number's line)
            self._newline(len(self.lines[-1][0]) // 2)
        if tag in ('h2', 'h3'):
            owner = self.stack[-2][1] if len(self.stack) > 1 else ''
            if owner == 'akn-chapter':
                self.heading = ('chapter', [])
            elif owner == 'akn-part':
                self.heading = ('part', [])
            elif owner == 'akn-section':
                self.heading = ('prov', [])
            elif cls == 'akn-heading':
                self.heading = ('sched', [])
            elif cls == 'akn-subheading':
                self.heading = ('subsched', [])
            elif cls == 'akn-crossHeading' and self.lines is not None:
                self._newline(0)

    def handle_startendtag(self, tag, attrs):
        if tag == 'br' and self.heading is not None:
            self.heading[1].append('\n')

    def handle_endtag(self, tag):
        if tag in VOID or not any(x[0] == tag for x in self.stack):
            return
        while True:                     # tolerate unclosed inner tags
            t, cls, eid = self.stack.pop()
            if t == tag:
                break
        if cls == 'akn-num':
            self.in_num = False
            self._emit(' ')
        if tag in ('h2', 'h3') and self.heading is not None:
            kind, buf = self.heading
            self.heading = None
            text = ''.join(buf)
            if kind == 'chapter':
                first, _, rest = text.partition('\n')
                self.cur_ch['num'] = re.sub(r'(?i)^chapter\s+', '', first).strip().title()
                self.cur_ch['title'] = tidy(rest) or None
            elif kind == 'part':
                m = re.match(r'\s*Part\s+(\S+)\s*\W\s*(.*)', text, re.S)
                self.cur_part['num'] = m.group(1) if m else None
                self.cur_part['title'] = tidy(m.group(2) if m else text)
            elif kind == 'prov':
                self.cur_prov['heading'] = tidy(text)
            elif kind == 'sched':
                self.cur_sched['title'] = tidy(text)
            elif kind == 'subsched':
                self.cur_sched['subtitle'] = tidy(text)
        if cls == 'akn-section' and self.cur_prov is not None:
            self._close_provision()
        elif cls == 'akn-attachment' and self.cur_sched is not None:
            self.cur_sched['text'] = render(self.cur_sched.pop('lines'))
            self.lines = None
            self.cur_sched = None

    def handle_data(self, data):
        if self.heading is not None or self.lines is not None:
            self._emit(data)

    def _close_provision(self):
        p = self.cur_prov
        m = re.match(r'(\d+[A-Z]*)\.\s*(.*)', p['heading'])
        num, title = (m.group(1), m.group(2)) if m else (p['eid'], p['heading'])
        lines = p['lines']
        clauses = [{'ref': c.group(1), 'text': tidy(c.group(2))}
                   for c in (re.match(r'\s*(\(\w+\))\s*(.*)', l[1]) for l in lines if not l[0]) if c]
        rec = {'num': num, 'title': title,
               'chapter': self.cur_ch['num'] if self.cur_ch else None,
               'chapter_title': self.cur_ch['title'] if self.cur_ch else None,
               'part': self.cur_part['num'] if self.cur_part else None,
               'part_title': self.cur_part['title'] if self.cur_part else None,
               'text': render(lines), 'clauses': clauses}
        self.provisions[num] = rec
        if self.cur_part is not None:
            self.cur_part['provisions'].append(num)
        if self.cur_ch is not None:
            self.cur_ch['provisions'].append(num)
        self.cur_prov = None
        self.lines = None


def tidy(s):
    return re.sub(r'\s+', ' ', s or '').strip()


def render(lines):
    return '\n'.join(ind + tidy(t) for ind, t in lines if tidy(t))


def build(key, html_path, url, version_date):
    html = open(html_path, encoding='utf-8').read()
    title = tidy(re.sub(r'<[^>]+>', '', re.search(r'<title>(.*?)</title>', html, re.S).group(1)))
    title = re.sub(r'\s*[-–|]\s*Kenya Law.*$', '', title)
    p = AknParser()
    p.feed(html)
    doc = {
        'title': title,
        'source': 'Kenya Law (National Council for Law Reporting)',
        'source_url': url,
        'version_date': version_date,
        'retrieved': datetime.date.today().isoformat(),
        'note': ('Official text as published by Kenya Law, converted to JSON by '
                 'tools/build_law_reference.py. Kenyan written law is not subject to '
                 'copyright (Copyright Act, Cap. 130, s. 2). Check source_url for '
                 'amendments after version_date.'),
        'provision_label': 'Article' if key == 'constitution' else 'section',
    }
    if p.chapters:
        doc['chapters'] = p.chapters
    doc['parts'] = p.parts
    doc['provisions'] = p.provisions
    doc['schedules'] = p.schedules
    os.makedirs(OUT_DIR, exist_ok=True)
    out = os.path.join(OUT_DIR, key + '.json')
    json.dump(doc, open(out, 'w', encoding='utf-8', newline='\n'), ensure_ascii=False, indent=1)
    print('%s: %d provisions, %d chapters, %d parts, %d schedules -> %s (%.0f KB)'
          % (key, len(p.provisions), len(p.chapters), len(p.parts), len(p.schedules),
             os.path.relpath(out, REPO), os.path.getsize(out) / 1024))
    return doc


if __name__ == '__main__':
    build(*sys.argv[1:5])
