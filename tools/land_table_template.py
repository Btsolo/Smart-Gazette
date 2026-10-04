"""
Template for land notices whose content is a parcel table (fix 9):
  - compulsory acquisition schedules (THE LAND ACT, National Land Commission)
  - their corrigenda: "DELETION, CORRIGENDA AND ADDENDUM" - one table per section
  - registration-unit conversions (Old L.R. No. -> New Parcel No.)
The lost-title template (land_template.py) reads one parcel from prose; these
notices list tens to thousands of parcels in tables, so they went to the AI
whole (and long ones were truncated).

extract(notice) -> {'notice_subtype', 'project', 'acquiring_body',
                    'related_notices', 'sections': [{'section', 'parcels'}],
                    'parcel_count', 'rows_not_read'} or None
None when no table has a parcel column, or < 80% of its rows are valid
records (the notice then goes to the AI path as before).
"""
import os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import table_extract as T
import table_records as TR

RE_ACT = re.compile(r'THE\s+LAND\s+(?:ACT|REGISTRATION\s+ACT)|NATIONAL\s+LAND\s+COMMISSION', re.I)
RE_PROJECT = re.compile(r'\b((?:CONSTRUCTION|REHABILITATION|UPGRADING|EXPANSION|DUALLING|IMPROVEMENT|DEVELOPMENT)'
                        r'\s+OF\s+[^a-z]{5,160}?(?:PROJECT|ROAD|LINE|DAM|SCHEME|STATION)S?)\b')
RE_CONVERSION = re.compile(r'CONVERSION\s+AND\s+MIGRATION|REGISTRATION\s+UNITS', re.I)
RE_BODY = re.compile(r'on\s+behalf\s+of\s+(?:the\s+)?(.+?)\s*(?:\(|,|\bgives\b|\bintends\b|\bhereby\b)', re.I | re.S)
RE_FURTHER = re.compile(r'further\s+to\s+Gazette\s+Notices?\s+Nos?\.?\s*(.{0,120}?)(?:,\s*the\b|\bthe\s+National)', re.I | re.S)
RE_REF = re.compile(r'(\d{1,5})\s*(?:of|/)\s*(\d{4})')


def extract(notice):
    if not RE_ACT.search(notice[:600]):
        return None
    sections = []
    total = ok = 0
    for t in T.tables(notice):
        rs = TR.roles(t['columns'])
        if not ({'parcel_id', 'new_parcel', 'old_lr'} & set(rs.values())):
            continue
        allrecs = TR.records(t)
        total += len(allrecs)
        recs = [r for r in allrecs if TR.valid(r)]       # unreadable rows are left out, and counted
        ok += len(recs)
        # a table without its own caption continues the section above (the
        # header was reprinted on the next page with a small difference)
        # the section is the section word itself: captions can carry a wrapped
        # data cell before it ("0.0115 John Doe Roe Corrigendum")
        m = T.SECTION.search(t.get('caption') or '')
        cap = m.group(0).title() if m else None
        if cap is None and sections:
            sections[-1]['parcels'] += recs
            continue
        sections.append({'section': cap, 'parcels': recs})
    if not sections or total == 0 or ok < 0.8 * total:
        return None
    head = notice[:900]
    proj = RE_PROJECT.search(head)
    body = RE_BODY.search(head)
    fur = RE_FURTHER.search(head)
    related = ['%s of %s' % m for m in RE_REF.findall(fur.group(1))] if fur else []
    if RE_CONVERSION.search(head):
        subtype = 'Conversion of land reference numbers to new parcel numbers'
    elif re.search(r'\bDELETION|ADDENDUM|CORRIGEND', head, re.I):
        subtype = 'Correction of a compulsory acquisition schedule'
    else:
        subtype = 'Compulsory acquisition of land'
    return {
        'notice_subtype': subtype,
        'project': re.sub(r'\s+', ' ', proj.group(1)).strip() if proj else None,
        # a stray page number printed inside the clause ("Authority 342")
        'acquiring_body': re.sub(r'\s+\d{1,5}$', '', re.sub(r'\s+', ' ', body.group(1)).strip()) if body else None,
        'related_notices': related,
        'sections': sections,
        'parcel_count': ok,
        'rows_not_read': total - ok,      # left for the reader / AI: shown, not guessed
    }


if __name__ == '__main__':
    import json
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import category_eval as E
    y, slug, num = sys.argv[1], sys.argv[2], int(sys.argv[3])
    for s, n, t in E.cleaned_notices(y):
        if s == slug and n == num:
            r = extract(t)
            if r:
                for sec in r['sections']:
                    sec['parcels'] = sec['parcels'][:3]
            print(json.dumps(r, indent=1, ensure_ascii=False))
