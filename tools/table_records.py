"""
Table rows as records (fix 9): a table's header names its columns, so each
row becomes {role: value}.

  roles(columns) -> {column index: role}
  records(table) -> [{'parcel_id': ..., 'owner': ..., 'area_ha': ...}, ...]

Roles come from the header words, the same words a reader uses:
  "Parcel No." / "Plot No." / "L.R. No."       -> parcel_id
  "Old L.R. No." / "New Parcel No."             -> old_lr / new_parcel (registration-unit conversion)
  "Registered Owner (s)" / "Proprietor"         -> owner
  "Area Acq. (Ha)" / "Approximate Area"         -> area_ha
  "Name" / "Position" / "Role" / "Designation"  -> name / position
Two Gazette print defects are repaired here:
  - an owner cell missing ("Akachiu/Auki/632 | 0.0786"): the area slid into
    the owner column and is moved back;
  - a parcel number split by a space ("Tetu/Unjiru/ 1926").
Values are validated (a parcel has a digit; an area is a number) so a
template can refuse a table it cannot read cleanly.

Java port: TableRecords (keep in sync).
"""
import re

ROLES = [
    ('old_lr',     re.compile(r'\bold\b.*\bl\.?\s*r\b', re.I)),
    ('new_parcel', re.compile(r'\bnew\b.*\b(?:parcel|plot)', re.I)),
    ('parcel_id',  re.compile(r'\b(?:parcel|plot|l\.?\s*r\.?\s*no|title\s*no|land\s*ref)', re.I)),
    ('owner',      re.compile(r'\b(?:owner|proprietor)', re.I)),
    ('area_ha',    re.compile(r'\barea\b|\(\s*ha|\bhectares?\b|\bacq', re.I)),
    ('position',   re.compile(r'\b(?:position|role|designation|representation|capacity)\b', re.I)),
    ('name',       re.compile(r'^\s*(?:full\s+)?names?\b|\bname\s+of\b', re.I)),
]
AREA = re.compile(r'^\d+(?:\s?[.,]\s?\d+)?$')
DECIMAL = re.compile(r'(?<![\d/])\d+\.\d{2,}(?![\d/])')        # an area such as 0.0017
TRAIL_AREA = re.compile(r'\s(\d+\.\d{2,})$')


def roles(columns):
    out, used = {}, set()
    for i, c in enumerate(columns or []):
        for role, pat in ROLES:
            if role not in used and pat.search(c):
                out[i] = role
                used.add(role)
                break
    return out


def area_value(v):
    v = (v or '').replace(' ', '').replace(',', '.')
    try:
        return float(v)
    except ValueError:
        return None


def records(table):
    rs = roles(table.get('columns'))
    if not rs:
        return []
    out = []
    for row in table['rows']:
        # a group label printed across the table ("Wednesday, 2nd July, 2025 at
        # Likoni Chief's office ...", fix 7b): one filled cell of >= 4 words is
        # not a record (a parcel number alone is 1-3 tokens)
        filled = [c for c in row if c.strip()]
        if len(filled) == 1 and len(filled[0].split()) >= 4:
            continue
        rec = {role: (row[i] if i < len(row) else '').strip() for i, role in rs.items()}
        # area slid into the owner column (owner cell not printed), or glued
        # to its end ("TBD 0.0107")
        if 'owner' in rec and not rec.get('area_ha'):
            if AREA.match(rec['owner']):
                rec['area_ha'], rec['owner'] = rec['owner'], ''
            else:
                m = TRAIL_AREA.search(rec['owner'])
                if m:
                    rec['area_ha'], rec['owner'] = m.group(1), rec['owner'][:m.start()].strip()
        for k in ('parcel_id', 'old_lr', 'new_parcel'):
            if rec.get(k):
                rec[k] = re.sub(r'\s*/\s*', '/', rec[k])
        out.append(rec)
    return out


def parcel_ok(p):
    """A parcel id, not a row glued into one cell: it has a digit, no
    area-like decimal ("752 0.0017 John ..."), <= 6 words, and <= 2 words
    after its last numbered part ("Kiganjo/Kiamwangi/2510 Hana Jane
    Roe" carries the owner)."""
    if not p or not re.search(r'\d', p) or DECIMAL.search(p):
        return False
    words = p.split()
    if len(words) > 6:
        return False
    last = max(i for i, w in enumerate(words) if re.search(r'\d', w))
    return len(words) - 1 - last <= 2


def valid(rec):
    """A parcel record is usable when its parcel id is clean (parcel_ok), its
    owner carries no area, and its area, if any, is a number."""
    p = rec.get('parcel_id') or rec.get('new_parcel') or rec.get('old_lr')
    if not parcel_ok(p):
        return False
    if rec.get('owner') and DECIMAL.search(rec['owner']):
        return False
    if 'area_ha' in rec and rec['area_ha'] and area_value(rec['area_ha']) is None:
        return False
    return True
