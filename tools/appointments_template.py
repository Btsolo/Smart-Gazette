"""
Rule-based extractor for appointment notices.
Spec: docs/specs/appointments-template.md

The notice is one formula (82% of 641 notices, 2022-2026):
  THE <ACT> <BODY> APPOINTMENT
  IN EXERCISE of the powers conferred by <provision> of the <Act>, [I, <name>,
  President ...,] <authority> (re-)appoint(s)- <names> to be / as <role> of
  <body>, for a period of <term>, with effect from <date>.
  Dated the <date>.  <SIGNATORY>, <title>.

Returns a list with one record per appointed person, or None for anything
else (tables and schedules, taskforces, customs places, court guardianship)
or when any name in the list does not look like a personal name - never a
partial list. Field names follow schemas/field/appointments.json.
Java port: AppointmentsTemplate (keep in sync).
"""
import os, re, sys
import importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
_ns = importlib.util.spec_from_file_location('names', os.path.join(HERE, 'names.py'))
NAMES = importlib.util.module_from_spec(_ns); _ns.loader.exec_module(NAMES)

F = re.I | re.S
# page-footer residue inside long lists ("83848384217") and running heads
JUNK = re.compile(r'(?<=\s)\d{8,}(?=\s)|\[\d+\s+\d+\S*\s+\d{1,2}:\d{2}\s*[AP]M')
# the power clause: "IN EXERCISE of the powers conferred by section 36 (1) of
# the Universities Act" / "PURSUANT to section 2 (3) of the ... Act"
RE_POWER = re.compile(r'(?:exercise\s+of\s+(?:the\s+)?powers?\s+conferred\s*(?:by|on|under|upon)?|pursuant\s+to)\s+'
                      r'(?:the\s+provisions\s+of\s+)?(?P<prov>(?:section|article|regulation|paragraph|rule|sections)\b.+?)'
                      r'\s+of\s+(?:the\s+)?(?:of\s+the\s+)?(?P<act>(?:[A-Z][^,;]*?\s+)??(?:Act|Constitution(?:\s+of\s+Kenya)?|Order|Regulations)\b(?:,?\s*\d{4}\b)?)', F)
# the verb, case-sensitive (the uppercase heading "APPOINTMENT" is not it);
# glued to the authority ("Healthappoints-") or to the list ("appoints--")
RE_VERB = re.compile(r'(?P<verb>re-?\s*appoints?|appoints?|makes\s+the\s+following\s+appointments?|revokes?\s+the\s+\*?appointments?\s+of)'
                     r'(?:\s*[-:\u2013]+\s*|\s+(?=[A-Z]))', re.S)
# "to be" / "as" + the role; "to be" may be glued to the last name
# ("DOEto be", "Rottto be"), "as" only after a capital or a bracket
# ("ROEas a"); "as read with section 51" is a legal phrase, not a role
RE_ROLE = re.compile(r'(?:\s*,?\s*(?<![A-Za-z])|(?<=[A-Za-z.)]))to\s*(?:be|serve\s+as)\s+(?P<r1>.+?)'
                     r'(?=,\s*for\s|\s+for\s+(?:a\s+|the\s+|another\s+)?(?:further\s+)?(?:period|term)|,?\s*with\s+effect|\.\s*\n|\.\s*Dated|\.\s*$|\n\s*Dated)'
                     r'|(?:\s*,?\s+|(?<=[A-Z.)]))as\s+(?!read\b)(?P<r2>[A-Za-z].+?)'
                     r'(?=,\s*for\s|\s+for\s+(?:a\s+|the\s+|another\s+)?(?:further\s+)?(?:period|term)|,?\s*with\s+effect|\.\s*\n|\.\s*Dated|\.\s*$|\n\s*Dated)', re.S)
RE_TERM = re.compile(r'for\s+(?:a\s+|the\s+|another\s+)?(?:further\s+)?(?:period|term)\s+of\s+(?P<t>[a-z\- ]+\(\s*\d+\s*\)\s*(?:years?|months?))', F)
RE_EFF = re.compile(r'with\s+effect\s+from\s*(?:the\s*)?(?P<d>\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of\s+)?[A-Za-z]+,?\s*\d{4})', F)
RE_DATED = re.compile(r'Dated\s+(?:the\s*)?(?P<d>\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:day\s+of\s+)?[A-Za-z]+,?\s*\d{4})\.?\s*'
                      r'(?P<name>[A-Z][A-Z.\'\- ]*[A-Z])\s*,\s*(?P<title>[^\n]+)', re.S)
RE_GN = re.compile(r'(?:Gazette\s+Notice\s+Nos?\.?|G\.\s*N\.?\s*(?:No\.?)?)\s*(?P<n>\d+)\s*(?:of|/)\s*(?P<y>\d{4})', F)
RE_LABEL = re.compile(r'Under\s+(?P<l>(?:paragraph|sub-?section|section|para\.?|regulation|rule|(?:first|second|third)\s+schedule)\s*[^-\u2013]{0,40}?)\s*[-\u2013]+\s*', F)
# a role printed with the names: "Chairperson: X, Members: Y, Z" (switches the
# role for the names after it) or "X - Chairperson" (one name)
ROLES = r'(?:Vice[- ]?)?Chair(?:person|man|woman)|Members?|Registrar|Secretary'
RE_ROLE_PREFIX = re.compile(r'\b(?P<r>' + ROLES + r')\s*:\s*|\b(?P<m>Members)\s+(?=[A-Z])', re.I)
# "the (Non-Executive) Chairperson and Members of X"
RE_CHAIR_AND_MEMBERS = re.compile(r'^(?:Non-Executive\s+)?Chair\w*\s+and\s+Members?$', re.I)
RE_ROLE_SUFFIX = re.compile(r'\s*[-\u2013]\s*(?P<r>' + ROLES + r')$', re.I)
# "NAME under section 6 (2) (f) of the Act" - the provision after the name
RE_UNDER_AFTER = re.compile(r'\s+under\s+(?P<l>(?:section|paragraph|regulation)\b.*)$', re.I)
PLACE = re.compile(r'customs\s+area|transit\s+shed|for\s+the\s+purposes?\s+of|for\s+purposes\s+of', F)
MIXED = re.compile(r'\band\b|,|respectively', F)
# honorifics: before the name ("Eng.", "Amb.", "BRIG (RTD.)", "Lady Justice")
# or after it in brackets ("(DR.)", "(Prof.)", "(RTD.) GEN.")
HON = r'(?:Dr|Prof|Eng|Amb|Hon|Rtd|Gen|Brig|Col|Maj|Lt|Captain|Capt|Canon|Justice|Lady\s+Justice|Bishop|Rev|Ms|Mrs|Mr|Arch|F?CPA|Sen|Fr|Amb|Ambassador|Gen\.?\s*\(Rtd\))'
RE_HON_PRE = re.compile(r'^(?P<h>(?:' + HON + r'\.?\s*(?:\(\s*Rtd\.?\s*\)\s*)?)+)\s+(?=[A-Z])', re.I)
RE_HON_POST = re.compile(r'\s*\(\s*(?P<h>(?:' + HON + r')\.?(?:\s*(?:' + HON + r')\.?)*(?:\s*\(?\s*Rtd\.?\s*\)?)?)\s*\)\s*(?P<g>(?:Gen|Brig|Col|Maj)\.?)?\s*$', re.I)
NOT_A_NAME = re.compile(r'\b(?:board|council|act|the|of|following|persons?|members?|under|paragraph|section|schedule|'
                        r'chair\w*|committee|authority|appoint\w*|to|be|serve|as|is|whose|names?|for|with|effect|period|vide|gazette|notice|'
                        r'republic|kenya\s+(?:defence|forces))\b|\d', re.I)
SINGULAR = [(re.compile(r'ies$'), 'y'), (re.compile(r'(?<=[^s])s$'), '')]


def _t(s):
    if not s:
        return None
    s = re.sub(r'\s+', ' ', s).strip(' ,.;:')
    return s or None


def _honorific(name):
    """(name, honorific) - the title printed before or after the name."""
    hs = []
    m = RE_HON_POST.search(name)
    while m:                                       # "(DR.) (BISHOP)"
        hs.insert(0, _t(m.group('h') + (' ' + m.group('g') if m.group('g') else '')))
        name = name[:m.start()]
        m = RE_HON_POST.search(name)
    m = RE_HON_PRE.match(name)
    if m:
        hs.insert(0, _t(m.group('h')))
        name = name[m.end():]
    h = ' '.join(x for x in hs if x)
    return _t(name), (h or None)


def _role_word(r):
    r = _t(r)
    return 'Member' if re.match(r'members?$', r, re.I) else r[:1].upper() + r[1:]


def _names(raw):
    """[(name, honorific, provision label, own role)] or None when any piece is
    not a personal name."""
    # a bracketed honorific followed by the next name with no comma:
    # "Jane Doe (Ms.) John Roe" -> two people
    raw = re.sub(r'(\([^)]{1,12}\))\s+(?=[A-Z][a-z])', r'\1, ', raw)
    # "Under paragraph (d)-" labels and "Chairperson:" / "Members:" switches,
    # in the order printed; each applies to the names after it
    marks = sorted([(m.start(), m.end(), 'label', _t(m.group('l'))) for m in RE_LABEL.finditer(raw)] +
                   [(m.start(), m.end(), 'role', _role_word(m.group('r') or m.group('m'))) for m in RE_ROLE_PREFIX.finditer(raw)])
    pieces, label, role, pos = [], None, None, 0
    for start, end, kind, val in marks:
        if start < pos:
            continue
        pieces.append((raw[pos:start], label, role))
        if kind == 'label':
            label = val
        else:
            role = val
        pos = end
    pieces.append((raw[pos:], label, role))
    out = []
    for text, lab, rl in pieces:
        for p in re.split(r'[,;\n]|\s+and\s+(?=[A-Z])', text):
            p = _t(p)
            if not p:
                continue
            own, plab = rl, lab
            m = RE_ROLE_SUFFIX.search(p)
            if m:
                own, p = _role_word(m.group('r')), p[:m.start()]
            m = RE_UNDER_AFTER.search(p)
            if m:
                plab, p = _t(m.group('l')), p[:m.start()]
            name, hon = _honorific(p)
            if not name:
                if hon and out and not out[-1][1]:
                    out[-1] = (out[-1][0], hon) + out[-1][2:]   # "Jane Doe, (Dr.)"
                    continue
                return None
            clean, _ = NAMES.clean_name(name)
            if not clean or NOT_A_NAME.search(clean) or len(clean) > 60 or not 2 <= len(clean.split()) <= 6:
                return None
            out.append((clean, hon, plab, own))
    return out or None


def _split_role(role, several):
    """'the Non-Executive Chairperson of the Board of X' -> ('Non-Executive Chairperson', 'Board of X')"""
    role = re.sub(r'^(?:a|an|the)\s+', '', role, flags=re.I)
    m = re.search(r'\s+(?:of|to)\s+(?:the\s+)?', role)
    pos, agency = (role[:m.start()], role[m.end():]) if m else (role, None)
    if several:
        for rx, rep in SINGULAR:
            if rx.search(pos):
                pos = rx.sub(rep, pos)
                break
    pos = _t(pos)
    return (pos[:1].upper() + pos[1:] if pos else None), _t(agency)


def _authority(pre):
    """The appointing authority, from the text between the power clause and the verb."""
    pre = _t(pre) or ''
    m = re.search(r'(?:^|,\s*|\s)I,?\s+[^,]+,\s*(?P<a>.+)$', pre)
    if m:
        a = m.group('a')
        if re.match(r'President\b', a):
            return 'President'
        return _t(re.split(r',\s*(?=and\b)|\s+and\s+Commander', a)[0])
    # the first ", the <Authority>" after the power clause: a later ", the"
    # is inside the title ("for Information, Communications and the Digital Economy")
    m = re.search(r'(?:^|,\s*)the\s+(?=[A-Z])', pre)
    if not m:
        return None
    return _t(pre[m.end():])


def extract(notice):
    return explain(notice)[0]


def explain(notice):
    """(records, None) or (None, reason)."""
    t = JUNK.sub(' ', notice)
    if ' | ' in t:
        return None, 'table / schedule'
    verb = RE_VERB.search(t)
    if not verb:
        return None, 'no verb formula'
    rest = t[verb.end():]
    role = RE_ROLE.search(rest)
    if not role:
        return None, 'no role'
    names_raw = rest[:role.start()]
    role_txt = _t(role.group('r1') or role.group('r2'))
    if not role_txt:
        return None, 'no role'
    if PLACE.search(role_txt):
        return None, 'place, not a person'
    names = _names(names_raw)
    if not names:
        return None, 'names check'
    if len(names) > 40:
        return None, 'list over 40 names'
    several = len(names) > 1
    position, agency = _split_role(role_txt, several)
    # a combined role ("a Member and Chairperson") belongs to a single person;
    # "the Chairperson and Members of X" is read when every name carries its
    # role, or when exactly one is labelled chair (the others are then the
    # members, as printed) - never by guessing who the chair is
    if position and MIXED.search(position) and several and not all(n[3] for n in names):
        chairs = [n for n in names if n[3] and n[3].lower().startswith('chair')]
        if RE_CHAIR_AND_MEMBERS.match(position) and len(chairs) == 1 and all(n[3] in (None, 'Member') or n in chairs for n in names):
            names = [n if n[3] else n[:3] + ('Member',) for n in names]
        else:
            return None, 'mixed roles'
    if not position:
        return None, 'no role'
    power = RE_POWER.search(t[:verb.start()])
    pre = t[power.end():verb.start()] if power else t[:verb.start()].split('\n')[-1]
    v = verb.group('verb').lower()
    kind = 'revocation' if v.startswith('revoke') else ('re-appointment' if v.startswith('re') else 'appointment')
    term = RE_TERM.search(rest)
    eff = RE_EFF.search(rest)
    dated = RE_DATED.search(t)
    gns = ['%s/%s' % (m.group('n'), m.group('y')) for m in RE_GN.finditer(t)] if re.search(r'revok', t, re.I) else []
    nid = re.match(r'\s*GAZETTE NOTICE NO\.\s*(\d+)', notice)
    base_prov = _t(power.group('prov')) if power else None
    out = []
    for name, hon, label, own in names:
        out.append({
            'person_name': name,
            'honorific': hon,
            'position': own or position,
            'agency': agency,
            'appointment_type': kind,
            'appointing_authority': _authority(pre),
            'term_length': _t(term.group('t')) if term else None,
            'effective_date': _t(eff.group('d')) if eff else None,
            'revokes_gn_number': '; '.join(dict.fromkeys(gns)) or None,
            'act': _t(power.group('act')) if power else None,
            'legal_provision': '; '.join(x for x in (base_prov, label) if x) or None,
            'signatory': _t(dated.group('name')) if dated else None,
            'date_signed': _t(dated.group('d')) if dated else None,
            'notice_id': nid.group(1) if nid else None,
        })
    return out, None


if __name__ == '__main__':
    import random, collections
    sys.path.insert(0, HERE)
    import category_eval as E, category_census as C
    rows = [(y, n, t) for y in ['2022', '2023', '2024', '2025', '2026'] for s, n, t in E.cleaned_notices(y)
            if C.categorise(t) == 'appointments']
    res = [(r, explain(r[2])) for r in rows]
    ok = [x for x in res if x[1][0]]
    why = collections.Counter(x[1][1].split(':')[0] for x in res if not x[1][0])
    people = sum(len(x[1][0]) for x in ok)
    print('appointments %d | read %d (%.1f%%) | people %d' % (len(rows), len(ok), 100.0 * len(ok) / len(rows), people))
    print('not read, by reason:', dict(why.most_common()))
    fill = collections.Counter(k for _, (recs, _) in ok for r in recs for k, v in r.items() if v)
    print('fields filled:', ', '.join('%s %.0f%%' % (k, 100.0 * v / people) for k, v in fill.most_common()))
    random.seed(int(sys.argv[1]) if len(sys.argv) > 1 else 1)
    mode = sys.argv[2] if len(sys.argv) > 2 else 'read'
    if mode == 'read':
        for r, (recs, _) in random.sample(ok, 10):
            print('---', r[0], r[1])
            for x in recs:
                print('   ', {k: v for k, v in x.items() if v and k not in ('notice_id',)})
    else:
        for r, (_, w) in random.sample([x for x in res if x[1][1] and x[1][1].startswith(mode)], 6):
            print('---', w, r[0], r[1], repr(r[2][:600]))
