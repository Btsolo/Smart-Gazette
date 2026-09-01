"""
Proper-noun handling shared by every template.

Names are the load-bearing field in a gazette notice - a wrong deceased name
or proprietor makes the record useless and unsearchable. They are also where
the upstream fragment-joiner does the most damage. So all name handling lives
here rather than being re-implemented per template.

clean_name() returns (name, extras) where extras carries anything peeled off
that is real information but is not part of the name itself (an ID number, a
capacity such as "as administrators").
"""
import re

# Trailing clauses that describe a capacity rather than forming part of a name.
CAPACITY = re.compile(
    r'\s*,?\s*\bas\s+(?:the\s+|duly\s+appointed\s+|lawfully\s+appointed\s+)?'
    r'(?:administrators?|administratrix|executors?|executrix|trustees?|attorneys?|'
    r'tenants?\s+in\s+common|joint\s+tenants?|legal\s+representatives?|'
    r'personal\s+representatives?)\b.*$', re.I)

# "X alias Y alias Z" - the aliases are real data, not noise, and belong in
# their own field rather than being glued onto the name.
ALIAS = re.compile(r'\s+alias\s+', re.I)

ADDRESS   = re.compile(r'\s*,?\s*\bof\s+P\.?\s*O\.?\s*Box.*$', re.I)
ID_PAREN  = re.compile(r'\s*\(\s*(?:ID|I\.D\.)\s*/?\s*(?P<id>[\w/\-]+)\)?\s*', re.I)
PAREN_ANY = re.compile(r'\s*\([^)]*\)\s*')
QUANTIFIER= re.compile(r'[,\s]+(?:both|all|each|jointly)$', re.I)
# a parenthetical removed mid-clause can leave a dangling verb ("X, , is the")
DANGLING  = re.compile(r'[,\s]+(?:is|are|was|were)(?:\s+the)?$', re.I)
LEAD_QUANT= re.compile(r'^(?:both|all|each)\s+', re.I)

def clean_name(raw):
    """Normalise one personal or corporate name. Returns (name, extras)."""
    if not raw:
        return None, {}
    extras = {}
    s = re.sub(r'\s+', ' ', raw).strip(' ,.;:')

    parts = ALIAS.split(s)
    if len(parts) > 1:
        s = parts[0]
        extras['aliases'] = [re.sub(r'\s+', ' ', p).strip(' ,.') for p in parts[1:] if p.strip()]

    m = ID_PAREN.search(s)
    if m:
        extras['id_number'] = m.group('id')
        s = ID_PAREN.sub(' ', s)

    m = CAPACITY.search(s)
    if m:
        extras['capacity'] = re.sub(r'^\s*,?\s*as\s+(?:the\s+)?', '', m.group(0)).strip(' ,.')
        s = CAPACITY.sub('', s)

    s = ADDRESS.sub('', s)          # "Peter Thuo, of P.O. Box 24" -> "Peter Thuo"
    s = PAREN_ANY.sub(' ', s)       # any remaining parenthetical aside
    s = LEAD_QUANT.sub('', s)
    s = QUANTIFIER.sub('', s)
    s = DANGLING.sub('', s)
    s = re.sub(r'\s*,\s*,\s*', ', ', s)
    s = re.sub(r'\s*-\s*', '-', s)  # "Ng'ang'a - Kimani" -> "Ng'ang'a-Kimani"
    s = re.sub(r'\s+', ' ', s).strip(' ,.;:-')

    if len(s) < 3 or not re.search(r'[A-Za-z]{2}', s):
        return None, extras
    return s, extras

def clean_names(raw_list):
    """Clean a list of names, dropping any that do not survive. Returns
    (names, extras_by_name)."""
    out, ex = [], {}
    for r in raw_list or []:
        n, e = clean_name(r)
        if n:
            out.append(n)
            if e: ex[n] = e
    return out, ex

def looks_suspect(name):
    """Residual quality signals, for the audit and the failure log."""
    if not name: return ['empty']
    p = []
    if re.search(r'[a-z][A-Z]', name):        p.append('glued')
    if re.search(r'\d', name):                p.append('digits')
    if len(name) > 55:                        p.append('too-long')
    if re.search(r'\bP\.?O\.?\s*Box\b', name, re.I): p.append('address-leak')
    return p
