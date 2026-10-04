"""
Category links prototype - measurement only (plan: Phase 6, linking).

A notice has ONE primary category (category_census.categorise - the priority
order decides, e.g. an IEBC commissioner's appointment is Elections), but it
can also be relevant to others. Instead of filing it twice (a duplicate
article), we would store links: primary=Elections, also=[Appointments], so the
Appointments page can list it as "related" without a second article.

This prototype finds the secondary candidates from the HEADING only (first
400 characters): every heading rule and every category signal that also fires
there, minus the primary. It reports how often each pair occurs, so the real
feature can be designed on numbers.

Usage: python tools/category_links.py 2022 2023 2024 2025 2026
Writes reports/eval/category_links.json
"""
import json, os, random, re, sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import category_eval as E
import category_census as CEN
import corpus_report as R

# heading-level signals for categories that have no heading rule of their own
HEAD_SIGNALS = [
    ('Appointments', re.compile(r'\bAPPOINTMENTS?\b|\bRE-?\s*APPOINTMENT\b|\bNOMINATION\b')),
    ('Tenders', re.compile(r'\bTENDER\b|PROCUREMENT|EXPRESSION\s*OF\s*INTEREST')),
    ('Licensing', re.compile(r'\bLICEN[CS]E|\bPERMIT\b')),
    ('Legislation', re.compile(r'\bREGULATIONS,?\s*20\d\d|\bRULES,?\s*20\d\d|\bBILL,?\s*20\d\d')),
    ('Land_Property', re.compile(r'THE\s*LAND\s*ACT|PHYSICAL\s*AND\s*LAND\s*USE\s*PLANNING|COMPULSORY\s*ACQUISITION')),
    ('Public_Service_HR', re.compile(r'\bPROMOTION\b|\bREDESIGNATION\b|\bRETIREMENT\b')),
]


def links(notice, primary=None):
    """(primary, [secondary categories]) from the heading."""
    primary = primary or R.canon(CEN.categorise(notice))
    head = notice[:400]
    found = []
    for name, pat, unless in CEN.HEADING_CATEGORIES:
        if pat.search(head) and not (unless and unless.search(head)):
            found.append(name)
    if CEN.county_heading(notice):
        found.append('County_Government')
    for name, pat in HEAD_SIGNALS:
        if pat.search(head):
            found.append(name)
    out = []
    for c in found:
        if c != primary and c not in out:
            out.append(c)
    return primary, out


def main(years):
    pairs, n, linked = Counter(), 0, 0
    ex = defaultdict(list)
    for y in years:
        for slug, num, t in E.cleaned_notices(y):
            n += 1
            p, sec = links(t)
            if sec:
                linked += 1
            for s in sec:
                pairs[(p, s)] += 1
                if len(ex[(p, s)]) < 30:
                    ex[(p, s)].append('%s/%d | %s' % (y, num, R.heading(t)[1][:70]))
    print('notices %d | with at least one secondary link: %d (%.1f%%)\n' % (n, linked, 100.0 * linked / n))
    print('%-20s %-20s %6s' % ('primary', 'also relevant to', 'n'))
    random.seed(4)
    for (p, s), k in pairs.most_common(20):
        print('%-20s %-20s %6d   e.g. %s' % (p, s, k, random.choice(ex[(p, s)])))
    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    json.dump({'notices': n, 'linked': linked,
               'pairs': [[p, s, k] for (p, s), k in pairs.most_common()],
               'examples': {'%s -> %s' % k: v for k, v in ex.items()}},
              open(os.path.join(REPO, 'reports', 'eval', 'category_links.json'), 'w', encoding='utf-8'), indent=1)


if __name__ == '__main__':
    main(sys.argv[1:] or ['2022', '2023', '2024', '2025', '2026'])
