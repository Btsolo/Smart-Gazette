"""
What laws do Gazette notices cite? - measurement only.

Counts, over the cleaned notices cached by category_eval.py:
  - Constitution citations: "Article 132 (2) (f) of the Constitution",
    "Articles 10 and 232", "section 7 of the Sixth Schedule to the Constitution"
  - Acts cited ("the County Governments Act", "section 45 of the Land
    Registration Act"), overall and in County_Government notices
This tells us which reference texts the app needs so that a notice can be
shown next to the provision it relies on.

Usage: python tools/citation_census.py 2022 2023 2024 2025 2026
Writes reports/eval/citations.json
"""
import json, os, re, sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import category_eval as E
import category_census as CEN

# "Article 132 (2) (f)", "Articles 10, 27 and 232", "Art. 47"
RE_ART = re.compile(
    r'\bArt(?:icles?|\.)\s*(?P<list>\d+[A-Z]?(?:\s*\(\s*[0-9a-z]+\s*\))*'
    r'(?:\s*(?:,|and|to|&)\s*\d+[A-Z]?(?:\s*\(\s*[0-9a-z]+\s*\))*)*)'
    r'(?P<tail>[^.;]{0,80})', re.I)
RE_CONST = re.compile(r'\bConstitution\b', re.I)
RE_SCHED = re.compile(r'\b(First|Second|Third|Fourth|Fifth|Sixth)\s+Schedule\s+to\s+the\s+Constitution', re.I)
RE_ACT = re.compile(r"\bthe\s+((?:[A-Z][A-Za-z'\-]*\s+)(?:(?:and|of|for|the|on|in|to|[A-Z][A-Za-z'\-]*|\([A-Za-z]+\))\s+){0,8}?Act)\b(?:\s*,?\s*(?:Cap\.?\s*\d+[A-Z]?|\d{4}|No\.\s*\d+\s+of\s+\d{4}))?")


def articles(text):
    """Article numbers cited as articles of the Constitution in this text."""
    out = []
    for m in RE_ART.finditer(text):
        # an Article of an Act's own schedule or a treaty is not the Constitution
        window = text[max(0, m.start() - 40): m.end() + 80]
        if not RE_CONST.search(window):
            continue
        out += [int(n) for n in re.findall(r'(?<![(\d])(\d+)(?![\d)])', re.sub(r'\([^)]*\)', ' ', m.group('list')))]
    return [n for n in out if 1 <= n <= 264]


def main(years):
    art_all, art_county = Counter(), Counter()
    acts_all, acts_county = Counter(), Counter()
    sched = Counter()
    notices = cited = cited_county = county = 0
    examples = defaultdict(list)
    for y in years:
        for slug, num, n in E.cleaned_notices(y):
            notices += 1
            is_county = CEN.categorise(n) == 'County_Government'
            county += is_county
            a = sorted(set(articles(n)))
            if a or RE_SCHED.search(n):
                cited += 1
                cited_county += is_county
            for k in a:
                art_all[k] += 1
                if is_county:
                    art_county[k] += 1
                if len(examples[k]) < 2:
                    examples[k].append('%s/%s/%d' % (y, slug, num))
            for s in set(m.group(1).title() for m in RE_SCHED.finditer(n)):
                sched[s] += 1
            acts = set(re.sub(r'\s+', ' ', m.group(1)) for m in RE_ACT.finditer(n))
            for x in acts:
                acts_all[x] += 1
                if is_county:
                    acts_county[x] += 1

    print('notices %d | citing the Constitution by article or schedule: %d (%.1f%%)'
          % (notices, cited, 100.0 * cited / notices))
    print('County_Government notices %d | citing the Constitution: %d (%.1f%%)\n'
          % (county, cited_county, 100.0 * cited_county / max(1, county)))
    print('distinct articles cited: %d of 264' % len(art_all))
    print('top articles (all notices):    ' + ', '.join('%d:%d' % kv for kv in art_all.most_common(25)))
    print('top articles (county notices): ' + ', '.join('%d:%d' % kv for kv in art_county.most_common(15)))
    print('schedules: ' + ', '.join('%s:%d' % kv for kv in sched.most_common()))
    print('\ntop Acts (all notices):')
    for k, v in acts_all.most_common(25):
        print('  %5d  %s' % (v, k))
    print('\ntop Acts (county notices):')
    for k, v in acts_county.most_common(20):
        print('  %5d  %s' % (v, k))
    os.makedirs(os.path.join(REPO, 'reports', 'eval'), exist_ok=True)
    json.dump({'articles': art_all, 'articles_county': art_county, 'schedules': sched,
               'acts': acts_all.most_common(300), 'acts_county': acts_county.most_common(100),
               'examples': examples},
              open(os.path.join(REPO, 'reports', 'eval', 'citations.json'), 'w', encoding='utf-8'), indent=1)


if __name__ == '__main__':
    main(sys.argv[1:] or ['2022', '2023', '2024', '2025', '2026'])
