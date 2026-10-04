"""
Fill the mined part of tools/gazette_lexicon.json from the corpus dictionary.

'observed_spelling_variants': pairs of words that are spelling variants of each
other (British / American and common Gazette alternations) where BOTH forms
occur in the corpus at least MIN times - e.g. licence / license. These are real
variation in the source, not extraction damage, so both must count as words and
should map to one canonical form for keyword matching.

Also checks every curated variant against the corpus and records how often it
occurs (0 = never seen in 2022-2026; keep, but treat as unconfirmed).

Usage: python tools/build_lexicon.py
"""
import json, os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import vocab_repair as VR

HERE = os.path.dirname(os.path.abspath(__file__))
LEX = os.path.join(HERE, 'gazette_lexicon.json')
MIN = 5

# (pattern on form A, replacement giving form B)
RULES = [
    (r'ence$', 'ense'),          # licence / license, defence / defense
    (r'ise$', 'ize'), (r'ised$', 'ized'), (r'ising$', 'izing'), (r'isation$', 'ization'),
    (r'our$', 'or'), (r'ours$', 'ors'),
    (r'gement$', 'gment'),       # judgement / judgment, acknowledgement
    (r'mme$', 'm'),              # programme / program
    (r'tre$', 'ter'), (r'tres$', 'ters'),     # centre / center
]


def main():
    V = VR.load_corpus_vocab()
    lex = json.load(open(LEX, encoding='utf-8'))
    pairs = []
    for w, c in V.items():
        # 5+ letters: shorter pairs are different words (for/four, or/our)
        if not w.isalpha() or c < MIN or len(w) < 5:
            continue
        for pat, rep in RULES:
            # -our/-or at 5 letters catches surname pairs
            if re.search(pat, w) and not (pat.startswith('our') and len(w) < 6):
                b = re.sub(pat, rep, w)
                if b != w and V.get(b, 0) >= MIN:
                    canon = w if c >= V[b] else b
                    pairs.append({'canonical': canon, 'variants': sorted({w, b}),
                                  'counts': {w: c, b: V[b]}})
    seen, out = set(), []
    for p in sorted(pairs, key=lambda p: -sum(p['counts'].values())):
        k = tuple(p['variants'])
        if k not in seen:
            seen.add(k); out.append(p)
    lex['observed_spelling_variants'] = out
    # corpus support for curated entries
    for e in lex['entries']:
        forms = [e['term']] + e.get('variants', []) + e.get('synonyms', [])
        e['corpus_count'] = {f: sum(V.get(t.lower(), 0) for t in [re.sub(r'\W+', '', f.lower())]) for f in forms}
    json.dump(lex, open(LEX, 'w', encoding='utf-8', newline='\n'), indent=1, ensure_ascii=False)
    print('spelling variant pairs:', len(out))
    for p in out[:15]:
        print('  ', p['variants'], p['counts'])


if __name__ == '__main__':
    main()
