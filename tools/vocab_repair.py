"""
Vocabulary-guided repair of pdf-inspector text - PROTOTYPE, measurement only.

pdf-inspector gives the right reading order but breaks words and numbers
("a dministration", "20 21", "knownas", "application shaving"). pdftotext
reads the same text layer with every word intact but can detach headings from
their bodies. So keep pdf-inspector's order and use the words pdftotext saw in
the SAME gazette as a dictionary:

  join   "a dministration" -> "administration"  (joined word in the dictionary,
                                                  a part is not)
  split  "knownas"         -> "known as"         (word not in the dictionary,
                                                  both halves are)
  move   "application shaving" -> "applications having"
         "ORDERSS PECIALS" -> "ORDERS SPECIAL"   (one letter across the boundary
                                                  turns two non-words into words)
  ordinal "7 thJanuary"    -> "7th January"

Applied after clean() + lock, so notice boundaries are untouched.

Usage (evaluation): python tools/vocab_repair.py 2023 [2022 2024 2026]
"""
import glob, json, os, re, sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

MONTHS = 'January|February|March|April|May|June|July|August|September|October|November|December'
TOKEN = re.compile(r"^([(\[\"'“‘]*)(.*?)([)\]\"'”’.,;:]*)$")


def build_vocab(text_layer):
    return Counter(w.lower() for w in re.findall(r"[A-Za-z0-9]+(?:['’][A-Za-z]+)?", text_layer))


VOCAB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gazette_vocab.txt')


def build_corpus_vocab(years, min_count=3, out=VOCAB_FILE):
    """One Gazette dictionary from the text layers of the given years (our own
    material): every word seen >= min_count times, with its count. Shipped with
    the code, so production needs no per-gazette pdftotext run."""
    import corpus_report as R
    V = Counter()
    for y in years:
        for p in glob.glob(os.path.join(R.REPO, 'raw', y, '*.pdftotext.txt')):
            # born-digital only: scan text layers carry letter-spaced fragments
            # ("G a z et te") that would pollute the dictionary
            sj = os.path.join(R.REPO, 'reports', 'ocr', y, os.path.basename(p).split('.')[0], 'summary.json')
            if os.path.exists(sj) and json.load(open(sj, encoding='utf-8'))['summary']['kind'] != 'born-digital':
                continue
            V.update(build_vocab(R.read_text(p)))
    with open(out, 'w', encoding='utf-8', newline='\n') as f:
        f.write('# Gazette vocabulary from the pdftotext layer of %s; word<TAB>count, count >= %d\n' % (', '.join(years), min_count))
        for w, c in sorted(V.items(), key=lambda kv: -kv[1]):
            if c >= min_count:
                f.write('%s\t%d\n' % (w, c))
    return out


def load_corpus_vocab(path=VOCAB_FILE):
    V = Counter()
    for line in open(path, encoding='utf-8'):
        if line.startswith('#'):
            continue
        w, c = line.rstrip('\n').split('\t')
        V[w] = int(c)
    return V


def _core(tok):
    m = TOKEN.match(tok)
    return m.group(1), m.group(2), m.group(3)


SHORT = {'a', 'i', 'of', 'to', 'in', 'at', 'by', 'on', 'as', 'is', 'an', 'or', 'no', 'be', 'it', 'he', 'we',
         'the', 'and', 'for', 'was', 'who', 'are', 'all', 'has', 'her', 'his', 'its', 'not', 'did', 'box', 'son'}


def segment(letters, V, max_word=30):
    """Word-break the letters into dictionary words: fewest words, then the
    most frequent. Words of 1-3 letters must be real short words (initials in
    the dictionary would otherwise shred rare names). Returns cut positions."""
    s = letters.lower()
    n = len(s)
    best = [None] * (n + 1)          # best[i] = (words, -min_freq, cuts) for s[:i]
    best[0] = (0, 0, [])
    for i in range(1, n + 1):
        for j in range(max(0, i - max_word), i):
            if best[j] is None:
                continue
            w = s[j:i]
            f = V.get(w, 0)
            # only well-attested words may be used to re-cut: broken forms
            # ("knownas", "dministration") occur a few times in any real-text
            # dictionary and would otherwise win as "one word"
            if f < RARE or (len(w) <= 3 and w not in SHORT):
                continue
            w, negf, cuts = best[j]
            cand = (w + 1, max(negf, -f), cuts + [i])
            if best[i] is None or cand[:2] < best[i][:2]:
                best[i] = cand
    return best[n][2] if best[n] else None


RARE = 20       # a token seen fewer times than this may be a broken word
RATIO = 5       # a re-cut must make the rarest word >= RATIO times more common


def repair_line(line, V):
    toks = line.split(' ')
    f = lambda w: V.get(w.lower(), 0)
    ok = lambda w: bool(w) and f(w) > 0
    rare = lambda w: bool(w) and w.isalpha() and f(w) < RARE
    i, out = 0, []
    while i < len(toks):
        p1, c1, s1 = _core(toks[i]) if toks[i] else ('', '', '')
        # digits split apart: "20 21" -> "2021", "403 18" -> "40318"
        if c1.isdigit() and i + 1 < len(toks) and toks[i + 1] and not s1:
            p2, c2, s2 = _core(toks[i + 1])
            if c2.isdigit() and not p2 and ok(c1 + c2) and len(c1 + c2) >= 4:
                toks[i + 1] = p1 + c1 + c2 + s2; i += 1; continue
        # a run of letters cut in the wrong places: re-cut with the dictionary.
        # Try the smallest window around a rare (possibly broken) token first.
        if c1.isalpha() and (rare(c1) or (i + 1 < len(toks) and toks[i + 1] and rare(_core(toks[i + 1])[1]))):
            best = None      # (rare tokens repaired, rarest new word's count, width, words)
            for width in (1, 2, 3, 4):
                win = toks[i:i + width]
                if len(win) < width or not all(win):
                    break
                parts = [_core(t) for t in win]
                # punctuation only allowed before the first / after the last token
                if any(p[0] for p in parts[1:]) or any(p[2] for p in parts[:-1]) or not all(p[1].isalpha() for p in parts):
                    break
                letters = ''.join(p[1] for p in parts)
                if len(letters) < 3:
                    continue
                cuts = segment(letters, V)
                if not cuts:
                    continue
                words, a = [], 0
                for c in cuts:
                    words.append(letters[a:c]); a = c
                if words == [p[1] for p in parts] or len(words) > width + 1:
                    continue
                # Guards against re-cutting good text (2022: "at Onywere" ->
                # "a tOny were", "in Kenya" -> "inKenya"):
                # (1) a common word of 3+ letters in the window must survive as
                #     a word - only short words ("a", "at") may be absorbed,
                #     which is the drop-cap case ("a dministration");
                # (2) more words out than tokens in only when EVERY token is rare
                #     ("ORDERSS PECIALS ITTINGO FTHE").
                #     A pure merge (fewer words out) is exempt: "REGISTRAT ION" ->
                #     "REGISTRATION" absorbs the common fragment "ion", and bad
                #     merges ("in Kenya" -> "inKenya") fail the evidence test below.
                lw = [w.lower() for w in words]
                if len(words) >= width and any(len(p[1]) > 2 and not rare(p[1]) and p[1].lower() not in lw for p in parts):
                    continue
                if len(words) > width and not all(rare(p[1]) for p in parts):
                    continue
                # evidence test: the rarest new word must be clearly more common
                # than the rarest old one
                low = min(f(w) for w in words)
                if low < RATIO * max(1, min(f(p[1]) for p in parts)):
                    continue
                score = (sum(1 for p in parts if rare(p[1])), low, width, words, parts)
                if best is None or score[:2] > best[:2]:
                    best = score
            # keep the window that repairs the most broken tokens with the
            # most common words ("REGISTRAT ION" -> "REGISTRATION", not
            # "REGI STRAT ION"; "a pplication shaving" -> "applications having")
            if best:
                _, _, width, words, parts = best
                words = list(words)
                words[0] = parts[0][0] + words[0]; words[-1] = words[-1] + parts[-1][2]
                out += words; i += width
                continue
        # two names glued at a case boundary: "JohnDoe" -> "John Doe"
        if c1.isalpha():
            m = re.match(r'^((?!(?:Mc|Mac|De|Van|Von|La|Le)[A-Z])[A-Z][a-z]{2,})([A-Z][a-z]{2,})$', c1)
            if m and f(c1) < RARE:
                out += [p1 + m.group(1), m.group(2) + s1]; i += 1; continue
        out.append(toks[i]); i += 1
    return ' '.join(out)


def repair(text, V):
    text = re.sub(r'(\d{1,2})\s?(st|nd|rd|th)\s?(%s)' % MONTHS, r'\1\2 \3', text)
    return '\n'.join(repair_line(l, V) for l in text.split('\n'))


def evaluate(years):
    import corpus_report as R
    import extractor_eval as E
    for y in years:
        tot = Counter()
        for sj in sorted(glob.glob(os.path.join(R.REPO, 'reports', 'ocr', y, '*', 'summary.json'))):
            s = json.load(open(sj, encoding='utf-8'))['summary']
            if s['kind'] != 'born-digital':
                continue
            ptt = R.read_text(os.path.join(R.REPO, 'raw', y, s['slug'] + '.pdftotext.txt'))
            V = build_vocab(ptt)
            cleaned = R.G.apply_ascending_lock(R.G.clean(R.read_text(os.path.join(R.REPO, 'raw', y, s['slug'] + '.inspector.txt'))))
            fixed = repair(cleaned, V)
            ref_w, ref_n = E.words(ptt), E.nums(ptt)
            for k, t in (('before', cleaned), ('after', fixed)):
                tot[k + ' words'] += sum((ref_w & E.words(t)).values())
                tot[k + ' nums'] += sum((ref_n & E.nums(t)).values())
            tot['ref words'] += sum(ref_w.values()); tot['ref nums'] += sum(ref_n.values())
            nb = {n.split('\n', 1)[0]: n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', cleaned) if n.startswith('GAZETTE NOTICE NO.')}
            na = {n.split('\n', 1)[0]: n for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', fixed) if n.startswith('GAZETTE NOTICE NO.')}
            tot['notices before'] += len(nb); tot['notices after'] += len(na)
            for h, n in nb.items():
                cat = R.CEN.categorise(n)
                if cat not in ('court_legal', 'land_property'):
                    continue
                c = R.canon(cat)
                tot[c + ' eligible'] += 1
                tot[c + ' before'] += R.PL.route_notice(n)[3] == 'template'
                if h in na:
                    tot[c + ' after'] += R.PL.route_notice(na[h])[3] == 'template'
                if cat == 'court_legal':
                    for k, t in (('before', n), ('after', na.get(h, ''))):
                        bl = [b for b in re.split(r'(?=CAUSE NO\.)', t) if b.startswith('CAUSE NO.')]
                        tot['blocks ' + k] += len(bl); tot['blocks ok ' + k] += sum(1 for b in bl if R.P.extract(b, t))
        print('== %s  notices %d -> %d' % (y, tot['notices before'], tot['notices after']))
        print('   words intact %.1f%% -> %.1f%% | numbers intact %.1f%% -> %.1f%%' % (
            100 * tot['before words'] / tot['ref words'], 100 * tot['after words'] / tot['ref words'],
            100 * tot['before nums'] / tot['ref nums'], 100 * tot['after nums'] / tot['ref nums']))
        for c in ('Court_Legal', 'Land_Property'):
            e = max(1, tot[c + ' eligible'])
            print('   %-14s notices templated %.1f%% -> %.1f%%  (of %d)' % (c, 100 * tot[c + ' before'] / e, 100 * tot[c + ' after'] / e, e))
        print('   probate blocks templated %.1f%% -> %.1f%%' % (100 * tot['blocks ok before'] / max(1, tot['blocks before']),
                                                           100 * tot['blocks ok after'] / max(1, tot['blocks after'])))


if __name__ == '__main__':
    evaluate(sys.argv[1:] or ['2023'])
