"""
Tesseract vs RapidOCR in the scan lane - measurement only (fix 5 step 2).

For gazettes read by both engines (reports/ocr/<year>/<slug>/p*.txt and r*.txt),
runs the same scan lane (OCR repair + cleaner + templates) on each engine's
text and compares, notice by notice:
  - notices found
  - probate cause blocks / land notices templated by each engine, and by EITHER
    (two witnesses: use the engine whose text the template can read)
  - where both template a cause block: do the extracted case number, deceased
    and petitioners agree? Disagreement = at least one engine misread.

Usage: python tools/ocr_engine_eval.py 2022/no231 2024/no006
"""
import os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import scan_lane as S
import vocab_repair as VR
import category_census as CEN
import probate_template as P
import land_template as L


def notices(text):
    out = {}
    for n in re.split(r'(?=GAZETTE NOTICE NO\. \d+)', text):
        m = re.match(r'GAZETTE NOTICE NO\. (\d+)', n)
        if m:
            out[int(m.group(1))] = n
    return out


def blocks(nmap):
    """{(notice, case_ref): record or None} for probate; {notice: record or None} for land."""
    pb, ld = {}, {}
    for num, n in nmap.items():
        c = CEN.categorise(n)
        if c == 'court_legal':
            for b in P.split_causes(n):
                m = re.match(r'CAUSE NO\.\s*([A-Z]?\s*\d+)', b)
                key = (num, re.sub(r'\s', '', m.group(1)) if m else b[:20])
                pb[key] = P.extract(b, n)
        elif c == 'land_property':
            ld[num] = L.extract(n)
    return pb, ld


def main(items):
    V = VR.load_corpus_vocab()
    tot = {}
    for it in items:
        y, slug = it.split('/')
        t = S.scan_text(S.ocr_pages_tesseract(y, slug), V)
        r = S.scan_text(S.ocr_pages_rapid(y, slug), V)
        nt, nr = notices(t), notices(r)
        pt, lt = blocks(nt)
        pr, lr = blocks(nr)
        keys = set(pt) | set(pr)
        ok_t = sum(1 for k in keys if pt.get(k))
        ok_r = sum(1 for k in keys if pr.get(k))
        ok_e = sum(1 for k in keys if pt.get(k) or pr.get(k))
        both = [k for k in keys if pt.get(k) and pr.get(k)]
        agree = {f: sum(1 for k in both if pt[k][f] == pr[k][f]) for f in ('case_reference', 'deceased_name', 'petitioner_names')}
        lk = set(lt) | set(lr)
        l_t = sum(1 for k in lk if lt.get(k)); l_r = sum(1 for k in lk if lr.get(k))
        l_e = sum(1 for k in lk if lt.get(k) or lr.get(k))
        print('%s: notices tesseract %d, rapidocr %d (both %d)' % (it, len(nt), len(nr), len(set(nt) & set(nr))))
        if keys:
            print('  probate blocks %d | templated: tesseract %d (%.1f%%), rapidocr %d (%.1f%%), either %d (%.1f%%)'
                  % (len(keys), ok_t, 100.0 * ok_t / len(keys), ok_r, 100.0 * ok_r / len(keys), ok_e, 100.0 * ok_e / len(keys)))
            print('  both templated %d: agree on case ref %d, deceased %d, petitioners %d'
                  % (len(both), agree['case_reference'], agree['deceased_name'], agree['petitioner_names']))
            dis = [k for k in both if pt[k]['deceased_name'] != pr[k]['deceased_name']][:8]
            for k in dis:
                print('     %-14s T: %-40s R: %s' % ('%d/%s' % k, pt[k]['deceased_name'][:40], pr[k]['deceased_name'][:40]))
        if lk:
            print('  land notices %d | templated: tesseract %d (%.1f%%), rapidocr %d (%.1f%%), either %d (%.1f%%)'
                  % (len(lk), l_t, 100.0 * l_t / len(lk), l_r, 100.0 * l_r / len(lk), l_e, 100.0 * l_e / len(lk)))
            both_l = [k for k in lk if lt.get(k) and lr.get(k)]
            ag = sum(1 for k in both_l if lt[k]['parcel_id'] == lr[k]['parcel_id'])
            print('  both templated %d: agree on parcel id %d' % (len(both_l), ag))
            for k in [k for k in both_l if lt[k]['parcel_id'] != lr[k]['parcel_id']][:8]:
                print('     %-6d T: %-35s R: %s' % (k, lt[k]['parcel_id'][:35], lr[k]['parcel_id'][:35]))


if __name__ == '__main__':
    main(sys.argv[1:])
