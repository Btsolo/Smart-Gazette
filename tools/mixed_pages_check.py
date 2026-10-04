"""
End-to-end check of page-level OCR (docs/specs/mixed-pages.md): a PDF whose
only page is an IMAGE of printed notice text (no text layer, like a scanned
page inside a born-digital issue) goes through the real path - extractor
(the page comes out empty) -> mixed_pages.fill with the lab OCR -> cleaner -
and the notice must come out readable.

Needs tesseract, poppler and Times New Roman (Windows fonts); prints a skip
line otherwise. Run by tools/audit.py.
"""
import os, sys, subprocess, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

NOTICE = ["GAZETTE NOTICE NO. 4321",
          "THE LAND REGISTRATION ACT",
          "ISSUE OF A NEW LAND TITLE DEED",
          "WHEREAS John Doe, of P.O. Box 5, Kisumu, is registered as",
          "proprietor of all that piece of land known as Kisumu/Karateng/1874,",
          "notice is given that after the expiration of sixty (60) days",
          "from the date hereof, I shall issue a new land title deed."]


def make_pdf(path):
    from PIL import Image, ImageDraw, ImageFont
    font = ImageFont.truetype('C:/Windows/Fonts/times.ttf', 34)
    img = Image.new('L', (2480, 3508), 255)
    d = ImageDraw.Draw(img)
    for i, line in enumerate(NOTICE):
        d.text((250, 400 + i * 60), line, fill=0, font=font)
    img.save(path, 'PDF', resolution=300)


def main():
    import mixed_pages as MP
    import vocab_repair as VR, gazette_clean as G
    tmp = tempfile.mkdtemp()
    pdf = os.path.join(tmp, 'image_page.pdf')
    try:
        make_pdf(pdf)
    except OSError as e:
        print('skip\tpage OCR: image page with text is read and inserted\t%s' % e)
        return
    raw = subprocess.run(['node', os.path.join(HERE, 'inspect_positions.js'), pdf], capture_output=True).stdout.decode('utf-8')
    V = VR.load_corpus_vocab()
    import ocr_check as OC, scan_tables as ST, scan_lane as SL

    def ocr(page):
        OC.ocr_page(pdf, page, tmp)
        R = ST.image_rules(ST.render(pdf, page), 2, 150)
        return SL.ocr_fix(SL.fix_headers(ST.page_text(ST.read_words(os.path.join(tmp, 'p%04d.tsv.gz' % page)), R)), V)
    text, done = MP.fill(raw, ocr)
    clean = G.apply_ascending_lock(VR.repair(G.clean(text), V))
    ok = MP.textless_pages(raw) == [1] and done == [1] and 'GAZETTE NOTICE NO. 4321' in clean and 'John Doe' in clean \
        and 'Kisumu/Karateng/1874' in clean
    print('%s\tpage OCR: image page with text is read and inserted\t%s' % ('ok' if ok else 'ERROR', '' if ok else repr(clean[:300])))


if __name__ == '__main__':
    main()
