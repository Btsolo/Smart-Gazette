"""
Template-based article generation.

The counterpart to the extraction templates: given structured fields, render
the same output shape the AI generation step produces, with no API call.

Output keys match generateNarrativeContent():
    title, summary, article, xSummary, actionableInfo, significance

Design notes
------------
* Prose is assembled from sentence fragments chosen by which fields are
  present, so a notice missing (say) place of death reads naturally rather
  than leaving a gap or an empty clause.
* Returns None if the fields are too sparse to write a faithful article -
  the caller then falls back to the AI path, exactly as with extraction.
* Nothing is invented. Every clause traces to an extracted field.
"""
import re

# --- helpers ---------------------------------------------------------------

def _join(names):
    """['A','B','C'] -> 'A, B and C'"""
    names = [n for n in (names or []) if n]
    if not names:  return None
    if len(names) == 1: return names[0]
    return '%s and %s' % (', '.join(names[:-1]), names[-1])

SMALL = {'of', 'at', 'the', 'in', 'and'}

def _titlecase_court(c):
    """'HIGH COURT OF KENYA AT NAIROBI' -> 'High Court of Kenya at Nairobi'"""
    if not c: return None
    out = []
    for i, w in enumerate(c.split()):
        lw = w.lower()
        out.append(lw if (i and lw in SMALL) else w.capitalize())
    return ' '.join(out).replace("'S", "'s")

def _smart_title(s):
    """Title case that leaves small words alone: 'Letters of Administration'."""
    out = []
    for i, w in enumerate(s.split()):
        lw = w.lower()
        out.append(lw if (i and lw in SMALL) else (w if w[:1].isupper() else w.capitalize()))
    return ' '.join(out)

def _clip(s, n):
    s = re.sub(r'\s+', ' ', s).strip()
    return s if len(s) <= n else s[:n - 1].rsplit(' ', 1)[0] + '…'

def _dehyphen(s):
    return re.sub(r'\s*-\s*', '-', s) if s else s

# Kenya's 47 counties, used to normalise the many spellings the extractor
# produces ("TransNzoia", "Trans Nzoia", "Trans-Nzoia" -> "Trans Nzoia").
COUNTIES = ["Baringo","Bomet","Bungoma","Busia","Elgeyo Marakwet","Embu","Garissa","Homa Bay",
 "Isiolo","Kajiado","Kakamega","Kericho","Kiambu","Kilifi","Kirinyaga","Kisii","Kisumu","Kitui",
 "Kwale","Laikipia","Lamu","Machakos","Makueni","Mandera","Marsabit","Meru","Migori","Mombasa",
 "Murang'a","Nairobi","Nakuru","Nandi","Narok","Nyamira","Nyandarua","Nyeri","Samburu","Siaya",
 "Taita Taveta","Tana River","Tharaka Nithi","Trans Nzoia","Turkana","Uasin Gishu","Vihiga",
 "Wajir","West Pokot"]
_CANON = {re.sub(r"[^a-z]", "", c.lower()): c for c in COUNTIES}

# Sub-counties and towns that appear in the "situate in" clause instead of
# the county. Extend as new ones show up in the failure log.
SUB_COUNTY = {
    'thika': 'Kiambu', 'ruiru': 'Kiambu', 'gatundu': 'Kiambu', 'limuru': 'Kiambu',
    'naivasha': 'Nakuru', 'molo': 'Nakuru', 'gilgil': 'Nakuru',
    'nyando': 'Kisumu', 'muhoroni': 'Kisumu',
    'rachuonyo': 'Homa Bay', 'ndhiwa': 'Homa Bay',
    'koibatek': 'Baringo', 'eldama ravine': 'Baringo',
    'sabatia': 'Vihiga', 'matungu': 'Kakamega', 'mumias': 'Kakamega',
    'githunguri': 'Kiambu', 'kikuyu': 'Kiambu', 'juja': 'Kiambu',
}
JUNK = {'the', 'sub', 'district', 'area', 'county', 'republic', 'kenya', ''}

def normalise_county(raw):
    """Map a messy location string to a canonical county name.

    Returns None when the string is not a place at all ("the", "sub -"),
    so callers can leave the field empty rather than storing noise.
    """
    if not raw: return None
    cleaned = re.sub(r"\s*-\s*$", "", re.sub(r"\s+", " ", raw)).strip()
    key = re.sub(r"[^a-z ]", "", cleaned.lower()).strip()
    if key in JUNK or len(key) < 3:
        return None
    flat = key.replace(" ", "")
    if flat in _CANON:
        return _CANON[flat]
    # sub-county / town names that stand in for the county
    for sc, county in SUB_COUNTY.items():
        if re.search(r"\b%s\b" % re.escape(sc), key):
            return county
    # "Nakuru Municipality" - a county name with a qualifier attached
    for k, v in _CANON.items():
        if k and k in flat and len(k) >= 5:
            return v
    return cleaned or None

def _plural(phrase):
    """'certificate of title' -> 'certificates of title' (head noun, not tail)."""
    if not phrase: return phrase
    parts = phrase.split()
    head = 0
    for i, w in enumerate(parts):
        if w.lower() in ('of', 'in', 'for'):
            break
        head = i
    parts[head] = parts[head] + ('es' if parts[head].endswith(('s', 'x', 'ch')) else 's')
    return ' '.join(parts)

def _person(name):
    """'M. O. OLIECH' -> 'M. O. Oliech'"""
    if not name: return name
    return ' '.join(w if (len(w) <= 2 and w.endswith('.')) else w.capitalize()
                    for w in name.split())

def _sentence(s):
    s = re.sub(r'\s+', ' ', s).strip()
    if s and s[-1] not in '.!?': s += '.'
    return s

# --- Court_Legal (probate) -------------------------------------------------

def court_legal(f):
    dec  = f.get('deceased_name')
    pets = _join(f.get('petitioner_names'))
    if not (dec and pets):
        return None

    court   = _titlecase_court(f.get('court_name'))
    action  = (f.get('action_type') or 'grant').lower()
    case    = f.get('case_reference')
    rel     = f.get('petitioner_relationship')
    died_on = f.get('date_of_death')
    place   = f.get('place_of_death')
    res     = f.get('deceased_residence')
    deadline= f.get('filing_deadline')
    firm    = f.get('advocate_firm')

    short = ('probate' if 'probate' in action else
             'resealing of a grant' if 'reseal' in action else
             'letters of administration')

    title = _clip('Application for %s in the estate of %s' % (_smart_title(short), dec), 90)

    summary = _sentence('%s %s applied for %s in the estate of the late %s'
                        % (pets, 'has' if (f.get('petitioner_names') or []).__len__() == 1 else 'have',
                           short, dec))

    # paragraph 1 - what the notice says
    p1 = '%s has received an application for a %s to the estate of %s' % (
         court or 'The court', action, dec)
    if case: p1 += ', filed under cause %s' % case
    p1 = _sentence(p1)
    bits = []
    if res:   bits.append('The deceased was late of %s' % res)
    if died_on:
        d = 'and died on %s' % died_on if bits else 'The deceased died on %s' % died_on
        if place: d += ' at %s' % place
        bits.append(d)
    elif place:
        bits.append('The deceased died at %s' % place)
    if bits: p1 += ' ' + _sentence(' '.join(bits))

    # paragraph 2 - who is affected
    p2 = 'The application was brought by %s' % pets
    if rel:  p2 += ', described as the %s of the deceased' % _dehyphen(rel)
    if firm: p2 += ', through %s, advocates' % firm
    p2 = _sentence(p2)
    p2 += (' A grant of this kind authorises the named applicants to administer '
           'the estate, so anyone with a competing claim or an interest in the '
           'estate is directly affected.')

    # paragraph 3 - what to do
    if deadline:
        p3 = _sentence('The court will issue the grant unless an objection is entered within %s' % deadline)
    else:
        p3 = ('The court will issue the grant unless cause is shown to the contrary '
              'and an appearance is entered within the period stated in the notice.')
    p3 += ' Objections are filed with the court named above, citing the cause number.'

    article = '\n\n'.join([p1, p2, p3])

    x = _clip('%s applied for %s in the estate of the late %s. Objections go to %s.'
              % (pets, short, dec, court or 'the court'), 240)

    action_info = (_sentence('Objections must be entered within %s' % deadline) if deadline
                   else 'Objections must be entered with the court within the period stated in the notice.')

    return {'title': title, 'summary': _clip(summary, 200), 'article': article,
            'xSummary': x, 'actionableInfo': action_info, 'significance': 3}

# --- Land_Property ---------------------------------------------------------

def land_property(f):
    parties = _join(f.get('parties'))
    parcel  = f.get('parcel_id')
    if not (parties and parcel):
        return None

    doc    = f.get('document_type') or 'title document'
    county = normalise_county(f.get('county'))
    period = f.get('objection_period')
    reg    = f.get('registrar_name')
    subj   = (f.get('notice_subtype') or 'provisional certificate').lower()

    title = _clip('Replacement %s sought for %s' % (doc, parcel), 90)

    summary = _sentence('A replacement %s has been advertised for %s%s after the '
                        'original was reported lost'
                        % (doc, parcel, ' in %s' % county if county else ''))

    p1 = _sentence('The Land Registrar has given notice that the %s for %s, '
                   'registered in the name of %s, has been reported lost'
                   % (doc, parcel, parties))
    if county: p1 += ' ' + _sentence('The parcel is situated in %s' % county)

    p2 = _sentence('Sufficient evidence has been produced to satisfy the Registrar '
                   'that the original document is lost, and a %s is to be issued in '
                   'its place' % subj)
    p2 += (' Anyone holding the original document, or claiming an interest in the '
           'parcel, is affected and should come forward before the replacement issues.')

    if period:
        p3 = _sentence('A replacement will be issued after %s from the date of the notice, '
                       'provided no objection is received within that period' % period)
    else:
        p3 = ('A replacement will be issued after the period stated in the notice, '
              'provided no objection is received.')
    if reg: p3 += ' ' + _sentence('Objections should be addressed to %s' % _person(reg))

    article = '\n\n'.join([p1, p2, p3])
    x = _clip('Lost %s for %s (%s) - a replacement will issue unless someone objects%s.'
              % (doc, parcel, parties, ' within %s' % period if period else ''), 240)
    ai = (_sentence('Objections must reach the Land Registrar within %s' % period) if period
          else 'Objections must reach the Land Registrar within the period stated in the notice.')

    return {'title': title, 'summary': _clip(summary, 200), 'article': article,
            'xSummary': x, 'actionableInfo': ai, 'significance': 2}

# --- Corrigenda ------------------------------------------------------------

def corrigenda(f):
    ams = f.get('amendments') or []
    if not ams: return None
    ref   = f.get('amends_notice')
    cause = f.get('cause_reference')
    first = ams[0]

    title = _clip('Correction to Gazette Notice %s' % (ref or ''), 90) if ref else 'Correction to a published notice'
    summary = _sentence('A published notice has been corrected: %s now reads "%s"'
                        % (first.get('field') or 'an entry', first.get('to_read') or ''))

    p1 = _sentence('A correction has been published to Gazette Notice %s%s'
                   % (ref or '(reference not stated)', ' in cause %s' % cause if cause else ''))
    lines = ['the %s printed as "%s" now reads "%s"'
             % (a.get('field') or 'entry', a.get('printed_as') or '', a.get('to_read') or '')
             for a in ams]
    p2 = _sentence('The correction records that %s' % _join(lines))
    p3 = ('The corrected details replace what was previously published. Anyone '
          'relying on the earlier notice should work from the corrected version, '
          'and any deadline stated in the original notice is unchanged.')

    return {'title': title, 'summary': _clip(summary, 200),
            'article': '\n\n'.join([p1, p2, p3]),
            'xSummary': _clip(summary, 240),
            'actionableInfo': 'Use the corrected details in place of the originally published entry.',
            'significance': 2}

RENDERERS = {'court_legal': court_legal, 'land_property': land_property, 'Corrigenda': corrigenda}

def generate(category, fields):
    fn = RENDERERS.get(category)
    if not fn: return None
    return _drop_unused_tweet(fn(fields))


# --- grouped articles ------------------------------------------------------
#
# Land notices are the clearest case for grouping: an issue can carry a
# hundred near-identical lost-title notices, and a hundred near-identical
# micro-articles is worse journalism than one article with a list. The
# individual records are still stored per notice; this renders the digest
# that sits above them.

X_THRESHOLD = 8      # matches the IFTTT posting rule; below this no tweet is used

def _drop_unused_tweet(article):
    """xSummary is only consumed when significance clears the posting
    threshold, so leave it empty rather than carrying a field nothing reads."""
    if article and article.get('significance', 0) < X_THRESHOLD:
        article['xSummary'] = None
    return article

def group_land(records, issue_label=None):
    """One digest article over a batch of lost-document land notices."""
    recs = [r for r in records if r and r.get('parcel_id')]
    if len(recs) < 3:
        return None                      # not worth grouping

    counties = [normalise_county(r.get('county')) for r in recs]
    counties = [c for c in counties if c]
    spread   = sorted(set(counties))
    docs     = [r.get('document_type') for r in recs if r.get('document_type')]
    common   = max(set(docs), key=docs.count) if docs else 'title document'
    common_pl = _plural(common)
    periods  = [r.get('objection_period') for r in recs if r.get('objection_period')]
    period   = max(set(periods), key=periods.count) if periods else None

    n = len(recs)
    where = ('across %d counties' % len(spread) if len(spread) > 3
             else 'in %s' % _join(spread) if spread else '')
    title = _clip('%d replacement %s gazetted%s'
                  % (n, common_pl, (' ' + where) if where else ''), 90)

    summary = _sentence('%d parcels have had their %s reported lost%s, and '
                        'replacements will be issued unless objections are received'
                        % (n, common_pl, (' ' + where) if where else ''))

    p1 = _sentence('The Land Registrar has advertised %d notices of lost %s in this '
                   'issue%s' % (n, common_pl, (', ' + where) if where else ''))
    if spread and len(spread) <= 8:
        p1 += ' ' + _sentence('The parcels lie in %s' % _join(spread))

    p2 = ('Each notice follows the same course: the registered proprietor has '
          'reported the original document lost, the Registrar is satisfied by the '
          'evidence produced, and a replacement is to be issued. Anyone holding an '
          'original document for one of these parcels, or claiming an interest in '
          'it, is affected.')

    p3 = (_sentence('Objections must reach the Land Registrar within %s of the '
                    'respective notice' % period) if period
          else 'Objections must reach the Land Registrar within the period stated in each notice.')
    p3 += ' The individual notices below carry the parcel numbers and registered proprietors.'

    return {
        'title': title,
        'summary': _clip(summary, 200),
        'article': '\n\n'.join([p1, p2, p3]),
        'xSummary': None,
        'actionableInfo': (('Objections must reach the Land Registrar within %s.' % period)
                           if period else
                           'Objections must reach the Land Registrar within the stated period.'),
        'significance': 3,
        'grouped_count': n,
        'members': [{'parcel_id': r.get('parcel_id'),
                     'parties': r.get('parties'),
                     'county': normalise_county(r.get('county'))} for r in recs],
    }

GROUPERS = {'land_property': group_land}

def generate_group(category, records, issue_label=None):
    fn = GROUPERS.get(category)
    return fn(records, issue_label) if fn else None
