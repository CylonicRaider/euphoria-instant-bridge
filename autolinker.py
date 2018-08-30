
# -*- coding: ascii -*-

"""
A partial Python reimplementation of Autolinker.js as used by euphoria.io.
"""

import re

# Pipeline-separated list of all top-level domain names recognized by the
# autolinker.
TLDS = ('international|construction|contractors|enterprises|photography|produ'
    'ctions|foundation|immobilien|industries|management|properties|technology'
    '|christmas|community|directory|education|equipment|institute|marketing|s'
    'olutions|vacations|bargains|boutique|builders|catering|cleaning|clothing'
    '|computer|democrat|diamonds|graphics|holdings|lighting|partners|plumbing'
    '|supplies|training|ventures|academy|careers|company|cruises|domains|expo'
    'sed|flights|florist|gallery|guitars|holiday|kitchen|neustar|okinawa|reci'
    'pes|rentals|reviews|shiksha|singles|support|systems|agency|berlin|camera'
    '|center|coffee|condos|dating|estate|events|expert|futbol|kaufen|luxury|m'
    'aison|monash|museum|nagoya|photos|repair|report|social|supply|tattoo|tie'
    'nda|travel|viajes|villas|vision|voting|voyage|actor|build|cards|cheap|co'
    'des|dance|email|glass|house|mango|ninja|parts|photo|shoes|solar|today|to'
    'kyo|tools|watch|works|aero|arpa|asia|best|bike|blue|buzz|camp|club|cool|'
    'coop|farm|fish|gift|guru|info|jobs|kiwi|kred|land|limo|link|menu|mobi|mo'
    'da|name|pics|pink|post|qpon|rich|ruhr|sexy|tips|vote|voto|wang|wien|wiki'
    '|zone|bar|bid|biz|cab|cat|ceo|com|edu|gov|int|kim|mil|net|onl|org|pro|pu'
    'b|red|tel|uno|wed|xxx|xyz|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|a'
    'w|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|c'
    'd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cu|cv|cw|cx|cy|cz|de|dj|dk|dm|do|dz|ec|e'
    'e|eg|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|g'
    'q|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|j'
    'm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|l'
    'y|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|n'
    'c|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|p'
    'w|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sk|sl|sm|sn|so|sr|st|s'
    'u|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|u'
    'k|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|za|zm|zw')

# Behold the Regex of Terror, and despair.
CAPTURE_REGEX = re.compile('((?:[\\-;:&=\\+\\$,\\w\\.]+@)(+dom)\\.(+tld)\\b)|'
    '((?:((?:[A-Za-z][-.+A-Za-z0-9]+:(?![A-Za-z][-.+A-Za-z0-9]+://)(?!\\d)(?:'
    '//)?)(+dom))|(?:(?:(?<!\\w)//)?(?:www\\.)(+dom))|(?:(?:(?<!\\w)//)?(+dom'
    ')\\.(+tld)\\b))(?:[\\-A-Za-z0-9+&@#/%=~_()|\'$*\\[\\]?!:,.;]*[\\-A-Za-z0'
    '-9+&@#/%=~_()|\'$*\\[\\]])?)'
    .replace('(+tld)', '(?:' + TLDS + ')')
    .replace('(+dom)', '[A-Za-z0-9\\.\\-]*[A-Za-z0-9\\-]'))

# Regular expression for an invalid scheme.
INVALID_SCHEME = re.compile('^(java|cb)script:')

# Regular expression for a URL having a full scheme.
FULL_SCHEME = re.compile('^[A-Za-z][-.+A-Za-z0-9]+://')

# Regular expression matching a letter after the URL scheme.
LETTER_AFTER_SCHEME = re.compile('.*?:.*?[a-zA-Z]')

def autolink(source):
    """
    Yield (type, text) pairs denoting normal text or hyperlinks as detected
    from source.

    type may be either 'text' (for normal text) or 'email' (for an email
    address) or 'link' (for a hyperlink); text is the portion of source
    categorized by type. The results are in the order they occur in source.
    Note that multiple adjacent text segments might be returned.
    """
    def match_valid(url, scheme_url):
        # Disallow javascript: URL-s.
        if INVALID_SCHEME.match(scheme_url):
            return False
        # To pass, a URL must have a "proper" scheme or include a dot.
        if not (FULL_SCHEME.match(scheme_url) or '.' in url):
            return False
        # If the match has a scheme, there must be a letter after it.
        if scheme_url and not LETTER_AFTER_SCHEME.match(url):
            return False
        return True
    idx, end = 0, len(source)
    while idx < end:
        m = CAPTURE_REGEX.search(source, idx)
        if not m: break
        if m.start() != idx: yield ('text', source[idx:m.start()])
        idx = m.end()
        # Extract semantical parts for validity checking.
        email, url, scheme_url = m.groups('')
        # Check if the match is valid.
        if not match_valid(url, scheme_url):
            yield ('text', m.group())
            continue
        # Emails are done here.
        if email:
            yield ('email', email)
            continue
        # Strip an optional trailing parenthesis and yield result.
        if url.endswith(')') and url.count(')') > url.count('('):
            yield ('link', url[:-1])
            yield ('text', ')')
        else:
            yield ('link', url)
    # Yield the remainder of the source.
    if idx != end: yield ('text', source[idx:])
