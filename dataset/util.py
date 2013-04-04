#coding: utf-8
import re
from unicodedata import normalize as ucnorm, category

SLUG_REMOVE = re.compile(r'[,\s\.\(\)/\\;:]*')

class DatasetException(Exception):
    pass

class FreezeException(DatasetException):
    pass


def normalize(text):
    """ Simplify a piece of text to generate a more canonical 
    representation. This involves lowercasing, stripping trailing
    spaces, removing symbols, diacritical marks (umlauts) and 
    converting all newlines etc. to single spaces.
    """
    if not isinstance(text, unicode):
        text = unicode(text)
    text = text.lower()
    decomposed = ucnorm('NFKD', text)
    filtered = []
    for char in decomposed:
        cat = category(char)
        if cat.startswith('C'):
            filtered.append(' ')
        elif cat.startswith('M'):
            # marks, such as umlauts
            continue
        elif cat.startswith('Z'):
            # newlines, non-breaking etc.
            filtered.append(' ')
        elif cat.startswith('S'):
            # symbols, such as currency
            continue
        else:
            filtered.append(char)
    text = u''.join(filtered)
    while '  ' in text:
        text = text.replace('  ', ' ')
    text = text.strip()
    return ucnorm('NFKC', text)

def slug(text):
    """ Create a version of a string convenient for use in a URL
    or file name. """
    text = normalize(text)
    text = text.replace(u'ß', 'ss')
    text = '-'.join(filter(lambda t: len(t), \
        SLUG_REMOVE.split(text)))
    return text.lower()

