import regex
import html
from bs4 import BeautifulSoup,NavigableString
import logging

import warnings
warnings.filterwarnings("ignore", message='.*looks like a URL.*', category=UserWarning, module='bs4')

def clean_text(txt: str) -> str:
    # html formatting
    txt = txt.replace('<span class="ndash">&ndash;</span>','-')
    txt = regex.sub(r'<div[^>]*><div[^>]*></div></div>','\n',txt)
    txt = regex.sub(r'<div[^>]*></div>','\n',txt)
    txt = txt.replace('<div class="quotes">&nbsp;</div>','')
    txt = txt.replace('<p class="imgplaceholder left">&nbsp;</p>','')
    txt = txt.replace('<p class="imgplaceholder left">&nbps;</p>','') # sic
    txt = txt.replace('<p class="imgplaceholder center">&nbsp;</p>','')
    txt = txt.replace('<p class="imgplaceholder right">&nbsp;</p>','')
    txt = txt.replace('<span class="pi_BlackSquare">&nbsp;</span>',' * ')
    txt = regex.sub(r'<p class="videoplaceholder"[^>]*>&nbsp;?</p>','',txt)
    txt = txt.replace('<li>',' * ').replace('</li>','')
    txt = txt.replace('<h2[^>]*>','\n').replace('</h2>','\n\n')
    txt = txt.replace('</p>','\n\n').replace('<p[^>]*>','')
    txt = txt.replace('<br />','\n')
    txt = regex.sub(r'<iframe[^>]*></iframe ?>','',txt)

    # markdown
    # markdown headings
    txt = regex.sub(r"^#+ ", "", txt, flags=regex.MULTILINE)
    while True:
        txt2 = regex.sub(r"_+(.+?)_+", r"\1", txt)  # markdown emphases
        txt2 = regex.sub(r"\*+(.+?)\*+", r"\1", txt2)  # markdown emphasess
        if txt == txt2:
            break
        txt = txt2
    txt = regex.sub(r"\[([^\]]+?)\]\([^\)]+?\)", r"\1", txt)  # markdown links

    # normalization
    txt = regex.sub(r"^&bull; ", " * ", txt, flags=regex.MULTILINE)
    txt = txt.replace("&nbsp;", " ") \
        .replace("&#160;", " ") \
        .replace("&nbps;", "") \
        .replace("&39;", "'")
    txt = txt.replace("\xad", "")
    txt = txt.replace("\x95", "-")
    txt = txt.replace("\x96", "-")
    txt = txt.replace("\x94", '"')
    txt = txt.replace("\u2028","\n") # weird unicode line break
    txt = txt.replace("–", "-")  # ndash
    txt = txt.replace("—", "-")  # mdash
    txt = txt.replace("\-", "-")
    txt = txt.replace("•", "*")  # bull
    txt = txt.replace("…", "...")  # hellip
    txt = txt.replace("“", "\"")  # ldquo
    txt = txt.replace("”", "\"")
    txt = txt.replace("’", "'")  # rsquo
    txt = regex.sub(r"\p{Zs}", " ", txt)  # weird spaces
    try: 
        s = BeautifulSoup(txt,'lxml')
        for bq in s.find_all('blockquote'):
            bq.insert_before(NavigableString('- '))
            for text in bq.find_all(string=True):
                text.replace_with(text.replace('\n',' '))
            bq.unwrap()
        txt = s.get_text() # remove all other HTML
    except:
        logging.warn("BeautifulSoup parsing failed.")
    txt = html.unescape(txt)

    # missing quote separators
    txt = regex.sub(r"^-([^ ]+)", r"- \1", txt, flags=regex.MULTILINE)
    # too many quote separators
    txt = regex.sub(r"^-  +", "- ", txt, flags=regex.MULTILINE)
    # multiple quote markers
    txt = regex.sub(r"^(- *-)+","- ", txt, flags=regex.MULTILINE)
    # remove superfluous paragraph breaks
    txt = regex.sub(r"\n\n\n+","\n\n",txt)
    txt = regex.sub(r"\n*$","",txt)
    return(txt)
