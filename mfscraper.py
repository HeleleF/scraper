import logging
import random
import re
import sys
from time import sleep

import requests

logger = logging.getLogger(__name__)

SECRETS = []
SECRET_TEXT = 'Sie haben einen Geist gefunden'
USER_AGENT = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:41.0) Gecko/20100101 Firefox/41.0'


def check_article_for_secret(s: requests.Session, pr_link: str, ref_link: str):
    '''
    Checks an article for a secret and stores the article number if it exists
    '''

    try:
        r = s.get(
            url=pr_link,
            headers={ 'Referer': ref_link }
        )

    except requests.exceptions.RequestException:
        logger.error('[-] Request failed, terminating...', exc_info=True)
        sys.exit(1)

    data = r.text

    if data.find(SECRET_TEXT) != -1:
        logger.info(f'[+] Found secret at {pr_link}')
        g = re.findall(r'<span class="products-model">(\d*)<', data)
        SECRETS.append(g[0])


def get_articles(s: requests.Session, mf_link: str):
    '''
    Finds all products and checks each for a secret
    '''

    try:
        r = s.get(
            url=mf_link,
            headers={ 'Referer': 'https://www.mindfactory.de/' }
        )

    except requests.exceptions.RequestException:
        logger.error('[-] Request failed, terminating...', exc_info=True)
        sys.exit(1)

    articles = re.findall(r'"(.*)" class="p-complete-link visible-xs visible-sm', r.text)

    for article_link in articles:
        check_article_for_secret(s, article_link, mf_link)


def main():
    '''
    finds hidden secrets on mindfactory
    '''

    all_links = ['<CHANGE_THIS>']

    sess = requests.Session()

    # set user cookies
    sess.cookies.set('NSid', '<CHANGE_THIS>', domain='.mindfactory.de', path='/')
    sess.cookies.set('lz_userid', '<CHANGE_THIS>', domain='chat.mindfactory.de', path='/livezilla')
    sess.cookies.set('cookies_accepted', 'true')

    # set user agent
    sess.headers.update({'User-Agent': USER_AGENT})

    for link in all_links:

        if not link.endswith('/article_per_page/5'):
            link = f'{link}/article_per_page/5'

        get_articles(sess, link)
        logger.debug('Waiting for next link...')
        sleep(3)

    if SECRETS:
        logger.info(f'[+] article numbers: {",".join(SECRETS)}')
    else:
        logger.warn('[-] No secrets found?')

    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='spam.log',
                        filemode='a'
                        )
    c = logging.StreamHandler()
    c.setLevel(logging.INFO)
    f = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    c.setFormatter(f)
    logger.addHandler(c)
    sys.exit(int(main() or 0))
