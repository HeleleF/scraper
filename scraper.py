#!/usr/bin/env python3
import json
import logging
import sys
from itertools import groupby
from random import randint
from time import sleep
from typing import Dict, Iterator, List

import requests
from bs4 import BeautifulSoup, PageElement

assert sys.version_info >= (3, 6), 'Install Python 3.6 or higher'


log = logging.getLogger('philly')
log.setLevel(logging.DEBUG)

fh = logging.FileHandler('./philly.log', 'w', 'utf-8')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

log.addHandler(fh)
log.addHandler(ch)

class PhillyScraper():
    '''
    Scrapes all available concerts and films from "The Digital Concert Hall" and
    writes the results as json files.

    This was possible in March 2020 during the corona outbreak when every concert and film was accessible for free.
    '''

    MIN_DELAY = 2
    MAX_DELAY = 5

    def __init__(self, user_token: str, concert_ids_path: str = None, film_ids_path: str = None):

        self.__data = []

        self.__concert_id_list = []

        if concert_ids_path:
            with open(concert_ids_path, 'r') as infile:
                self.__concert_id_list = infile.read().splitlines()

        self.__film_id_list = []

        if film_ids_path:
            with open(film_ids_path, 'r') as infile:
                self.__film_id_list = infile.read().splitlines()


        self.__sess = requests.Session()
        self.__sess.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15'
        })
        self.__sess.cookies.update(dict(hasBeenLoggedIn='1', dch_user_token=user_token))

    def __write_output(self, out_name: str = 'all'):
        '''
        Helper to create the final json file
        '''
        with open(f'{out_name}.json', 'w') as out:
            json.dump(self.__data, out)

    def __get_seasons(self) -> List[Dict]:
        '''
        Returns a list of dicts containing information about each season
        '''

        log.debug('Getting seasons...')

        try:
            r = self.__sess.get('https://www.digitalconcerthall.com/json_cacheable_services/get_seasons?language=en')
            r.raise_for_status()
        except requests.HTTPError as httpe:
            log.error(f'Get seasons failed with http error: {httpe}')
            sys.exit(-1)
        except requests.exceptions.ConnectionError as cerr:
            log.error(f'Get seasons failed with network problems: {cerr}')
            sys.exit(-1)
        except requests.exceptions.Timeout:
            log.error('Get seasons timed out!')
            sys.exit(-1)
        except requests.exceptions.RequestException as err:
            log.error(f'Get seasons failed with request error: {err}')
            sys.exit(-1)

        try:
            seasons_dict = r.json()
        except ValueError:
            log.error(f'Get seasons returned non-json data: {r.text}')
            sys.exit(-1)

        if len(seasons_dict['items']) != seasons_dict['count']:
            log.warning(f'API returned a season count of {seasons_dict["count"]}, but {len(seasons_dict["items"])} were found!')

        return seasons_dict['items']

    def __extract_text(self, page_element: PageElement) -> str:
        '''
        Helper to extract the text content from a bs4 page element.
        Whitespace between words is trimmed
        '''
        try: 
            return ' '.join(page_element.text.split())
        except AttributeError: 
            return ' '.join(page_element.split())

    def __make_dict(self, groups: Iterator) -> Dict[str, str]:
        '''
        Helper to create a dict from a itertoolsgroup
        '''

        ret = dict(role='KEINE ROLLE', player='KEINER')

        for tag in groups:
            if tag.name == 'strong':
                ret['player'] = tag.text.strip()
            elif tag.name == 'em':
                ret['role'] = tag.text.strip()
        return ret

    def __extract_metadata(self, concert_id: str, soup: BeautifulSoup) -> Dict:
        '''
        Extracts all available metadata for a concert and returns it as a dict
        '''

        log.debug(f'Extracting metadata for concert with ID {concert_id}...')

        streams = self.__get_streams(concert_id)

        metaDict = dict(concertId=concert_id)

        concertTitleTag = soup.select_one('h1[itemprop="name"]')
        if concertTitleTag:
            metaDict['concertTitle'] = concertTitleTag.text.replace(u'\u2019', "'").strip()

        concertProgrammeTag = soup.select_one('div[itemprop="description"]')
        if concertProgrammeTag:
            metaDict['concertProgramme'] = concertProgrammeTag.text.replace(u'\u2019', "'").strip()

        programmeGuideTag = soup.select_one('div#tabs-1')
        if programmeGuideTag:
            metaDict['concertProgrammeGuide'] = programmeGuideTag.text.replace(u'\u2019', "'").strip()

        concertMetaTag = soup.select_one('p.concertMeta')
        metaElms = concertMetaTag.contents
        metaDict['concertDate'] = metaElms[0].replace(u'\u2013', '-').strip()

        if len(metaElms) == 3:
            metaDict['concertMeta'] = ' '.join(metaElms[2].split()).replace(u'\u2019', "'")

        mainArtistTag = soup.select_one('p.mainArtist')      
        mainElms = mainArtistTag.contents

        try:
            metaDict['mainArtist'] = mainElms[0].strip()
        except TypeError:
            metaDict['mainArtist'] = mainElms[0].text.strip()
        except IndexError:
            pass

        if len(mainElms) == 3:
            metaDict['conductor'] = ' '.join(mainElms[2].text.split())

        starArtists = soup.select('p.starArtist span[itemprop="name"]')
        if len(starArtists):
            metaDict['starArtists'] = [' '.join(spanTag.text.split()) for spanTag in starArtists]

        supportTag = soup.select_one('div#concert-support')
        if supportTag:
            metaDict['support'] = supportTag.text.strip()

        metaDict['pieces'] = []
        for piece in soup.select('ul.list-lines > li'):

            concert_piece_id = piece.select_one('div.jsConcertWork')['id']

            pieceDict = dict(pieceId=concert_piece_id)

            if concert_piece_id in streams:
                pieceDict['streamUrl'] = streams[concert_piece_id]
            else:
                log.warning(f'No stream url found for concert piece with ID {concert_piece_id}')
                pieceDict['streamUrl'] = 'not-found'

            headers = piece.find('h2').contents
            for idx, tag in enumerate(headers):

                if tag.name == 'strong':
                    pieceDict['composer'] = tag.text.strip()
                elif tag.name == 'br':
                    pieceDict['description'] = ''.join(map(self.__extract_text, headers[idx + 1:])).strip()
                    break

            if 'composer' not in pieceDict and 'description' not in pieceDict:
                pieceDict['description'] = ''.join(map(self.__extract_text, headers)).strip()

            artists = piece.find('p')
            if not artists:
                metaDict['pieces'].append(pieceDict)
                continue

            artistList = [ self.__make_dict(g[1]) for g in groupby(artists.contents, key=lambda x: str(x).strip() != ',') if g[0] ]

            if len(artistList) != 1 or artistList[0]['role'] != 'KEINE ROLLE' or artistList[0]['player'] != 'KEINER':

                temp = dict()

                for d in artistList:

                    role = d['role']

                    if role in temp:
                        temp[role].append(d['player'])
                    else:
                        temp[role] = [d['player']]

                pieceArtists = [ dict(role=k, names=v) for k,v in temp.items() ]

                if len(pieceArtists):
                    pieceDict['artists'] = pieceArtists

            metaDict['pieces'].append(pieceDict)

        return metaDict

    def __get_streams(self, content_id: str) -> Dict[str, str]:
        '''
        Returns all available stream links for a content id as a dict
        '''

        log.debug(f'Getting streams for content with ID {content_id}...')

        try:
            r = self.__sess.get(f'https://www.digitalconcerthall.com/json_services/get_stream_urls?id={content_id}&language=en')
            r.raise_for_status()
        except requests.HTTPError as httpe:
            log.error(f'Get streams for content with ID {content_id} failed with http error: {httpe}')
            sys.exit(-1)
        except requests.exceptions.ConnectionError as cerr:
            log.error(f'Get streams for content with ID {content_id} failed with network problems: {cerr}')
            sys.exit(-1)
        except requests.exceptions.Timeout:
            log.error(f'Get streams for content with ID {content_id} timed out!')
            sys.exit(-1)
        except requests.exceptions.RequestException as err:
            log.error(f'Get streams for content with ID {content_id} failed with request error: {err}')
            sys.exit(-1)

        try:
            urls_dict = r.json()
        except ValueError:
            log.error(f'Get streams for content with ID {content_id} returned non-json data: {r.text}')
            sys.exit(-1)

        if not urls_dict['success']:
            log.error(f'Get streams failed with message: {urls_dict["message"]}')
            sys.exit(-1)

        manifest_dict = { k:v[0]['url'] for k, v in urls_dict['urls'].items() }

        log.debug(f'Extracted {len(manifest_dict)} streams for content with ID {content_id}')

        return manifest_dict  

    def __handle_concert(self, concert_id: str) -> Dict:

        if concert_id not in self.__concert_id_list:

            log.debug(f'Scraping concert with ID {concert_id}...')

            try:
                r = self.__sess.get(f'https://www.digitalconcerthall.com/en/concert/{concert_id}')
                r.raise_for_status()
            except requests.HTTPError as httpe:
                log.error(f'Get concert with ID {concert_id} failed with http error: {httpe}')
                sys.exit(-1)
            except requests.exceptions.ConnectionError as cerr:
                log.error(f'Get concert with ID {concert_id} failed with network problems: {cerr}')
                sys.exit(-1)
            except requests.exceptions.Timeout:
                log.error(f'Get concert with ID {concert_id} timed out!')
                sys.exit(-1)
            except requests.exceptions.RequestException as err:
                log.error(f'Get concert with ID {concert_id} failed with request error: {err}')
                sys.exit(-1)

            soup = BeautifulSoup(r.content, 'lxml')

            return self.__extract_metadata(concert_id, soup)

        else:
            log.debug(f'Skipping concert with ID {concert_id} because it already exists')
            return None

    def __handle_season(self, season: Dict):

        season_id = season['id']
        log.debug(f'Scraping season {season["label"]} with ID {season_id}...')

        try:
            r = self.__sess.get(f'https://www.digitalconcerthall.com/en/concerts/season_{season_id}')
            r.raise_for_status()
        except requests.HTTPError as httpe:
            log.error(f'Get season with ID {season_id} failed with http error: {httpe}')
            sys.exit(-1)
        except requests.exceptions.ConnectionError as cerr:
            log.error(f'Get season with ID {season_id} failed with network problems: {cerr}')
            sys.exit(-1)
        except requests.exceptions.Timeout:
            log.error(f'Get season with ID {season_id} timed out!')
            sys.exit(-1)
        except requests.exceptions.RequestException as err:
            log.error(f'Get season with ID {season_id} failed with request error: {err}')
            sys.exit(-1)

        soup = BeautifulSoup(r.content, 'lxml')
        concerts = soup.select('li.archive')

        season_dict = dict(seasonId=season_id, season=season["label"].replace(u'\u2013', '-'), concerts=[])

        for concert in concerts:
            concert_dict = self.__handle_concert(concert['id'][8:])

            # if none, concert already existed
            if concert_dict:
                season_dict['concerts'].append(concert_dict)

            sleep(randint(self.MIN_DELAY, self.MAX_DELAY))

        self.__data.append(season_dict)

    def scrape_seasons(self):
        '''
        Scrapes all concerts for all seasons
        '''
        self.__data = []

        all_seasons = self.__get_seasons()

        for season in all_seasons:
            self.__handle_season(season)
            sleep(randint(self.MIN_DELAY, self.MAX_DELAY))

        log.info('Writing to file...')
        self.__write_output('seasons')
        log.info('Done')

    def __extract_film_data(self, tag: PageElement) -> Dict[str, str]:

        link = tag.select_one('a')
        film_id = link['href'].split('/')[-1]

        return dict(film_id=film_id, title=link['title'])

    def __get_films(self) -> Iterator[Dict]:
        '''
        Returns a list of dicts containing information about each film
        '''

        log.debug('Getting films...')

        try:
            r = self.__sess.get('https://www.digitalconcerthall.com/en/films')
            r.raise_for_status()
        except requests.HTTPError as httpe:
            log.error(f'Get films failed with http error: {httpe}')
            sys.exit(-1)
        except requests.exceptions.ConnectionError as cerr:
            log.error(f'Get films failed with network problems: {cerr}')
            sys.exit(-1)
        except requests.exceptions.Timeout:
            log.error('Get films timed out!')
            sys.exit(-1)
        except requests.exceptions.RequestException as err:
            log.error(f'Get films failed with request error: {err}')
            sys.exit(-1)

        soup = BeautifulSoup(r.content, 'lxml')
        films = soup.select('li.item')

        log.debug(f'Found {len(films)} films')

        return map(self.__extract_film_data, films)

    def __handle_film(self, film_dict: Dict[str, str]):

        film_id = film_dict['film_id']

        if film_id not in self.__film_id_list:

            log.debug(f'Scraping film with ID {film_id}...')

            try:
                r = self.__sess.get(f'https://www.digitalconcerthall.com/en/film/{film_id}')
                r.raise_for_status()
            except requests.HTTPError as httpe:
                log.error(f'Get film with ID {film_id} failed with http error: {httpe}')
                sys.exit(-1)
            except requests.exceptions.ConnectionError as cerr:
                log.error(f'Get film with ID {film_id} failed with network problems: {cerr}')
                sys.exit(-1)
            except requests.exceptions.Timeout:
                log.error(f'Get film with ID {film_id} timed out!')
                sys.exit(-1)
            except requests.exceptions.RequestException as err:
                log.error(f'Get film with ID {film_id} failed with request error: {err}')
                sys.exit(-1)

            soup = BeautifulSoup(r.content, 'lxml')

            streams = self.__get_streams(film_id)

            if film_id in streams:
                film_dict['streamUrl'] = streams[film_id]
            else:
                log.warning(f'No stream url found for film with ID {film_id}')
                film_dict['streamUrl'] = 'not-found'

            subTitleTag = soup.select_one('div.margin-15 p')
            if subTitleTag:
                film_dict['subtitle'] = subTitleTag.text.strip()

            actorsTag = soup.select('div.box-50 strong')
            if len(actorsTag):
                film_dict['actors'] = [actor.text.strip() for actor in actorsTag]

            descTag = soup.select_one('div#tabs-0')
            if descTag:
                film_dict['description'] = descTag.text.strip()

            creditsTag = soup.select_one('div#tabs-2')
            if creditsTag:
                film_dict['credits'] = creditsTag.text.strip()

            self.__data.append(film_dict)

        else:
            log.debug(f'Skipping film with ID {film_id} because it already exists')     

    def scrape_films(self):
        '''
        Scrapes all films
        '''

        self.__data = []

        all_films = self.__get_films()

        for film in all_films:
            self.__handle_film(film)
            sleep(randint(self.MIN_DELAY, self.MAX_DELAY))

        log.info('Writing to file...')
        self.__write_output('films')
        log.info('Done')

if __name__ == '__main__':
    ps = PhillyScraper('<USER_TOKEN>')
    ps.scrape_films()
