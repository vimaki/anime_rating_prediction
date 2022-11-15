import csv
import logging
import os.path
import re
import sys

import click
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from retry import retry

ANIME_LIST_URL = 'https://myanimelist.net/topanime.php?limit='
DATA_FILE = '../../data/raw/anime_data.csv'

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger()

retry_exceptions = (requests.ConnectionError,
                    requests.Timeout)


class AnimeParser:

    def __init__(self, out_file: str | None = None, redo: bool = False, progress_bar: bool = False) -> None:
        self.start_url = ANIME_LIST_URL

        if out_file is None:
            self.data_file = DATA_FILE
        else:
            self.data_file = out_file

        self.redo = redo

        if not os.path.isfile(self.data_file) or self.redo:
            with open(self.data_file, 'w') as data_file:
                writer = csv.writer(data_file)
                writer.writerow(
                    (
                        'title',
                        'english_title',
                        'type_of_anime',
                        'episodes',
                        'status',
                        'aired',
                        'premiered',
                        'broadcast',
                        'producers',
                        'licensors',
                        'studios',
                        'source',
                        'genres',
                        'themes',
                        'demographics',
                        'duration',
                        'adult_rating',
                        'synopsis',
                        'rating',
                    )
                )

        self.progress_bar = progress_bar

    @staticmethod
    def _search_exception_handling(search_func):
        def exception_wrapper(*args, **kwargs):
            try:
                return search_func(*args, **kwargs)
            except AttributeError:
                return 'NaN'
        return exception_wrapper

    @staticmethod
    def _feature_formatting(search_func):
        def formatting_wrapper(*args, **kwargs):
            feature_search_result = search_func(*args, **kwargs)
            if feature_search_result is None:
                return 'NaN'
            if isinstance(feature_search_result, list):
                feature_values = []
                for value in feature_search_result:
                    feature_values.append(value.text.strip())
                feature_value = ', '.join(sorted(set(feature_values)))
                if feature_value == 'add some':
                    return 'NaN'
                return feature_value
            return feature_search_result
        return formatting_wrapper

    @staticmethod
    @_feature_formatting
    @_search_exception_handling
    def _info_from_sibling(soup: BeautifulSoup, tag_text: str) -> str:
        return soup.find('span', text=tag_text).next_sibling.text.strip()

    @staticmethod
    @_feature_formatting
    @_search_exception_handling
    def _info_from_next_tag(soup: BeautifulSoup, tag_text: str) -> str:
        return soup.find('span', text=tag_text).find_next().text.strip()

    @staticmethod
    @_feature_formatting
    @_search_exception_handling
    def _info_from_several_siblings(soup: BeautifulSoup, tag_text: str | re.Pattern):
        return soup.find('span', text=tag_text).find_next_siblings()

    @staticmethod
    @_feature_formatting
    @_search_exception_handling
    def _search_rating_info(soup: BeautifulSoup) -> str:
        return soup.find('span', itemprop='ratingValue').text.strip()

    @retry(exceptions=retry_exceptions, tries=5, delay=5, backoff=2, logger=logger)
    def get_anime_info(self, anime_url: str) -> None:
        response = requests.get(anime_url, headers={'User-Agent': UserAgent().chrome})
        soup = BeautifulSoup(response.text, 'lxml')

        title = soup.find('h1', class_='title-name h1_bold_none').text.strip()
        english_title = self._info_from_sibling(soup, 'English:')
        type_of_anime = self._info_from_next_tag(soup, 'Type:')
        episodes = self._info_from_sibling(soup, 'Episodes:')
        status = self._info_from_sibling(soup, 'Status:')
        aired = self._info_from_sibling(soup, 'Aired:')
        premiered = self._info_from_next_tag(soup, 'Premiered:')
        broadcast = self._info_from_sibling(soup, 'Broadcast:')
        producers = self._info_from_several_siblings(soup, 'Producers:')
        licensors = self._info_from_several_siblings(soup, 'Licensors:')
        studios = self._info_from_several_siblings(soup, 'Studios:')
        source = self._info_from_sibling(soup, 'Source:')
        genres = self._info_from_several_siblings(soup, 'Genres:')
        themes = self._info_from_several_siblings(soup, re.compile('Theme'))
        demographics = self._info_from_several_siblings(soup, 'Demographic:')
        duration = self._info_from_sibling(soup, 'Duration:')
        adult_rating = self._info_from_sibling(soup, 'Rating:')

        try:
            synopsis = soup.find('h2', text='Synopsis').find_parent().next_sibling.text.strip()
            if synopsis is None:
                synopsis = 'NaN'
            else:
                synopsis = synopsis.split('\n')[:-1]
                synopsis = ' '.join(synopsis)
        except AttributeError:
            synopsis = 'NaN'

        rating = self._search_rating_info(soup)

        with open(self.data_file, 'a') as data_file:
            writer = csv.writer(data_file)
            writer.writerow(
                (
                    title,
                    english_title,
                    type_of_anime,
                    episodes,
                    status,
                    aired,
                    premiered,
                    broadcast,
                    producers,
                    licensors,
                    studios,
                    source,
                    genres,
                    themes,
                    demographics,
                    duration,
                    adult_rating,
                    synopsis,
                    rating,
                )
            )

    @staticmethod
    @retry(exceptions=retry_exceptions, tries=5, delay=5, backoff=2, logger=logger)
    def collect_anime_links(page_url: str) -> list[str]:
        response = requests.get(page_url, headers={'User-Agent': UserAgent().chrome})

        if response.status_code == 404:
            raise requests.HTTPError

        soup = BeautifulSoup(response.text, 'lxml')

        anime_link_list = []
        anime_link_tags = soup.find_all('a', class_='hoverinfo_trigger fl-l ml12 mr8')
        for anime_link in anime_link_tags:
            anime_link_list.append(anime_link.get('href'))

        return anime_link_list

    def run_parser(self) -> None:
        with open(self.data_file) as data_file:
            reader = csv.reader(data_file)
            num_anime_records = sum(1 for _ in reader) - 1
        page_num = num_anime_records // 50

        while True:
            page_url = self.start_url + str(page_num)

            try:
                anime_links = self.collect_anime_links(page_url)
                logger.info(f'[*] Page {page_num} processing...')
                if self.progress_bar:
                    with click.progressbar(anime_links) as bar:
                        for link in bar:
                            self.get_anime_info(link)
                else:
                    for link_num, link in enumerate(anime_links, 1):
                        self.get_anime_info(link)
                        logger.info(f'    Link {link_num}/{len(anime_links)} processed')

            except requests.HTTPError:
                logger.info('[*] All pages have been processed. End of parsing.')
                break

            page_num += 50


@click.command()
@click.option('--out-file', '-f', default=None, type=click.Path())
@click.option('--redo', '-r', default=False)
@click.option('--progress-bar', '-p', default=False)
def main(out_file, redo, progress_bar) -> None:
    parser = AnimeParser(out_file, redo, progress_bar)
    parser.run_parser()


if __name__ == '__main__':
    main()
