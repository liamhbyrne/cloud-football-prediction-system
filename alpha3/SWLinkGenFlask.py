import logging
import random
import re
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import psycopg2
import requests

# Enables Info logging to be displayed on console
logging.basicConfig(level = logging.INFO)


class SWLinkGenerator:
    '''
    This class handles the process of scraping lineups from websites and adding them to the database.
    '''
    def __init__(self, address):
        self._conn = self.connectToDB(address)


    def connectToDB(self, address : str):
        '''
        Obtain and return a connection object
        '''
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)

    def seasonLinkFetcher(self, urls):
        season_links = []
        years = ["2011-2012"] + ["{}{}".format(i, i+1) for i in range(2012, 2021)]
        for league_code, link in urls.items():
            season_search = re.search("\d\d\d\d-?\d\d\d\d", link).group(0)
            for year in years[years.index(season_search):]:
                year_endings = re.search("\d\d(\d\d)-?\d\d(\d\d)", year)
                season_links.append(
                    (league_code, year_endings.group(1) + year_endings.group(2), re.sub("\d\d\d\d-\d\d\d\d", year, link))
                )


        with ThreadPoolExecutor(max_workers=35) as executer:
            futures = [executer.submit(self.requestPage, link[2]) for link in season_links]

            working_links = []
            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                operating_link = future.result()
                if operating_link:
                    working_links.append(operating_link)

        if len(season_links) > len(working_links):
            logging.info("Some links have been omitted")
        season_links = [link for link in season_links if link[2] in working_links]

        return season_links


    def requestPage(self, url : str):
        user_agent_list = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ]
        next_user_agent = random.choice(user_agent_list)

        response = None
        try:
            header = {'user-agent': next_user_agent}
            response = requests.get(url, headers=header)  # Get page
        except requests.exceptions.ConnectionError as e:
            logging.error("ConnectionError: Likely too many simultaneous connections")

        if response.status_code != 200:
            return False

        return url

    def insertLinkIntoDB(self, links):
        cursor = self._conn.cursor()
        template = ','.join(['%s'] * len(links))
        insert_statement = '''INSERT INTO league (league, season, match_location)
                            VALUES {}
                            ON CONFLICT (season, league) DO UPDATE SET match_location=EXCLUDED.match_location;'''.format(template)

        cursor.execute(insert_statement, links)
        self._conn.commit()
