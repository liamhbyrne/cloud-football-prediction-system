import logging
import os
import re
import time
import random
from typing import Dict, Tuple

import psycopg2
import requests


class LineupScraper:
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

    def seasonLinkFetcher(self, urls : Dict):
        season_links = []
        years = ["{}--{}".format(i, i+1) for i in range(1990, 2021)]
        for league_code, link in urls.items():
            season_search = re.search("\d\d\d\d--\d\d\d\d", link)
            for year in years[years.index(season_search.group(0)):]:
                year_endings = re.search("\d\d(\d\d)--\d\d(\d\d)", year)
                season_links.append(
                    (league_code, year_endings.group(1) + year_endings.group(2), re.sub("\d\d\d\d--\d\d\d\d", year, link))
                )
        return season_links

    def insertLinksToDB(self, links):
        cursor = self._conn.cursor()
        template = ','.join(['%s'] * len(links))
        insert_statement = '''
                    UPDATE league 
                    SET lineups_location=payload.link
                    FROM (VALUES {}) AS payload (league_code, season_code, link)
                    WHERE league=payload.league_code AND season=payload.season_code;
                           '''.format(template)

        cursor.execute(insert_statement, links)
        self._conn.commit()


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
            raise Exception("RESPONSE {} ON >>> {}".format(response.status_code, url))

        return response


def main(request):
    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment

    scraper = LineupScraper(address)
    scraper.requestPage("https://m.football-lineups.com/tourn/FA-Premier-League-2014--2015/fixture")

    leagues = {'B1' : 'https://m.football-lineups.com/tourn/Jupiler-League-2006--2007/fixture',
                 'E0' : 'https://m.football-lineups.com/tourn/FA-Premier-League-1997--1998/fixture',
                 'E1': 'https://m.football-lineups.com/tourn/The-Championship-2007--2008/fixture',
                 'F1' : 'https://m.football-lineups.com/tourn/Ligue-1-2005--2006/fixture',
                 'D1' : 'https://m.football-lineups.com/tourn/Bundesliga-2003--2004/fixture',
                 'I1' : 'https://m.football-lineups.com/tourn/Serie-A-1997--1998/fixture',
                 'N1' : 'https://m.football-lineups.com/tourn/Eredivisie-2005--2006/fixture',
                 'P1' : 'https://m.football-lineups.com/tourn/Portuguese-Liga-2005--2006/fixture',
                 'SP1' : 'https://m.football-lineups.com/tourn/La-Liga-2001--2002/fixture',
                 'SC0' : 'https://m.football-lineups.com/tourn/Scottish-Premiership-2013--2014/fixture'
               }

    links = scraper.seasonLinkFetcher(leagues)
    print(links)
    scraper.insertLinksToDB(links)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
