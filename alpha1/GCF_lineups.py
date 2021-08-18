import logging
import os
import re
import time
import random
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict

import psycopg2
import requests
from bs4 import BeautifulSoup

# Enables Info logging to be displayed on console
logging.basicConfig(level = logging.INFO)

'''
ABANDONED: m.football-lineups.com has weirdly high anti-scraping mechanisms
'''

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

    def toSoup(self, response):
        return BeautifulSoup(response.text, "lxml")

    def fixtureLinkScraper(self, season_page_link):
        league, season, link = season_page_link

        club_ids = self.fetchClubIds(league, season)

        response = self.requestPage(link)
        soup = self.toSoup(response)

        fixtures = []

        table_tags = soup.find('table', {'class': 'table table-responsive table-condensed table-hover table-striped'}).find('tbody')
        for match in table_tags.find_all('tr'):
            current_match = {}
            if match.find('td', {'class': 'td_resul'}):

                # The following data will aid with matching the game in the database
                scoreline = match.find('td', {'class': 'td_resul'}).get_text()
                score_matcher = re.search("(\d|-):(\d|-)", scoreline)
                if not score_matcher:
                    continue
                if score_matcher.group(1) == "-" or score_matcher.group(2) == "-":
                    continue
                current_match['home_goals'] = score_matcher.group(1)
                current_match['away_goals'] = score_matcher.group(2)

                date_time = match.find('td', {'class': 'mobile-hiddenTD'}).get_text()
                date = re.search("\d\d-[A-Z][a-z][a-z]-\d\d", date_time).group(0)
                current_match['game_date'] = datetime.strptime(date, '%d-%b-%y').strftime("%Y-%m-%d")

                home_club_name = match.find('td', {'align': 'right'}).get_text()
                current_match['home_id'] = self.findMostSimilarClubName(home_club_name, club_ids)

                away_club_name = match.find('td', {'align': 'left'}).get_text()
                current_match['away_id'] = self.findMostSimilarClubName(away_club_name, club_ids)

                # Link to fixture page
                fixture_link = "https://m.football-lineups.com" + match.find('td', {'class': 'td_resul'}).a.get("href")
                self.scrapeLineup(fixture_link)



    def scrapeLineup(self, link):
        print(link)
        response = self.requestPage(link)
        soup = self.toSoup(response)

        tables = soup.find_all('table', {'class': 'table table-responsive table-condensed table-hover'})
        home_box = tables[0].find('tbody')
        away_box = tables[1].find('tbody')

        home_lineup = [row.get_text() for row in home_box.find_all('tr')]
        away_lineup = [row.get_text() for row in away_box.find_all('tr')]
        print(home_lineup)


    def findMostSimilarClubName(self, club_name, club_ids):
        if club_name in club_ids:
            return club_ids[club_name]
        else:
            closest = ("", 0.0)
            for key in club_ids:
                similarity = SequenceMatcher(None, key, club_name).ratio()
                if closest[1] < similarity:
                    closest = (key, similarity)

            return club_ids[closest[0]]

    def fetchClubIds(self, league_code, season):
        cursor = self._conn.cursor()
        select_statement = '''SELECT club_name, club_id 
                              FROM club 
                              JOIN league ON league.league_id=club.league_id 
                              WHERE league.league='{}' AND league.season='{}';'''.format(league_code, season)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())


    def runner(self, links):
        '''
        Uses multithreading to speed up the scraping of multiple pages
        '''
        with ThreadPoolExecutor(max_workers=5) as executer:
            futures = [executer.submit(self.fixtureLinkScraper, link) for link in links]

            dataset = []
            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                dataset += future.result()



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
    #scraper.insertLinksToDB(links)

    scraper.runner(links)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
