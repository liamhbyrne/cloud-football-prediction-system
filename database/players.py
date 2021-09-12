import logging
import random
import re
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import psycopg2
import requests
from bs4 import BeautifulSoup
from flask import Flask

app = Flask(__name__)

# Enables Info logging to be displayed on console
logging.basicConfig(level=logging.INFO)


class PlayerScraper:
    '''
    This class handles the process of scraping players from sofifa and inserting them to the database.
    '''

    def __init__(self, address):
        self._conn = self.connectToDB(address)

    def connectToDB(self, address: str):
        '''
        Obtain and return a connection object
        '''
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)

    def linkGenerator(self, edition_numbers, league_numbers):
        '''
        Makes substitution into the URL to access different leagues and seasons of sofifa.com
        '''
        generated_links = []
        # for each league
        for league_identifier, league_number in league_numbers.items():
            # for each edition (season)
            for season, edition_number in edition_numbers.items():
                base_url = "https://sofifa.com/players?type=all&lg%5B0%5D={}&r={}&set=true&offset=0".format(
                    league_number, edition_number
                )
                generated_links.append((league_identifier, season, base_url))

        return generated_links

    def insertLinksToDB(self, links):
        '''
        Insert link into the players_location column of the league table
        '''
        cursor = self._conn.cursor()
        template = ','.join(['%s'] * len(links))
        insert_statement = '''
                    INSERT INTO league (league, season, players_location)
                    VALUES {} ON CONFLICT (league, season) DO
                    UPDATE SET players_location=EXCLUDED.players_location;
                           '''.format(template)

        cursor.execute(insert_statement, links)
        self._conn.commit()

    def requestPage(self, url: str):
        '''
        HTTP GET page with rotating user agents
        '''
        user_agent_list = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ]
        next_user_agent = random.choice(user_agent_list)

        try:
            header = {'user-agent': next_user_agent}
            response = requests.get(url, headers=header)  # Get page with random user agent
        except requests.exceptions.ConnectionError:
            logging.error("ConnectionError: Likely too many simultaneous connections")
            logging.warning("Not all pages have been scraped")
            return None

        if response.status_code != 200:
            logging.error("RESPONSE {} ON >>> {}".format(response.status_code, url))
            logging.warning("Not all pages have been scraped")
            return None

        return response

    def toSoup(self, response):
        '''
        Make sure lxml parser has been installed
        '''
        return BeautifulSoup(response.text, "lxml")

    def runner(self, links):
        '''
        Uses multithreading to speed up the scraping process
        '''
        with ThreadPoolExecutor(max_workers=4) as executer:
            futures = [executer.submit(self.preprocess, league_code, season, link) for league_code, season, link in
                       links]

            dataset = []
            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                dataset += future.result()

        # Insert into DB in one go
        self.insertPlayers(dataset)

    def preprocess(self, league_code, season, link):
        """
        This method controls the execution process for each league/season
        """
        club_ids = self.fetchClubIds(league_code, season)
        league_id = self.selectLeagueID(league_code, season)

        response = self.requestPage(link)
        if not response:  # If no/invalid response end execution and safely exit
            return None

        players = []

        while response.url != "https://sofifa.com/players":  # While page is not redirected to home page
            soup = self.toSoup(response)

            extracted_values = self.parseHTML(soup, club_ids, league_id)  # parse page
            players += extracted_values

            # Edit link for next page
            offset = int(re.search("offset=(\d+)\Z", response.url).group(1)) + 60
            next_link = re.sub("\d+\Z", str(offset), response.url)

            response = self.requestPage(next_link)
            if not response:
                return None

        return players

    def parseHTML(self, soup, club_ids, league_id):
        """
        This method handles the content extraction
        """

        table_tags = soup.find('table', {'class': 'table table-hover persist-area'}).find('tbody')

        players = []

        # for each table row
        for player in table_tags.find_all('tr'):
            current_player = {}

            # for each table column
            for attribute in player.find_all('td'):
                # name/position/country or club tag
                if attribute['class'] == ['col-name']:
                    if attribute.find('a', {'class': 'tooltip'}):
                        current_player['name'] = attribute.find('a', {'class': 'tooltip'}).get_text()
                        current_player['position'] = attribute.find('a', {'rel': 'nofollow'}).get_text()
                        current_player['country'] = attribute.find('img').get('title')
                    else:
                        # club tag
                        club_name = attribute.div.a.get_text()
                        if club_name in club_ids:  # If club already exists
                            current_player['club_id'] = club_ids[club_name]
                        else:
                            generated_club_id = self.insertClub(club_name, league_id)
                            current_player['club_id'] = generated_club_id
                            club_ids[club_name] = generated_club_id  # Add to club_ids to prevent multiple inserts

                # Overall rating tag
                elif attribute['class'] == ['col', 'col-oa', 'col-sort']:
                    current_player['overall_rating'] = attribute.get_text()

                # Potential rating tag
                elif attribute['class'] == ['col', 'col-pt']:
                    current_player['potential_rating'] = attribute.get_text()

                # Age tag
                elif attribute['class'] == ['col', 'col-ae']:
                    current_player['age'] = attribute.get_text()

                # Value tag
                elif attribute['class'] == ['col', 'col-vl']:
                    value = attribute.get_text().replace("€", "")
                    if "M" in value:  # If value in the millions
                        current_player['value'] = value.replace("M", "")
                    elif "K" in value: # if the value is in the thousands divide by 1000
                        current_player['value'] = str(int(value.replace("K", "")) / 1000)
                    else:
                        current_player['value'] = value

                # Total rating tag
                elif attribute['class'] == ['col', 'col-tt']:
                    current_player['total_rating'] = attribute.get_text()

            players.append((current_player['name'], current_player['club_id'], current_player['overall_rating'],
                            current_player['potential_rating'], current_player['position'], current_player['age'],
                            current_player['value'], current_player['country'], current_player['total_rating']))

        return players

    def selectLeagueID(self, league_code, season):
        cursor = self._conn.cursor()
        select_statement = '''SELECT league_id
                              FROM league
                              WHERE league.league='{}' AND league.season='{}';'''.format(league_code, season)
        cursor.execute(select_statement)

        return cursor.fetchone()

    def fetchClubIds(self, league_code, season):
        cursor = self._conn.cursor()
        select_statement = '''SELECT club_name, club_id 
                              FROM club 
                              JOIN league ON league.league_id=club.league_id 
                              WHERE league.league='{}' AND league.season='{}';'''.format(league_code, season)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())

    def insertClub(self, club_name, league_id):
        cursor = self._conn.cursor()
        insert_statement = '''INSERT INTO club (league_id, club_name)
                                        VALUES (%s, %s)
                                        ON CONFLICT DO NOTHING 
                                        RETURNING club.club_id;'''

        cursor.execute(insert_statement, (league_id, club_name))
        self._conn.commit()

        return cursor.fetchone()[0]

    def insertPlayers(self, players):
        cursor = self._conn.cursor()
        template = ','.join(['%s'] * len(players))
        insert_statement = '''INSERT INTO player (name, club_id, overall_rating, potential_rating,
                            position, age, value, country, total_rating)
                            VALUES {};'''.format(template)

        cursor.execute(insert_statement, players)
        self._conn.commit()
