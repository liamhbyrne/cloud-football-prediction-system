import logging
import os
import random
import re
import time
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import psycopg2
import requests
from bs4 import BeautifulSoup

# Enables Info logging to be displayed on console
logging.basicConfig(level=logging.INFO)


class PlayerScraper:
    '''
    This class handles the process of scraping players from sofifa and adding them to the database.
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
        generated_links = []
        for league_identifier, league_number in league_numbers.items():
            for season, edition_number in edition_numbers.items():
                base_url = "https://sofifa.com/players?type=all&lg%5B0%5D={}&r={}&set=true&offset=0".format(
                    league_number, edition_number
                )
                generated_links.append((league_identifier, season, base_url))

        return generated_links

    def insertLinksToDB(self, links):
        cursor = self._conn.cursor()
        template = ','.join(['%s'] * len(links))
        insert_statement = '''
                    UPDATE league 
                    SET players_location=payload.link
                    FROM (VALUES {}) AS payload (league_code, season_code, link)
                    WHERE league=payload.league_code AND season=payload.season_code;
                           '''.format(template)

        cursor.execute(insert_statement, links)
        self._conn.commit()

    def requestPage(self, url: str):
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

    def runner(self, links):
        '''
        Uses multithreading to speed up the CSV parsing
        '''
        with ThreadPoolExecutor(max_workers=4) as executer:
            futures = [executer.submit(self.preprocess, league_code, season, link) for league_code, season, link in
                       links]

            dataset = []
            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                dataset += future.result()

        self.insertPlayers(dataset)

    def preprocess(self, league_code, season, link):
        print(link)
        club_ids = self.fetchClubIds(league_code, season)
        league_id = self.selectLeagueID(league_code, season)

        response = self.requestPage(link)
        players = []

        while response.url != "https://sofifa.com/players":
            soup = self.toSoup(response)

            extracted_values = self.parseHTML(soup, club_ids, league_id)
            players += extracted_values

            offset = int(re.search("offset=(\d+)\Z", response.url).group(1)) + 60
            next_link = re.sub("\d+\Z", str(offset), response.url)

            response = self.requestPage(next_link)

        return players

    def parseHTML(self, soup, club_ids, league_id):
        table_tags = soup.find('table', {'class': 'table table-hover persist-area'}).find('tbody')
        players = []
        for player in table_tags.find_all('tr'):  # for each table row
            current_player = {}
            for attribute in player.find_all('td'):  # for each table column
                # name/position tag
                if attribute['class'] == ['col-name']:
                    if attribute.find('a', {'class': 'tooltip'}):
                        current_player['name'] = attribute.find('a', {'class': 'tooltip'}).get_text()[1:]
                        current_player['position'] = attribute.find('a', {'rel': 'nofollow'}).get_text()
                        current_player['country'] = attribute.find('a', {'class': 'tooltip'}).div.img.get('title')
                    else:
                        club_name = attribute.div.a.get_text()
                        if club_name in club_ids:
                            current_player['club_id'] = club_ids[club_name]
                        else:
                            generated_club_id = self.insertClub(club_name, league_id)
                            current_player['club_id'] = generated_club_id
                            club_ids[club_name] = generated_club_id

                elif attribute['class'] == ['col', 'col-oa', 'col-sort']:
                    current_player['overall_rating'] = attribute.get_text()

                elif attribute['class'] == ['col', 'col-pt']:
                    current_player['potential_rating'] = attribute.get_text()

                elif attribute['class'] == ['col', 'col-ae']:
                    current_player['age'] = attribute.get_text()

                elif attribute['class'] == ['col', 'col-vl']:
                    current_player['value'] = attribute.get_text().replace("€", "").replace("M", "").replace("K", "")

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


def main(request):
    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    scraper = PlayerScraper(address)

    edition_numbers = {
        '1112': 120002,
        '1213': 130034,
        '1314': 140052,
        '1415': 150059,
        '1516': 160058,
        '1617': 170099,
        '1718': 180084,
        '1819': 190075,
        '1920': 200061,
        '2021': 210055
    }

    league_numbers = {
        'B1': 4,
        'E0': 13,
        'E1': 14,
        'F1': 16,
        'D1': 19,
        'I1': 31,
        'N1': 10,
        'P1': 308,
        'SP1': 53,
        'SC0': 50
    }

    links = scraper.linkGenerator(edition_numbers, league_numbers)
    scraper.insertLinksToDB(links)
    scraper.runner(links)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + " seconds")
    return str(end - start)

# Call to main, GCP does this implicitly
main("")
