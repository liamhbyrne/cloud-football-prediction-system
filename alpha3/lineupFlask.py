import logging
import random
import re
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from difflib import SequenceMatcher

import psycopg2
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level = logging.INFO)


class SWLineupScraper:
    '''
    This class handles the process of scraping lineups from websites and adding them to the database.
    '''
    def __init__(self, address, league, season):
        self._season = season
        self._league = league
        self._conn = self.connectToDB(address)
        self._club_ids = self.fetchClubIds()
        self._player_ids = self.fetchPlayerIds()


    def connectToDB(self, address : str):
        '''
        Obtain and return a connection object
        '''
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)

    def requestPage(self, url: str):
        '''
        HTTP GET each fixture page with an alternating user agent.
        '''
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
        Makes appropriate function calls
        '''

        counter = 1
        '''
        for link in links:
            status = self.extractLineups(link)
            print("{} / {} complete".format(counter, len(links)))
            if status != 200:
                raise Exception("ERROR: Lineup extraction failed on: {}".format(link))
            counter += 1
        '''
        with ThreadPoolExecutor(max_workers=6) as executer:
            futures = [executer.submit(self.extractLineups, link) for link in links]

            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                status = future.result()
                print("{} / {} complete".format(counter, len(links)))
                if status != 200:
                    raise Exception("ERROR: Lineup extraction failed on: {}".format(status))
                counter += 1

    def extractLineups(self, link):
        soup = self.toSoup(self.requestPage(link))
        match_info = {}

        match_details = soup.find('div', {'class' : 'match-info'})
        if not match_details:
            return 200

        # DATE
        date = match_details.find('div', {'class': 'details'}).a.get_text()
        match_info['game_date'] = datetime.strptime(date, '%d/%m/%Y').strftime("%Y-%m-%d")

        # TEAMS
        home_team = match_details.find('div', {'class' : 'container left'})\
                                 .find('a', {'class' : 'team-title'}).get_text()

        away_team = match_details.find('div', {'class': 'container right'}) \
                                 .find('a', {'class': 'team-title'}).get_text()

        if home_team in self._club_ids:
            match_info["home_id"] = self._club_ids[home_team]
        else:
            match_info["home_id"] = self.searchSimilar(self._club_ids, home_team)

        if away_team in self._club_ids:
            match_info["away_id"] = self._club_ids[away_team]
        else:
            match_info["away_id"] = self.searchSimilar(self._club_ids, away_team)

        # Check if game has not happened yet
        status_box = match_details.find("div", {'class' : 'container middle'}).span
        if status_box:
            if match_details.find("div", {'class' : 'container middle'}).span.get_text() == "KO":
                match_info["status"] = "UPCOMING"

        if match_details.find("h3", {'class' : 'thick scoretime'}):
            game_state = match_details.find("h3", {'class' : 'thick scoretime'}).span.get_text()

            # if game is finished or in progress
            if "status" not in match_info:
                if game_state in ["FT", "AET"]:
                    match_info["status"] = "FT"
                else:
                    match_info["status"] = "STARTED"

                # SCORELINE
                scoreline = re.search(r'(\d) - (\d)', match_details.find("h3", {'class': 'thick scoretime'}).get_text())
                if scoreline:
                    match_info['home_goals'] = scoreline.group(1)
                    match_info['away_goals'] = scoreline.group(2)
                else:
                    logging.error("RegEx did not find scoreline.")

            else:  # No scoreline given to upcoming games
                match_info['home_goals'] = None
                match_info['away_goals'] = None

        else:
            # If the match does not have a scoreline, it may of been cancelled or erroneous (continue)
            return 200

        # LINEUPS
        lineups_containers = soup.find('div', {'class' : 'combined-lineups-container'})

        if lineups_containers:
            home_lineup_box = lineups_containers.find('div', {'class' : 'container left'}).table.tbody
            away_lineup_box = lineups_containers.find('div', {'class' : 'container right'}).table.tbody

            # HOME
            home_squad_ids = dict(self._player_ids[match_info["home_id"]])
            match_info["home_lineup"] = []

            for player in home_lineup_box.find_all('tr')[:11]:
                h_name = player.find('td', {'class' : 'player large-link'}).a.get_text()
                if h_name in home_squad_ids:
                    match_info["home_lineup"].append(home_squad_ids[h_name])
                else:
                    match_info["home_lineup"].append(self.searchSimilar(home_squad_ids, h_name))

            # AWAY
            away_squad_ids = dict(self._player_ids[match_info["away_id"]])
            match_info["away_lineup"] = []

            for player in away_lineup_box.find_all('tr')[:11]:
                a_name = player.find('td', {'class': 'player large-link'}).a.get_text()
                if a_name in away_squad_ids:
                    match_info["away_lineup"].append(away_squad_ids[a_name])
                else:
                    match_info["away_lineup"].append(self.searchSimilar(away_squad_ids, a_name))

        else:  #  If the lineups are not available
            match_info["home_lineup"] = [None for _ in range(11)]
            match_info["away_lineup"] = [None for _ in range(11)]

        try:  # Make calls to INSERT to database
            self.insertMatch(match_info["home_id"], match_info["away_id"], match_info["game_date"],
                             match_info["status"], link, match_info["home_lineup"],
                             match_info["away_lineup"], match_info["home_goals"], match_info["away_goals"])
        except Exception as e:
            print(e)
        return 200

    def searchSimilar(self, name_ids_dict, name):
        closest = ("", 0.0)
        for key in name_ids_dict:
            similarity = SequenceMatcher(None, key, name).ratio()
            if closest[1] < similarity:
                closest = (key, similarity)
        return name_ids_dict[closest[0]]

    def fetchClubIds(self):
        cursor = self._conn.cursor()

        select_statement = '''SELECT club_name, club_id 
                              FROM club 
                              JOIN league ON league.league_id=club.league_id 
                              WHERE league.league='{}' AND league.season='{}';'''.format(self._league, self._season)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())

    def fetchPlayerIds(self):
        cursor = self._conn.cursor()

        select_statement = '''SELECT club.club_id, player.name, player.player_id FROM player
                                JOIN club ON player.club_id=club.club_id
                                JOIN league ON league.league_id=club.league_id
                                WHERE league.season='{}' AND league.league='{}';
                            '''.format(self._season, self._league)
        cursor.execute(select_statement)

        query_result_set = cursor.fetchall()
        club_player_ids = {}
        for club_id, player_name, player_id in query_result_set:
            if club_id in club_player_ids:
                club_player_ids[club_id].append((player_name, player_id))
            else:
                club_player_ids[club_id] = [(player_name, player_id)]

        return club_player_ids

    def insertMatch(self, home_id, away_id, game_date, status, link, home_lineup, away_lineup, home_goals, away_goals):
        cursor = self._conn.cursor()

        template = ','.join(['%s'] * 29)
        match_insert_statement = '''
                            INSERT INTO match (home_id, away_id, game_date, status, link,
                             h1_player_id, h2_player_id, h3_player_id, h4_player_id, h5_player_id,
                             h6_player_id, h7_player_id, h8_player_id, h9_player_id, h10_player_id,
                             h11_player_id, a1_player_id, a2_player_id, a3_player_id,
				             a4_player_id, a5_player_id, a6_player_id, a7_player_id, a8_player_id,
				             a9_player_id, a10_player_id, a11_player_id, home_goals, away_goals)
                    VALUES ({}) RETURNING match_id;'''.format(template)

        cursor.execute(match_insert_statement, (home_id, away_id, game_date, status, link, *home_lineup, *away_lineup, home_goals, away_goals))
        self._conn.commit()

        match_id = cursor.fetchone()[0]
        club_match_insert_statement = '''
                            INSERT INTO club_match (club_id, match_id)
                            VALUES (%s, %s), (%s, %s)
                '''
        cursor.execute(club_match_insert_statement, (home_id, match_id, away_id, match_id))
        self._conn.commit()
