import logging
import random
import re
from difflib import SequenceMatcher

import psycopg2
import requests
from bs4 import BeautifulSoup


# Enables Info logging to be displayed on console
logging.basicConfig(level=logging.INFO)


class MatchRefresher:
    '''
    This class handles contains the functionality to update matches listed as 'UPCOMING' with lineups as
    they happen.
    '''

    def __init__(self, address):
        self._conn = self.connectToDB(address)
        self._player_ids = {} # Fetch all players from that league


    def connectToDB(self, address: str):
        '''
        Obtain and return a connection object
        '''
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)

    def fetchUpcomingMatches(self):
        """
        Select every match that is upcoming
        """
        cursor = self._conn.cursor()

        select_statement = '''SELECT match_id, home_id, away_id, link, league.league, league.season
                              FROM match
                              JOIN club ON match.home_id = club.club_id
                              JOIN league ON league.league_id = club.league_id
                              WHERE status = 'UPCOMING' AND date(game_date) <= current_date;
                            '''
        cursor.execute(select_statement)
        for match_id, home_id, away_id, link, league, season in cursor.fetchall():
            if (home_id not in self._player_ids) or (away_id not in self._player_ids):
                self.fetchPlayerIds(league, season)

            home_goals, away_goals, home_lineup, away_lineup = self.extractMatchInfo(link)
            if all(x is not None for x in [home_goals, away_goals, home_lineup, away_lineup]):
                home_lineup_ids, away_lineup_ids = self.matchPlayerIds(home_id, away_id, home_lineup, away_lineup)
                self.updateMatch(match_id, home_goals, away_goals, home_lineup_ids, away_lineup_ids)

    def updateMatch(self, match_id, home_goals, away_goals, home_lineup_ids, away_lineup_ids):
        values = [match_id, home_goals, away_goals, *home_lineup_ids, *away_lineup_ids]
        cursor = self._conn.cursor()

        template = ','.join(['%s'] * 25)
        update_statement = '''UPDATE match 
                SET home_goals = payload.home_goals, away_goals = payload.away_goals,
                h1_player_id = payload.h1_player_id, h2_player_id = payload.h2_player_id,
                h3_player_id = payload.h3_player_id, h4_player_id = payload.h4_player_id,
                h5_player_id = payload.h5_player_id, h6_player_id = payload.h6_player_id,
                h7_player_id = payload.h7_player_id, h8_player_id = payload.h8_player_id,
                h9_player_id = payload.h9_player_id, h10_player_id = payload.h10_player_id,
                h11_player_id = payload.h11_player_id, 
                a1_player_id = payload.a1_player_id, a2_player_id = payload.a2_player_id,
                a3_player_id = payload.a3_player_id, a4_player_id = payload.a4_player_id,
                a5_player_id = payload.a5_player_id, a6_player_id = payload.a6_player_id,
                a7_player_id = payload.a7_player_id, a8_player_id = payload.a8_player_id,
                a9_player_id = payload.a9_player_id, a10_player_id = payload.a10_player_id,
                a11_player_id = payload.a11_player_id
                FROM (VALUES {}) AS payload (match_id, home_goals, away_goals,
                h1_player_id, h2_player_id, h3_player_id, h4_player_id, h5_player_id, h6_player_id, h7_player_id, 
                h8_player_id, h9_player_id, h10_player_id, h11_player_id, 
                a1_player_id, a2_player_id, a3_player_id, a4_player_id, a5_player_id, a6_player_id, a7_player_id, 
                a8_player_id, a9_player_id, a10_player_id, a11_player_id)
                WHERE match.match_id = payload.match_id'''.format(template)
        cursor.execute(update_statement, values)

    def requestPage(self, url: str):
        '''
        HTTP GET each fixture page with an alternating user agent.
        '''
        user_agent_list = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.22 (KHTML, like Gecko) Chrome/19.0.1047.0 Safari/535.22",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21",
            "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1041.0 Safari/535.21",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
            "Mozilla/5.0 (Macintosh; AMD Mac OS X 10_8_2) AppleWebKit/535.22 (KHTML, like Gecko) Chrome/18.6.872",
            "Mozilla/5.0 (X11; CrOS i686 1660.57.0) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.46 Safari/535.19"]
        next_user_agent = random.choice(user_agent_list)

        try:
            header = {'user-agent': next_user_agent}
            response = requests.get(url, headers=header)  # Get page
        except requests.exceptions.ConnectionError as e:
            logging.warning("Failed to get a response on {}".format(url))
            return None

        # Some pages are inaccessible due to server, these can be passed but ideally a minimum
        if response.status_code == 500:
            logging.warning("Response 500 on {}".format(url))
            return None

        if response.status_code != 200:
            raise Exception("RESPONSE {} ON >>> {}".format(response.status_code, url))

        return response

    def toSoup(self, response):
        """
        Make sure lxml html parser is installed
        """
        return BeautifulSoup(response.text, "lxml")

    def extractMatchInfo(self, link: str):
        '''
        Content extraction method for match info and lineups
        '''
        home_goals = None
        away_goals = None
        home_lineup = None
        away_lineup = None

        response = self.requestPage(link)
        if not response:
            return (None, None, None, None)

        soup = self.toSoup(response)

        match_details = soup.find('div', {'class': 'match-info'})
        if match_details:

            # GAME STATUS
            if match_details.find("h3", {'class': 'thick scoretime'}):
                game_state = match_details.find("h3", {'class': 'thick scoretime'}).span.get_text()

                if game_state in ["FT", "AET"]:

                    # SCORELINE
                    scoreline = re.search(r'(\d) - (\d)',
                                          match_details.find("h3", {'class': 'thick scoretime'}).get_text())
                    if scoreline:
                        home_goals = scoreline.group(1)
                        away_goals = scoreline.group(2)
                    else:
                        logging.error("RegEx did not find scoreline.")

        # LINEUPS
        lineups_containers = soup.find('div', {'class': 'combined-lineups-container'})

        if lineups_containers:
            home_lineup_box = lineups_containers.find('div', {'class': 'container left'}).table.tbody
            away_lineup_box = lineups_containers.find('div', {'class': 'container right'}).table.tbody

            # If a full lineup is not provided, ignore the match
            if len(home_lineup_box.find_all('tr')) < 12 or len(away_lineup_box.find_all('tr')) < 12:
                return None

            # HOME
            home_lineup = [player.find('td', {'class': 'player large-link'}).a.get_text()
                                         for player in home_lineup_box.find_all('tr')[:11]]

            # AWAY
            away_lineup = [player.find('td', {'class': 'player large-link'}).a.get_text()
                                         for player in away_lineup_box.find_all('tr')[:11]]

        return home_goals, away_goals, home_lineup, away_lineup

    def searchSimilar(self, name_ids_dict, name):
        """
        Finds the most similar string to name in name_ids_dict
        """
        closest = ("", 0.0)
        for key in name_ids_dict:
            similarity = SequenceMatcher(None, key, name).ratio()
            if closest[1] < similarity:
                closest = (key, similarity)
        return name_ids_dict[closest[0]]

    def matchPlayerIds(self, home_id, away_id, home_lineup, away_lineup):
        # HOME
        home_squad_ids = dict(self._player_ids[home_id])
        home_lineup_ids = []

        for h_name in home_lineup:
            if h_name is None:  # If lineup not available
                home_lineup_ids.append(None)
            elif h_name in home_squad_ids:
                home_lineup_ids.append(home_squad_ids[h_name])
            else:
                # If the h_name can't be matched use string similarity
                home_lineup_ids.append(self.searchSimilar(home_squad_ids, h_name))

        # AWAY
        away_squad_ids = dict(self._player_ids[away_id])
        away_lineup_ids = []

        for a_name in away_lineup:
            if a_name is None:  # If lineup not available
                away_lineup_ids.append(None)
            elif a_name in away_squad_ids:
                away_lineup_ids.append(away_squad_ids[a_name])
            else:
                away_lineup_ids.append(self.searchSimilar(away_squad_ids, a_name))

        return home_lineup_ids, away_lineup_ids

    def fetchPlayerIds(self, season, league):
        """
        Select every player in a given league and group by the club in a dictionary
        """
        cursor = self._conn.cursor()

        select_statement = '''SELECT club.club_id, player.name, player.player_id FROM player
                                JOIN club ON player.club_id=club.club_id
                                JOIN league ON league.league_id=club.league_id
                                WHERE league.season='{}' AND league.league='{}';
                            '''.format(season, league)
        cursor.execute(select_statement)

        query_result_set = cursor.fetchall()

        for club_id, player_name, player_id in query_result_set:
            if club_id in self._player_ids:
                self._player_ids[club_id].append((player_name, player_id))
            else:
                self._player_ids[club_id] = [(player_name, player_id)]
