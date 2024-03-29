from datetime import datetime
import logging
import os
import re
import time
import random
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from difflib import SequenceMatcher

import psycopg2
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level = logging.INFO)


class SWLineupScraper:
    '''
    This class handles the process of scraping lineups from websites and adding them to the database.
    '''
    def __init__(self, address, league, season, links):
        self._links = links
        self._season = season
        self._league = league
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
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = [executor.submit(self.extractLineups, link) for link in links]

            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                if future.result() != 200:
                    raise Exception("")

    def extractLineups(self, link):
        soup = self.toSoup(self.requestPage(link))
        match_info = {}

        match_details = soup.find('div', {'class' : 'match-info'})

        home_team = match_details.find('div', {'class' : 'container left'})\
                                 .find('a', {'class' : 'team-title'}).get_text()

        away_team = match_details.find('div', {'class': 'container right'}) \
                                 .find('a', {'class': 'team-title'}).get_text()

        club_ids = self.fetchClubIds()

        if home_team in club_ids:
            match_info["home_id"] = club_ids[home_team]
        else:
            match_info["home_id"] = self.searchSimilar(club_ids, home_team)

        if away_team in club_ids:
            match_info["away_id"] = club_ids[away_team]
        else:
            match_info["away_id"] = self.searchSimilar(club_ids, away_team)

        home_squad_ids = self.fetchPlayerIds(match_info["home_id"])
        away_squad_ids = self.fetchPlayerIds(match_info["away_id"])

        game_state = match_details.find("h3", {'class' : 'thick scoretime'}).span.get_text()
        if game_state in ["FT", "AET"]:
            match_info["status"] = "FT"
        elif game_state == "KO":
            match_info["status"] = "UPCOMING"
        else:
            match_info["status"] = "STARTED"

        scoreline = re.search(r'(\d) - (\d)', match_details.find("h3", {'class' : 'thick scoretime'}).get_text())
        if scoreline:
            match_info['home_goals'] = scoreline.group(1)
            match_info['away_goals'] = scoreline.group(2)
        else:
            logging.error("RegEx did not find scoreline.")

        date = soup.find('div', {'class' : 'details'}).a.get_text()
        match_info['game_date'] = datetime.strptime(date, '%d/%m/%Y').strftime("%Y-%m-%d")

        lineups_containers = soup.find('div', {'class' : 'combined-lineups-container'})
        home_lineup_box = lineups_containers.find('div', {'class' : 'container left'}).table.tbody
        away_lineup_box = lineups_containers.find('div', {'class' : 'container right'}).table.tbody

        match_info["home_lineup"] = []

        for player in home_lineup_box.find_all('tr')[:11]:
            h_name = player.find('td', {'class' : 'player large-link'}).a.get_text()
            if h_name in home_squad_ids:
                match_info["home_lineup"].append(home_squad_ids[h_name])
            else:
                match_info["home_lineup"].append(self.searchSimilar(home_squad_ids, h_name))

        match_info["away_lineup"] = []

        for player in away_lineup_box.find_all('tr')[:11]:
            a_name = player.find('td', {'class': 'player large-link'}).a.get_text()
            if a_name in away_squad_ids:
                match_info["away_lineup"].append(away_squad_ids[a_name])
            else:
                match_info["away_lineup"].append(self.searchSimilar(away_squad_ids, a_name))

        try:
            self.insertMatch(match_info["home_id"], match_info["away_id"], match_info["game_date"], match_info["home_lineup"],
                         match_info["away_lineup"], match_info["home_goals"], match_info["away_goals"])
        except Exception as e:
            print(e)
        return 200

    def searchSimilar(self, club_ids, club_name):
        closest = ("", 0.0)
        for key in club_ids:
            similarity = SequenceMatcher(None, key, club_name).ratio()
            if closest[1] < similarity:
                closest = (key, similarity)
        return club_ids[closest[0]]

    def fetchClubIds(self):
        cursor = self._conn.cursor()
        season_search = re.search("\d\d(\d\d)\d\d(\d\d)", self._season)
        short_season = season_search.group(1) + season_search.group(2)

        select_statement = '''SELECT club_name, club_id 
                              FROM club 
                              JOIN league ON league.league_id=club.league_id 
                              WHERE league.league='{}' AND league.season='{}';'''.format(self._league, short_season)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())

    def fetchPlayerIds(self, club_id):
        cursor = self._conn.cursor()

        select_statement = '''SELECT name, player_id 
                              FROM player
                              WHERE club_id={};'''.format(club_id)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())

    def insertMatch(self, home_id, away_id, game_date, home_lineup, away_lineup, home_goals, away_goals):
        cursor = self._conn.cursor()

        template = ','.join(['%s'] * 27)
        match_insert_statement = '''
                            INSERT INTO match (home_id, away_id, game_date, h1_player_id, h2_player_id, h3_player_id,
				   h4_player_id, h5_player_id, h6_player_id, h7_player_id, h8_player_id,
				   h9_player_id, h10_player_id, h11_player_id, a1_player_id, a2_player_id, a3_player_id,
				   a4_player_id, a5_player_id, a6_player_id, a7_player_id, a8_player_id,
				   a9_player_id, a10_player_id, a11_player_id, home_goals, away_goals)
                    VALUES ({}) RETURNING match_id;'''.format(template)

        cursor.execute(match_insert_statement, (home_id, away_id, game_date, *home_lineup, *away_lineup, home_goals, away_goals))
        self._conn.commit()

        match_id = cursor.fetchone()[0]
        club_match_insert_statement = '''
                            INSERT INTO club_match (club_id, match_id)
                            VALUES (%s, %s), (%s, %s)
                '''
        cursor.execute(club_match_insert_statement, (home_id, match_id, away_id, match_id))
        self._conn.commit()


def main(request):
    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment

    links = ['https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/fc-bayern-munchen/fc-augsburg/3331888/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/bv-borussia-09-dortmund/bayer-04-leverkusen/3331889/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/tsg-1899-hoffenheim-ev/hertha-bsc-berlin/3331890/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/vfl-wolfsburg/1-fsv-mainz-05/3331891/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/eintracht-frankfurt/sc-freiburg/3331892/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/1-fc-union-berlin/rb-leipzig/3331893/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/1-fc-koln/fc-schalke-04/3331894/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/sv-werder-bremen/borussia-monchengladbach/3331895/', 'https://uk.soccerway.com//matches/2021/05/22/germany/bundesliga/vfb-stuttgart-1893-ev/dsc-arminia-bielefeld/3331896/', 'https://uk.soccerway.com//matches/2020/09/18/germany/bundesliga/fc-bayern-munchen/fc-schalke-04/3331591/', 'https://uk.soccerway.com//matches/2020/09/19/germany/bundesliga/eintracht-frankfurt/dsc-arminia-bielefeld/3331595/', 'https://uk.soccerway.com//matches/2020/09/19/germany/bundesliga/1-fc-union-berlin/fc-augsburg/3331596/', 'https://uk.soccerway.com//matches/2020/09/19/germany/bundesliga/1-fc-koln/tsg-1899-hoffenheim-ev/3331597/', 'https://uk.soccerway.com//matches/2020/09/19/germany/bundesliga/sv-werder-bremen/hertha-bsc-berlin/3331598/', 'https://uk.soccerway.com//matches/2020/09/19/germany/bundesliga/vfb-stuttgart-1893-ev/sc-freiburg/3331599/', 'https://uk.soccerway.com//matches/2020/09/19/germany/bundesliga/bv-borussia-09-dortmund/borussia-monchengladbach/3331592/', 'https://uk.soccerway.com//matches/2020/09/20/germany/bundesliga/rb-leipzig/1-fsv-mainz-05/3331593/', 'https://uk.soccerway.com//matches/2020/09/20/germany/bundesliga/vfl-wolfsburg/bayer-04-leverkusen/3331594/', 'https://uk.soccerway.com//matches/2020/09/25/germany/bundesliga/hertha-bsc-berlin/eintracht-frankfurt/3331604/', 'https://uk.soccerway.com//matches/2020/09/26/germany/bundesliga/borussia-monchengladbach/1-fc-union-berlin/3331600/', 'https://uk.soccerway.com//matches/2020/09/26/germany/bundesliga/bayer-04-leverkusen/rb-leipzig/3331601/', 'https://uk.soccerway.com//matches/2020/09/26/germany/bundesliga/1-fsv-mainz-05/vfb-stuttgart-1893-ev/3331606/', 'https://uk.soccerway.com//matches/2020/09/26/germany/bundesliga/fc-augsburg/bv-borussia-09-dortmund/3331607/', 'https://uk.soccerway.com//matches/2020/09/26/germany/bundesliga/dsc-arminia-bielefeld/1-fc-koln/3331608/', 'https://uk.soccerway.com//matches/2020/09/26/germany/bundesliga/fc-schalke-04/sv-werder-bremen/3331605/', 'https://uk.soccerway.com//matches/2020/09/27/germany/bundesliga/tsg-1899-hoffenheim-ev/fc-bayern-munchen/3331602/', 'https://uk.soccerway.com//matches/2020/09/27/germany/bundesliga/sc-freiburg/vfl-wolfsburg/3331603/', 'https://uk.soccerway.com//matches/2020/10/02/germany/bundesliga/1-fc-union-berlin/1-fsv-mainz-05/3331614/', 'https://uk.soccerway.com//matches/2020/10/03/germany/bundesliga/bv-borussia-09-dortmund/sc-freiburg/3331610/', 'https://uk.soccerway.com//matches/2020/10/03/germany/bundesliga/eintracht-frankfurt/tsg-1899-hoffenheim-ev/3331613/', 'https://uk.soccerway.com//matches/2020/10/03/germany/bundesliga/1-fc-koln/borussia-monchengladbach/3331615/', 'https://uk.soccerway.com//matches/2020/10/03/germany/bundesliga/sv-werder-bremen/dsc-arminia-bielefeld/3331616/', 'https://uk.soccerway.com//matches/2020/10/03/germany/bundesliga/vfb-stuttgart-1893-ev/bayer-04-leverkusen/3331617/', 'https://uk.soccerway.com//matches/2020/10/03/germany/bundesliga/rb-leipzig/fc-schalke-04/3331611/', 'https://uk.soccerway.com//matches/2020/10/04/germany/bundesliga/vfl-wolfsburg/fc-augsburg/3331612/', 'https://uk.soccerway.com//matches/2020/10/04/germany/bundesliga/fc-bayern-munchen/hertha-bsc-berlin/3331609/', 'https://uk.soccerway.com//matches/2020/10/17/germany/bundesliga/tsg-1899-hoffenheim-ev/bv-borussia-09-dortmund/3331619/', 'https://uk.soccerway.com//matches/2020/10/17/germany/bundesliga/sc-freiburg/sv-werder-bremen/3331620/', 'https://uk.soccerway.com//matches/2020/10/17/germany/bundesliga/hertha-bsc-berlin/vfb-stuttgart-1893-ev/3331621/', 'https://uk.soccerway.com//matches/2020/10/17/germany/bundesliga/1-fsv-mainz-05/bayer-04-leverkusen/3331623/', 'https://uk.soccerway.com//matches/2020/10/17/germany/bundesliga/fc-augsburg/rb-leipzig/3331625/', 'https://uk.soccerway.com//matches/2020/10/17/germany/bundesliga/dsc-arminia-bielefeld/fc-bayern-munchen/3331626/', 'https://uk.soccerway.com//matches/2020/10/17/germany/bundesliga/borussia-monchengladbach/vfl-wolfsburg/3331618/', 'https://uk.soccerway.com//matches/2020/10/18/germany/bundesliga/1-fc-koln/eintracht-frankfurt/3331624/', 'https://uk.soccerway.com//matches/2020/10/18/germany/bundesliga/fc-schalke-04/1-fc-union-berlin/3331622/', 'https://uk.soccerway.com//matches/2020/10/23/germany/bundesliga/vfb-stuttgart-1893-ev/1-fc-koln/3331635/', 'https://uk.soccerway.com//matches/2020/10/24/germany/bundesliga/fc-bayern-munchen/eintracht-frankfurt/3331627/', 'https://uk.soccerway.com//matches/2020/10/24/germany/bundesliga/rb-leipzig/hertha-bsc-berlin/3331629/', 'https://uk.soccerway.com//matches/2020/10/24/germany/bundesliga/1-fc-union-berlin/sc-freiburg/3331632/', 'https://uk.soccerway.com//matches/2020/10/24/germany/bundesliga/1-fsv-mainz-05/borussia-monchengladbach/3331633/', 'https://uk.soccerway.com//matches/2020/10/24/germany/bundesliga/bv-borussia-09-dortmund/fc-schalke-04/3331628/', 'https://uk.soccerway.com//matches/2020/10/25/germany/bundesliga/vfl-wolfsburg/dsc-arminia-bielefeld/3331631/', 'https://uk.soccerway.com//matches/2020/10/25/germany/bundesliga/sv-werder-bremen/tsg-1899-hoffenheim-ev/3331634/', 'https://uk.soccerway.com//matches/2020/10/26/germany/bundesliga/bayer-04-leverkusen/fc-augsburg/3331630/', 'https://uk.soccerway.com//matches/2020/10/30/germany/bundesliga/fc-schalke-04/vfb-stuttgart-1893-ev/3331641/', 'https://uk.soccerway.com//matches/2020/10/31/germany/bundesliga/eintracht-frankfurt/sv-werder-bremen/3331639/', 'https://uk.soccerway.com//matches/2020/10/31/germany/bundesliga/1-fc-koln/fc-bayern-munchen/3331642/', 'https://uk.soccerway.com//matches/2020/10/31/germany/bundesliga/fc-augsburg/1-fsv-mainz-05/3331643/', 'https://uk.soccerway.com//matches/2020/10/31/germany/bundesliga/dsc-arminia-bielefeld/bv-borussia-09-dortmund/3331644/', 'https://uk.soccerway.com//matches/2020/10/31/germany/bundesliga/borussia-monchengladbach/rb-leipzig/3331636/', 'https://uk.soccerway.com//matches/2020/11/01/germany/bundesliga/sc-freiburg/bayer-04-leverkusen/3331638/', 'https://uk.soccerway.com//matches/2020/11/01/germany/bundesliga/hertha-bsc-berlin/vfl-wolfsburg/3331640/', 'https://uk.soccerway.com//matches/2020/11/02/germany/bundesliga/tsg-1899-hoffenheim-ev/1-fc-union-berlin/3331637/', 'https://uk.soccerway.com//matches/2020/11/06/germany/bundesliga/sv-werder-bremen/1-fc-koln/3331652/', 'https://uk.soccerway.com//matches/2020/11/07/germany/bundesliga/rb-leipzig/sc-freiburg/3331646/', 'https://uk.soccerway.com//matches/2020/11/07/germany/bundesliga/1-fc-union-berlin/dsc-arminia-bielefeld/3331649/', 'https://uk.soccerway.com//matches/2020/11/07/germany/bundesliga/1-fsv-mainz-05/fc-schalke-04/3331650/', 'https://uk.soccerway.com//matches/2020/11/07/germany/bundesliga/fc-augsburg/hertha-bsc-berlin/3331651/', 'https://uk.soccerway.com//matches/2020/11/07/germany/bundesliga/vfb-stuttgart-1893-ev/eintracht-frankfurt/3331653/', 'https://uk.soccerway.com//matches/2020/11/07/germany/bundesliga/bv-borussia-09-dortmund/fc-bayern-munchen/3331645/', 'https://uk.soccerway.com//matches/2020/11/08/germany/bundesliga/vfl-wolfsburg/tsg-1899-hoffenheim-ev/3331648/', 'https://uk.soccerway.com//matches/2020/11/08/germany/bundesliga/bayer-04-leverkusen/borussia-monchengladbach/3331647/', 'https://uk.soccerway.com//matches/2020/11/21/germany/bundesliga/fc-bayern-munchen/sv-werder-bremen/3331654/', 'https://uk.soccerway.com//matches/2020/11/21/germany/bundesliga/borussia-monchengladbach/fc-augsburg/3331655/', 'https://uk.soccerway.com//matches/2020/11/21/germany/bundesliga/tsg-1899-hoffenheim-ev/vfb-stuttgart-1893-ev/3331656/', 'https://uk.soccerway.com//matches/2020/11/21/germany/bundesliga/fc-schalke-04/vfl-wolfsburg/3331660/', 'https://uk.soccerway.com//matches/2020/11/21/germany/bundesliga/dsc-arminia-bielefeld/bayer-04-leverkusen/3331662/', 'https://uk.soccerway.com//matches/2020/11/21/germany/bundesliga/eintracht-frankfurt/rb-leipzig/3331658/', 'https://uk.soccerway.com//matches/2020/11/21/germany/bundesliga/hertha-bsc-berlin/bv-borussia-09-dortmund/3331659/', 'https://uk.soccerway.com//matches/2020/11/22/germany/bundesliga/sc-freiburg/1-fsv-mainz-05/3331657/', 'https://uk.soccerway.com//matches/2020/11/22/germany/bundesliga/1-fc-koln/1-fc-union-berlin/3331661/', 'https://uk.soccerway.com//matches/2020/11/27/germany/bundesliga/vfl-wolfsburg/sv-werder-bremen/3331667/', 'https://uk.soccerway.com//matches/2020/11/28/germany/bundesliga/bv-borussia-09-dortmund/1-fc-koln/3331663/', 'https://uk.soccerway.com//matches/2020/11/28/germany/bundesliga/rb-leipzig/dsc-arminia-bielefeld/3331664/', 'https://uk.soccerway.com//matches/2020/11/28/germany/bundesliga/1-fc-union-berlin/eintracht-frankfurt/3331668/', 'https://uk.soccerway.com//matches/2020/11/28/germany/bundesliga/fc-augsburg/sc-freiburg/3331670/', 'https://uk.soccerway.com//matches/2020/11/28/germany/bundesliga/vfb-stuttgart-1893-ev/fc-bayern-munchen/3331671/', 'https://uk.soccerway.com//matches/2020/11/28/germany/bundesliga/borussia-monchengladbach/fc-schalke-04/3331665/', 'https://uk.soccerway.com//matches/2020/11/29/germany/bundesliga/bayer-04-leverkusen/hertha-bsc-berlin/3331666/', 'https://uk.soccerway.com//matches/2020/11/29/germany/bundesliga/1-fsv-mainz-05/tsg-1899-hoffenheim-ev/3331669/', 'https://uk.soccerway.com//matches/2020/12/04/germany/bundesliga/hertha-bsc-berlin/1-fc-union-berlin/3331676/', 'https://uk.soccerway.com//matches/2020/12/05/germany/bundesliga/sc-freiburg/borussia-monchengladbach/3331674/', 'https://uk.soccerway.com//matches/2020/12/05/germany/bundesliga/eintracht-frankfurt/bv-borussia-09-dortmund/3331675/', 'https://uk.soccerway.com//matches/2020/12/05/germany/bundesliga/1-fc-koln/vfl-wolfsburg/3331678/', 'https://uk.soccerway.com//matches/2020/12/05/germany/bundesliga/dsc-arminia-bielefeld/1-fsv-mainz-05/3331680/', 'https://uk.soccerway.com//matches/2020/12/05/germany/bundesliga/fc-bayern-munchen/rb-leipzig/3331672/', 'https://uk.soccerway.com//matches/2020/12/06/germany/bundesliga/sv-werder-bremen/vfb-stuttgart-1893-ev/3331679/', 'https://uk.soccerway.com//matches/2020/12/06/germany/bundesliga/fc-schalke-04/bayer-04-leverkusen/3331677/', 'https://uk.soccerway.com//matches/2020/12/07/germany/bundesliga/tsg-1899-hoffenheim-ev/fc-augsburg/3331673/', 'https://uk.soccerway.com//matches/2020/12/11/germany/bundesliga/vfl-wolfsburg/eintracht-frankfurt/3331685/', 'https://uk.soccerway.com//matches/2020/12/12/germany/bundesliga/bv-borussia-09-dortmund/vfb-stuttgart-1893-ev/3331681/', 'https://uk.soccerway.com//matches/2020/12/12/germany/bundesliga/rb-leipzig/sv-werder-bremen/3331682/', 'https://uk.soccerway.com//matches/2020/12/12/germany/bundesliga/borussia-monchengladbach/hertha-bsc-berlin/3331683/', 'https://uk.soccerway.com//matches/2020/12/12/germany/bundesliga/sc-freiburg/dsc-arminia-bielefeld/3331686/', 'https://uk.soccerway.com//matches/2020/12/12/germany/bundesliga/1-fsv-mainz-05/1-fc-koln/3331688/', 'https://uk.soccerway.com//matches/2020/12/12/germany/bundesliga/1-fc-union-berlin/fc-bayern-munchen/3331687/', 'https://uk.soccerway.com//matches/2020/12/13/germany/bundesliga/fc-augsburg/fc-schalke-04/3331689/', 'https://uk.soccerway.com//matches/2020/12/13/germany/bundesliga/bayer-04-leverkusen/tsg-1899-hoffenheim-ev/3331684/', 'https://uk.soccerway.com//matches/2020/12/15/germany/bundesliga/eintracht-frankfurt/borussia-monchengladbach/3331692/', 'https://uk.soccerway.com//matches/2020/12/15/germany/bundesliga/hertha-bsc-berlin/1-fsv-mainz-05/3331693/', 'https://uk.soccerway.com//matches/2020/12/15/germany/bundesliga/sv-werder-bremen/bv-borussia-09-dortmund/3331696/', 'https://uk.soccerway.com//matches/2020/12/15/germany/bundesliga/vfb-stuttgart-1893-ev/1-fc-union-berlin/3331698/', 'https://uk.soccerway.com//matches/2020/12/16/germany/bundesliga/fc-schalke-04/sc-freiburg/3331694/', 'https://uk.soccerway.com//matches/2020/12/16/germany/bundesliga/fc-bayern-munchen/vfl-wolfsburg/3331690/', 'https://uk.soccerway.com//matches/2020/12/16/germany/bundesliga/tsg-1899-hoffenheim-ev/rb-leipzig/3331691/', 'https://uk.soccerway.com//matches/2020/12/16/germany/bundesliga/1-fc-koln/bayer-04-leverkusen/3331695/', 'https://uk.soccerway.com//matches/2020/12/16/germany/bundesliga/dsc-arminia-bielefeld/fc-augsburg/3331697/', 'https://uk.soccerway.com//matches/2020/12/18/germany/bundesliga/1-fc-union-berlin/bv-borussia-09-dortmund/3331704/', 'https://uk.soccerway.com//matches/2020/12/19/germany/bundesliga/rb-leipzig/1-fc-koln/3331699/', 'https://uk.soccerway.com//matches/2020/12/19/germany/bundesliga/borussia-monchengladbach/tsg-1899-hoffenheim-ev/3331700/', 'https://uk.soccerway.com//matches/2020/12/19/germany/bundesliga/fc-schalke-04/dsc-arminia-bielefeld/3331705/', 'https://uk.soccerway.com//matches/2020/12/19/germany/bundesliga/1-fsv-mainz-05/sv-werder-bremen/3331706/', 'https://uk.soccerway.com//matches/2020/12/19/germany/bundesliga/fc-augsburg/eintracht-frankfurt/3331707/', 'https://uk.soccerway.com//matches/2020/12/19/germany/bundesliga/bayer-04-leverkusen/fc-bayern-munchen/3331701/', 'https://uk.soccerway.com//matches/2020/12/20/germany/bundesliga/sc-freiburg/hertha-bsc-berlin/3331703/', 'https://uk.soccerway.com//matches/2020/12/20/germany/bundesliga/vfl-wolfsburg/vfb-stuttgart-1893-ev/3331702/', 'https://uk.soccerway.com//matches/2021/01/02/germany/bundesliga/tsg-1899-hoffenheim-ev/sc-freiburg/3331710/', 'https://uk.soccerway.com//matches/2021/01/02/germany/bundesliga/eintracht-frankfurt/bayer-04-leverkusen/3331711/', 'https://uk.soccerway.com//matches/2021/01/02/germany/bundesliga/1-fc-koln/fc-augsburg/3331713/', 'https://uk.soccerway.com//matches/2021/01/02/germany/bundesliga/sv-werder-bremen/1-fc-union-berlin/3331714/', 'https://uk.soccerway.com//matches/2021/01/02/germany/bundesliga/dsc-arminia-bielefeld/borussia-monchengladbach/3331715/', 'https://uk.soccerway.com//matches/2021/01/02/germany/bundesliga/hertha-bsc-berlin/fc-schalke-04/3331712/', 'https://uk.soccerway.com//matches/2021/01/02/germany/bundesliga/vfb-stuttgart-1893-ev/rb-leipzig/3331716/', 'https://uk.soccerway.com//matches/2021/01/03/germany/bundesliga/bv-borussia-09-dortmund/vfl-wolfsburg/3331709/', 'https://uk.soccerway.com//matches/2021/01/03/germany/bundesliga/fc-bayern-munchen/1-fsv-mainz-05/3331708/', 'https://uk.soccerway.com//matches/2021/01/08/germany/bundesliga/borussia-monchengladbach/fc-bayern-munchen/3331718/', 'https://uk.soccerway.com//matches/2021/01/09/germany/bundesliga/bayer-04-leverkusen/sv-werder-bremen/3331719/', 'https://uk.soccerway.com//matches/2021/01/09/germany/bundesliga/sc-freiburg/1-fc-koln/3331720/', 'https://uk.soccerway.com//matches/2021/01/09/germany/bundesliga/1-fc-union-berlin/vfl-wolfsburg/3331721/', 'https://uk.soccerway.com//matches/2021/01/09/germany/bundesliga/fc-schalke-04/tsg-1899-hoffenheim-ev/3331722/', 'https://uk.soccerway.com//matches/2021/01/09/germany/bundesliga/1-fsv-mainz-05/eintracht-frankfurt/3331723/', 'https://uk.soccerway.com//matches/2021/01/09/germany/bundesliga/rb-leipzig/bv-borussia-09-dortmund/3331717/', 'https://uk.soccerway.com//matches/2021/01/10/germany/bundesliga/fc-augsburg/vfb-stuttgart-1893-ev/3331724/', 'https://uk.soccerway.com//matches/2021/01/10/germany/bundesliga/dsc-arminia-bielefeld/hertha-bsc-berlin/3331725/', 'https://uk.soccerway.com//matches/2021/01/15/germany/bundesliga/1-fc-union-berlin/bayer-04-leverkusen/3331731/', 'https://uk.soccerway.com//matches/2021/01/16/germany/bundesliga/bv-borussia-09-dortmund/1-fsv-mainz-05/3331727/', 'https://uk.soccerway.com//matches/2021/01/16/germany/bundesliga/tsg-1899-hoffenheim-ev/dsc-arminia-bielefeld/3331728/', 'https://uk.soccerway.com//matches/2021/01/16/germany/bundesliga/vfl-wolfsburg/rb-leipzig/3331729/', 'https://uk.soccerway.com//matches/2021/01/16/germany/bundesliga/1-fc-koln/hertha-bsc-berlin/3331732/', 'https://uk.soccerway.com//matches/2021/01/16/germany/bundesliga/sv-werder-bremen/fc-augsburg/3331733/', 'https://uk.soccerway.com//matches/2021/01/16/germany/bundesliga/vfb-stuttgart-1893-ev/borussia-monchengladbach/3331734/', 'https://uk.soccerway.com//matches/2021/01/17/germany/bundesliga/fc-bayern-munchen/sc-freiburg/3331726/', 'https://uk.soccerway.com//matches/2021/01/17/germany/bundesliga/eintracht-frankfurt/fc-schalke-04/3331730/', 'https://uk.soccerway.com//matches/2021/01/19/germany/bundesliga/borussia-monchengladbach/sv-werder-bremen/3331736/', 'https://uk.soccerway.com//matches/2021/01/19/germany/bundesliga/bayer-04-leverkusen/bv-borussia-09-dortmund/3331737/', 'https://uk.soccerway.com//matches/2021/01/19/germany/bundesliga/hertha-bsc-berlin/tsg-1899-hoffenheim-ev/3331739/', 'https://uk.soccerway.com//matches/2021/01/19/germany/bundesliga/1-fsv-mainz-05/vfl-wolfsburg/3331741/', 'https://uk.soccerway.com//matches/2021/01/20/germany/bundesliga/fc-schalke-04/1-fc-koln/3331740/', 'https://uk.soccerway.com//matches/2021/01/20/germany/bundesliga/rb-leipzig/1-fc-union-berlin/3331735/', 'https://uk.soccerway.com//matches/2021/01/20/germany/bundesliga/sc-freiburg/eintracht-frankfurt/3331738/', 'https://uk.soccerway.com//matches/2021/01/20/germany/bundesliga/fc-augsburg/fc-bayern-munchen/3331742/', 'https://uk.soccerway.com//matches/2021/01/20/germany/bundesliga/dsc-arminia-bielefeld/vfb-stuttgart-1893-ev/3331743/', 'https://uk.soccerway.com//matches/2021/01/22/germany/bundesliga/borussia-monchengladbach/bv-borussia-09-dortmund/3331744/', 'https://uk.soccerway.com//matches/2021/01/23/germany/bundesliga/bayer-04-leverkusen/vfl-wolfsburg/3331745/', 'https://uk.soccerway.com//matches/2021/01/23/germany/bundesliga/sc-freiburg/vfb-stuttgart-1893-ev/3331747/', 'https://uk.soccerway.com//matches/2021/01/23/germany/bundesliga/1-fsv-mainz-05/rb-leipzig/3331750/', 'https://uk.soccerway.com//matches/2021/01/23/germany/bundesliga/fc-augsburg/1-fc-union-berlin/3331751/', 'https://uk.soccerway.com//matches/2021/01/23/germany/bundesliga/dsc-arminia-bielefeld/eintracht-frankfurt/3331752/', 'https://uk.soccerway.com//matches/2021/01/23/germany/bundesliga/hertha-bsc-berlin/sv-werder-bremen/3331748/', 'https://uk.soccerway.com//matches/2021/01/24/germany/bundesliga/fc-schalke-04/fc-bayern-munchen/3331749/', 'https://uk.soccerway.com//matches/2021/01/24/germany/bundesliga/tsg-1899-hoffenheim-ev/1-fc-koln/3331746/', 'https://uk.soccerway.com//matches/2021/01/29/germany/bundesliga/vfb-stuttgart-1893-ev/1-fsv-mainz-05/3331761/', 'https://uk.soccerway.com//matches/2021/01/30/germany/bundesliga/fc-bayern-munchen/tsg-1899-hoffenheim-ev/3331753/', 'https://uk.soccerway.com//matches/2021/01/30/germany/bundesliga/bv-borussia-09-dortmund/fc-augsburg/3331754/', 'https://uk.soccerway.com//matches/2021/01/30/germany/bundesliga/eintracht-frankfurt/hertha-bsc-berlin/3331757/', 'https://uk.soccerway.com//matches/2021/01/30/germany/bundesliga/1-fc-union-berlin/borussia-monchengladbach/3331758/', 'https://uk.soccerway.com//matches/2021/01/30/germany/bundesliga/sv-werder-bremen/fc-schalke-04/3331760/', 'https://uk.soccerway.com//matches/2021/01/30/germany/bundesliga/rb-leipzig/bayer-04-leverkusen/3331755/', 'https://uk.soccerway.com//matches/2021/01/31/germany/bundesliga/1-fc-koln/dsc-arminia-bielefeld/3331759/', 'https://uk.soccerway.com//matches/2021/01/31/germany/bundesliga/vfl-wolfsburg/sc-freiburg/3331756/', 'https://uk.soccerway.com//matches/2021/02/05/germany/bundesliga/hertha-bsc-berlin/fc-bayern-munchen/3331766/', 'https://uk.soccerway.com//matches/2021/02/06/germany/bundesliga/bayer-04-leverkusen/vfb-stuttgart-1893-ev/3331763/', 'https://uk.soccerway.com//matches/2021/02/06/germany/bundesliga/sc-freiburg/bv-borussia-09-dortmund/3331765/', 'https://uk.soccerway.com//matches/2021/02/06/germany/bundesliga/fc-schalke-04/rb-leipzig/3331767/', 'https://uk.soccerway.com//matches/2021/02/06/germany/bundesliga/1-fsv-mainz-05/1-fc-union-berlin/3331768/', 'https://uk.soccerway.com//matches/2021/02/06/germany/bundesliga/fc-augsburg/vfl-wolfsburg/3331769/', 'https://uk.soccerway.com//matches/2021/02/06/germany/bundesliga/borussia-monchengladbach/1-fc-koln/3331762/', 'https://uk.soccerway.com//matches/2021/02/07/germany/bundesliga/tsg-1899-hoffenheim-ev/eintracht-frankfurt/3331764/', 'https://uk.soccerway.com//matches/2021/03/10/germany/bundesliga/dsc-arminia-bielefeld/sv-werder-bremen/3331770/', 'https://uk.soccerway.com//matches/2021/02/12/germany/bundesliga/rb-leipzig/fc-augsburg/3331773/', 'https://uk.soccerway.com//matches/2021/02/13/germany/bundesliga/bv-borussia-09-dortmund/tsg-1899-hoffenheim-ev/3331772/', 'https://uk.soccerway.com//matches/2021/02/13/germany/bundesliga/bayer-04-leverkusen/1-fsv-mainz-05/3331774/', 'https://uk.soccerway.com//matches/2021/02/13/germany/bundesliga/sv-werder-bremen/sc-freiburg/3331778/', 'https://uk.soccerway.com//matches/2021/02/13/germany/bundesliga/vfb-stuttgart-1893-ev/hertha-bsc-berlin/3331779/', 'https://uk.soccerway.com//matches/2021/02/13/germany/bundesliga/1-fc-union-berlin/fc-schalke-04/3331777/', 'https://uk.soccerway.com//matches/2021/02/14/germany/bundesliga/eintracht-frankfurt/1-fc-koln/3331776/', 'https://uk.soccerway.com//matches/2021/02/14/germany/bundesliga/vfl-wolfsburg/borussia-monchengladbach/3331775/', 'https://uk.soccerway.com//matches/2021/02/15/germany/bundesliga/fc-bayern-munchen/dsc-arminia-bielefeld/3331771/', 'https://uk.soccerway.com//matches/2021/02/19/germany/bundesliga/dsc-arminia-bielefeld/vfl-wolfsburg/3331788/', 'https://uk.soccerway.com//matches/2021/02/20/germany/bundesliga/borussia-monchengladbach/1-fsv-mainz-05/3331780/', 'https://uk.soccerway.com//matches/2021/02/20/germany/bundesliga/sc-freiburg/1-fc-union-berlin/3331782/', 'https://uk.soccerway.com//matches/2021/02/20/germany/bundesliga/eintracht-frankfurt/fc-bayern-munchen/3331783/', 'https://uk.soccerway.com//matches/2021/02/20/germany/bundesliga/1-fc-koln/vfb-stuttgart-1893-ev/3331786/', 'https://uk.soccerway.com//matches/2021/02/20/germany/bundesliga/fc-schalke-04/bv-borussia-09-dortmund/3331785/', 'https://uk.soccerway.com//matches/2021/02/21/germany/bundesliga/fc-augsburg/bayer-04-leverkusen/3331787/', 'https://uk.soccerway.com//matches/2021/02/21/germany/bundesliga/hertha-bsc-berlin/rb-leipzig/3331784/', 'https://uk.soccerway.com//matches/2021/02/21/germany/bundesliga/tsg-1899-hoffenheim-ev/sv-werder-bremen/3331781/', 'https://uk.soccerway.com//matches/2021/02/26/germany/bundesliga/sv-werder-bremen/eintracht-frankfurt/3331796/', 'https://uk.soccerway.com//matches/2021/02/27/germany/bundesliga/fc-bayern-munchen/1-fc-koln/3331789/', 'https://uk.soccerway.com//matches/2021/02/27/germany/bundesliga/bv-borussia-09-dortmund/dsc-arminia-bielefeld/3331790/', 'https://uk.soccerway.com//matches/2021/02/27/germany/bundesliga/vfl-wolfsburg/hertha-bsc-berlin/3331793/', 'https://uk.soccerway.com//matches/2021/02/27/germany/bundesliga/vfb-stuttgart-1893-ev/fc-schalke-04/3331797/', 'https://uk.soccerway.com//matches/2021/02/27/germany/bundesliga/rb-leipzig/borussia-monchengladbach/3331791/', 'https://uk.soccerway.com//matches/2021/02/28/germany/bundesliga/1-fc-union-berlin/tsg-1899-hoffenheim-ev/3331794/', 'https://uk.soccerway.com//matches/2021/02/28/germany/bundesliga/1-fsv-mainz-05/fc-augsburg/3331795/', 'https://uk.soccerway.com//matches/2021/02/28/germany/bundesliga/bayer-04-leverkusen/sc-freiburg/3331792/', 'https://uk.soccerway.com//matches/2021/03/05/germany/bundesliga/fc-schalke-04/1-fsv-mainz-05/3331804/', 'https://uk.soccerway.com//matches/2021/03/06/germany/bundesliga/borussia-monchengladbach/bayer-04-leverkusen/3331799/', 'https://uk.soccerway.com//matches/2021/03/06/germany/bundesliga/tsg-1899-hoffenheim-ev/vfl-wolfsburg/3331800/', 'https://uk.soccerway.com//matches/2021/03/06/germany/bundesliga/sc-freiburg/rb-leipzig/3331801/', 'https://uk.soccerway.com//matches/2021/03/06/germany/bundesliga/eintracht-frankfurt/vfb-stuttgart-1893-ev/3331802/', 'https://uk.soccerway.com//matches/2021/03/06/germany/bundesliga/hertha-bsc-berlin/fc-augsburg/3331803/', 'https://uk.soccerway.com//matches/2021/03/06/germany/bundesliga/fc-bayern-munchen/bv-borussia-09-dortmund/3331798/', 'https://uk.soccerway.com//matches/2021/03/07/germany/bundesliga/1-fc-koln/sv-werder-bremen/3331805/', 'https://uk.soccerway.com//matches/2021/03/07/germany/bundesliga/dsc-arminia-bielefeld/1-fc-union-berlin/3331806/', 'https://uk.soccerway.com//matches/2021/03/12/germany/bundesliga/fc-augsburg/borussia-monchengladbach/3331813/', 'https://uk.soccerway.com//matches/2021/03/13/germany/bundesliga/vfl-wolfsburg/fc-schalke-04/3331810/', 'https://uk.soccerway.com//matches/2021/03/13/germany/bundesliga/1-fc-union-berlin/1-fc-koln/3331811/', 'https://uk.soccerway.com//matches/2021/03/13/germany/bundesliga/1-fsv-mainz-05/sc-freiburg/3331812/', 'https://uk.soccerway.com//matches/2021/03/13/germany/bundesliga/sv-werder-bremen/fc-bayern-munchen/3331814/', 'https://uk.soccerway.com//matches/2021/03/13/germany/bundesliga/bv-borussia-09-dortmund/hertha-bsc-berlin/3331807/', 'https://uk.soccerway.com//matches/2021/03/14/germany/bundesliga/bayer-04-leverkusen/dsc-arminia-bielefeld/3331809/', 'https://uk.soccerway.com//matches/2021/03/14/germany/bundesliga/rb-leipzig/eintracht-frankfurt/3331808/', 'https://uk.soccerway.com//matches/2021/03/14/germany/bundesliga/vfb-stuttgart-1893-ev/tsg-1899-hoffenheim-ev/3331815/', 'https://uk.soccerway.com//matches/2021/03/19/germany/bundesliga/dsc-arminia-bielefeld/rb-leipzig/3331824/', 'https://uk.soccerway.com//matches/2021/03/20/germany/bundesliga/fc-bayern-munchen/vfb-stuttgart-1893-ev/3331816/', 'https://uk.soccerway.com//matches/2021/03/20/germany/bundesliga/eintracht-frankfurt/1-fc-union-berlin/3331819/', 'https://uk.soccerway.com//matches/2021/03/20/germany/bundesliga/1-fc-koln/bv-borussia-09-dortmund/3331822/', 'https://uk.soccerway.com//matches/2021/03/20/germany/bundesliga/sv-werder-bremen/vfl-wolfsburg/3331823/', 'https://uk.soccerway.com//matches/2021/03/20/germany/bundesliga/fc-schalke-04/borussia-monchengladbach/3331821/', 'https://uk.soccerway.com//matches/2021/03/21/germany/bundesliga/tsg-1899-hoffenheim-ev/1-fsv-mainz-05/3331817/', 'https://uk.soccerway.com//matches/2021/03/21/germany/bundesliga/hertha-bsc-berlin/bayer-04-leverkusen/3331820/', 'https://uk.soccerway.com//matches/2021/03/21/germany/bundesliga/sc-freiburg/fc-augsburg/3331818/', 'https://uk.soccerway.com//matches/2021/04/03/germany/bundesliga/bv-borussia-09-dortmund/eintracht-frankfurt/3331825/', 'https://uk.soccerway.com//matches/2021/04/03/germany/bundesliga/bayer-04-leverkusen/fc-schalke-04/3331828/', 'https://uk.soccerway.com//matches/2021/04/03/germany/bundesliga/vfl-wolfsburg/1-fc-koln/3331829/', 'https://uk.soccerway.com//matches/2021/04/03/germany/bundesliga/1-fsv-mainz-05/dsc-arminia-bielefeld/3331831/', 'https://uk.soccerway.com//matches/2021/04/03/germany/bundesliga/fc-augsburg/tsg-1899-hoffenheim-ev/3331832/', 'https://uk.soccerway.com//matches/2021/04/03/germany/bundesliga/rb-leipzig/fc-bayern-munchen/3331826/', 'https://uk.soccerway.com//matches/2021/04/03/germany/bundesliga/borussia-monchengladbach/sc-freiburg/3331827/', 'https://uk.soccerway.com//matches/2021/04/04/germany/bundesliga/vfb-stuttgart-1893-ev/sv-werder-bremen/3331833/', 'https://uk.soccerway.com//matches/2021/04/04/germany/bundesliga/1-fc-union-berlin/hertha-bsc-berlin/3331830/', 'https://uk.soccerway.com//matches/2021/04/09/germany/bundesliga/dsc-arminia-bielefeld/sc-freiburg/3331841/', 'https://uk.soccerway.com//matches/2021/04/10/germany/bundesliga/fc-bayern-munchen/1-fc-union-berlin/3331834/', 'https://uk.soccerway.com//matches/2021/04/10/germany/bundesliga/eintracht-frankfurt/vfl-wolfsburg/3331836/', 'https://uk.soccerway.com//matches/2021/04/10/germany/bundesliga/hertha-bsc-berlin/borussia-monchengladbach/3331837/', 'https://uk.soccerway.com//matches/2021/04/10/germany/bundesliga/sv-werder-bremen/rb-leipzig/3331840/', 'https://uk.soccerway.com//matches/2021/04/10/germany/bundesliga/vfb-stuttgart-1893-ev/bv-borussia-09-dortmund/3331842/', 'https://uk.soccerway.com//matches/2021/04/11/germany/bundesliga/fc-schalke-04/fc-augsburg/3331838/', 'https://uk.soccerway.com//matches/2021/04/11/germany/bundesliga/1-fc-koln/1-fsv-mainz-05/3331839/', 'https://uk.soccerway.com//matches/2021/04/12/germany/bundesliga/tsg-1899-hoffenheim-ev/bayer-04-leverkusen/3331835/', 'https://uk.soccerway.com//matches/2021/04/16/germany/bundesliga/rb-leipzig/tsg-1899-hoffenheim-ev/3331844/', 'https://uk.soccerway.com//matches/2021/04/17/germany/bundesliga/borussia-monchengladbach/eintracht-frankfurt/3331845/', 'https://uk.soccerway.com//matches/2021/04/17/germany/bundesliga/vfl-wolfsburg/fc-bayern-munchen/3331847/', 'https://uk.soccerway.com//matches/2021/04/17/germany/bundesliga/sc-freiburg/fc-schalke-04/3331848/', 'https://uk.soccerway.com//matches/2021/04/17/germany/bundesliga/1-fc-union-berlin/vfb-stuttgart-1893-ev/3331849/', 'https://uk.soccerway.com//matches/2021/04/17/germany/bundesliga/fc-augsburg/dsc-arminia-bielefeld/3331851/', 'https://uk.soccerway.com//matches/2021/04/17/germany/bundesliga/bayer-04-leverkusen/1-fc-koln/3331846/', 'https://uk.soccerway.com//matches/2021/04/18/germany/bundesliga/bv-borussia-09-dortmund/sv-werder-bremen/3331843/', 'https://uk.soccerway.com//matches/2021/05/03/germany/bundesliga/1-fsv-mainz-05/hertha-bsc-berlin/3331850/', 'https://uk.soccerway.com//matches/2021/04/20/germany/bundesliga/1-fc-koln/rb-leipzig/3331857/', 'https://uk.soccerway.com//matches/2021/04/20/germany/bundesliga/fc-bayern-munchen/bayer-04-leverkusen/3331852/', 'https://uk.soccerway.com//matches/2021/04/20/germany/bundesliga/eintracht-frankfurt/fc-augsburg/3331855/', 'https://uk.soccerway.com//matches/2021/04/20/germany/bundesliga/dsc-arminia-bielefeld/fc-schalke-04/3331859/', 'https://uk.soccerway.com//matches/2021/04/21/germany/bundesliga/bv-borussia-09-dortmund/1-fc-union-berlin/3331853/', 'https://uk.soccerway.com//matches/2021/04/21/germany/bundesliga/tsg-1899-hoffenheim-ev/borussia-monchengladbach/3331854/', 'https://uk.soccerway.com//matches/2021/04/21/germany/bundesliga/sv-werder-bremen/1-fsv-mainz-05/3331858/', 'https://uk.soccerway.com//matches/2021/04/21/germany/bundesliga/vfb-stuttgart-1893-ev/vfl-wolfsburg/3331860/', 'https://uk.soccerway.com//matches/2021/05/06/germany/bundesliga/hertha-bsc-berlin/sc-freiburg/3331856/', 'https://uk.soccerway.com//matches/2021/04/23/germany/bundesliga/fc-augsburg/1-fc-koln/3331869/', 'https://uk.soccerway.com//matches/2021/04/24/germany/bundesliga/vfl-wolfsburg/bv-borussia-09-dortmund/3331864/', 'https://uk.soccerway.com//matches/2021/04/24/germany/bundesliga/sc-freiburg/tsg-1899-hoffenheim-ev/3331865/', 'https://uk.soccerway.com//matches/2021/04/24/germany/bundesliga/1-fc-union-berlin/sv-werder-bremen/3331866/', 'https://uk.soccerway.com//matches/2021/04/24/germany/bundesliga/1-fsv-mainz-05/fc-bayern-munchen/3331868/', 'https://uk.soccerway.com//matches/2021/04/24/germany/bundesliga/bayer-04-leverkusen/eintracht-frankfurt/3331863/', 'https://uk.soccerway.com//matches/2021/04/25/germany/bundesliga/rb-leipzig/vfb-stuttgart-1893-ev/3331861/', 'https://uk.soccerway.com//matches/2021/04/25/germany/bundesliga/borussia-monchengladbach/dsc-arminia-bielefeld/3331862/', 'https://uk.soccerway.com//matches/2021/05/12/germany/bundesliga/fc-schalke-04/hertha-bsc-berlin/3331867/', 'https://uk.soccerway.com//matches/2021/05/07/germany/bundesliga/vfb-stuttgart-1893-ev/fc-augsburg/3331878/', 'https://uk.soccerway.com//matches/2021/05/08/germany/bundesliga/bv-borussia-09-dortmund/rb-leipzig/3331871/', 'https://uk.soccerway.com//matches/2021/05/08/germany/bundesliga/tsg-1899-hoffenheim-ev/fc-schalke-04/3331872/', 'https://uk.soccerway.com//matches/2021/05/08/germany/bundesliga/vfl-wolfsburg/1-fc-union-berlin/3331873/', 'https://uk.soccerway.com//matches/2021/05/08/germany/bundesliga/sv-werder-bremen/bayer-04-leverkusen/3331877/', 'https://uk.soccerway.com//matches/2021/05/08/germany/bundesliga/fc-bayern-munchen/borussia-monchengladbach/3331870/', 'https://uk.soccerway.com//matches/2021/05/09/germany/bundesliga/1-fc-koln/sc-freiburg/3331876/', 'https://uk.soccerway.com//matches/2021/05/09/germany/bundesliga/eintracht-frankfurt/1-fsv-mainz-05/3331874/', 'https://uk.soccerway.com//matches/2021/05/09/germany/bundesliga/hertha-bsc-berlin/dsc-arminia-bielefeld/3331875/', 'https://uk.soccerway.com//matches/2021/05/15/germany/bundesliga/borussia-monchengladbach/vfb-stuttgart-1893-ev/3331880/', 'https://uk.soccerway.com//matches/2021/05/15/germany/bundesliga/bayer-04-leverkusen/1-fc-union-berlin/3331881/', 'https://uk.soccerway.com//matches/2021/05/15/germany/bundesliga/sc-freiburg/fc-bayern-munchen/3331882/', 'https://uk.soccerway.com//matches/2021/05/15/germany/bundesliga/hertha-bsc-berlin/1-fc-koln/3331883/', 'https://uk.soccerway.com//matches/2021/05/15/germany/bundesliga/fc-schalke-04/eintracht-frankfurt/3331884/', 'https://uk.soccerway.com//matches/2021/05/15/germany/bundesliga/fc-augsburg/sv-werder-bremen/3331886/', 'https://uk.soccerway.com//matches/2021/05/15/germany/bundesliga/dsc-arminia-bielefeld/tsg-1899-hoffenheim-ev/3331887/', 'https://uk.soccerway.com//matches/2021/05/16/germany/bundesliga/1-fsv-mainz-05/bv-borussia-09-dortmund/3331885/', 'https://uk.soccerway.com//matches/2021/05/16/germany/bundesliga/rb-leipzig/vfl-wolfsburg/3331879/']
    print(len(links))
    scraper = SWLineupScraper(address, 'D1', '20202021', links)
    scraper.runner(links)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
