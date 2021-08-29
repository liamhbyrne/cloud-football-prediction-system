import logging
import random
import re
import time
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

if __name__ == '__main__':
    # TIMER START
    start = time.time()

    s = SWLineupScraper("postgres://lhb:WashingMachine065@34.74.68.51/football",
                        "B1", "1112")
    s.runner(["https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/club-brugge-kv/koninklijke-lierse-sportkring/1123108/","https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/royal-sporting-club-anderlecht/sv-zulte-waregem/1123109/","https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/krc-genk/koninklijke-atletiek-associatie-gent/1123110/","https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/fc-oud-heverlee-leuven/sint-truidense-vv/1123111/","https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/sporting-lokeren-oost-vlaanderen/cercle-brugge-ksv/1123112/","https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/kv-kortrijk/kfc-germinal-beerschot-antwerp-nv/1123113/","https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/kv-mechelen/standard-de-liege/1123114/","https://uk.soccerway.com/matches/2012/03/21/belgium/pro-league/raec-mons/kvc-westerlo/1123115/","https://uk.soccerway.com/matches/2011/07/29/belgium/pro-league/fc-oud-heverlee-leuven/royal-sporting-club-anderlecht/1122878/","https://uk.soccerway.com/matches/2011/07/30/belgium/pro-league/raec-mons/standard-de-liege/1122882/","https://uk.soccerway.com/matches/2011/07/30/belgium/pro-league/krc-genk/kfc-germinal-beerschot-antwerp-nv/1122876/","https://uk.soccerway.com/matches/2011/07/30/belgium/pro-league/koninklijke-atletiek-associatie-gent/cercle-brugge-ksv/1122877/","https://uk.soccerway.com/matches/2011/07/30/belgium/pro-league/sporting-lokeren-oost-vlaanderen/sv-zulte-waregem/1122879/","https://uk.soccerway.com/matches/2011/07/30/belgium/pro-league/kv-kortrijk/koninklijke-lierse-sportkring/1122880/","https://uk.soccerway.com/matches/2011/07/30/belgium/pro-league/kv-mechelen/sint-truidense-vv/1122881/","https://uk.soccerway.com/matches/2011/07/31/belgium/pro-league/club-brugge-kv/kvc-westerlo/1122883/","https://uk.soccerway.com/matches/2011/08/05/belgium/pro-league/royal-sporting-club-anderlecht/kv-mechelen/1122884/","https://uk.soccerway.com/matches/2011/08/06/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/koninklijke-atletiek-associatie-gent/1122886/","https://uk.soccerway.com/matches/2011/08/06/belgium/pro-league/sv-zulte-waregem/fc-oud-heverlee-leuven/1122888/","https://uk.soccerway.com/matches/2011/08/06/belgium/pro-league/koninklijke-lierse-sportkring/krc-genk/1122889/","https://uk.soccerway.com/matches/2011/08/06/belgium/pro-league/cercle-brugge-ksv/raec-mons/1122891/","https://uk.soccerway.com/matches/2011/08/07/belgium/pro-league/standard-de-liege/sporting-lokeren-oost-vlaanderen/1122885/","https://uk.soccerway.com/matches/2011/08/07/belgium/pro-league/sint-truidense-vv/club-brugge-kv/1122887/","https://uk.soccerway.com/matches/2011/08/07/belgium/pro-league/kvc-westerlo/kv-kortrijk/1122890/","https://uk.soccerway.com/matches/2011/08/13/belgium/pro-league/koninklijke-atletiek-associatie-gent/standard-de-liege/1122894/","https://uk.soccerway.com/matches/2011/08/13/belgium/pro-league/club-brugge-kv/fc-oud-heverlee-leuven/1122892/","https://uk.soccerway.com/matches/2011/08/13/belgium/pro-league/krc-genk/sv-zulte-waregem/1122893/","https://uk.soccerway.com/matches/2011/08/13/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/kvc-westerlo/1122895/","https://uk.soccerway.com/matches/2011/08/13/belgium/pro-league/kv-kortrijk/cercle-brugge-ksv/1122897/","https://uk.soccerway.com/matches/2011/08/13/belgium/pro-league/raec-mons/sint-truidense-vv/1122899/","https://uk.soccerway.com/matches/2011/08/14/belgium/pro-league/sporting-lokeren-oost-vlaanderen/royal-sporting-club-anderlecht/1122896/","https://uk.soccerway.com/matches/2011/08/14/belgium/pro-league/kv-mechelen/koninklijke-lierse-sportkring/1122898/","https://uk.soccerway.com/matches/2011/08/19/belgium/pro-league/koninklijke-lierse-sportkring/kfc-germinal-beerschot-antwerp-nv/1122904/","https://uk.soccerway.com/matches/2011/08/20/belgium/pro-league/sint-truidense-vv/sporting-lokeren-oost-vlaanderen/1122901/","https://uk.soccerway.com/matches/2011/08/20/belgium/pro-league/fc-oud-heverlee-leuven/kv-mechelen/1122903/","https://uk.soccerway.com/matches/2011/08/20/belgium/pro-league/kvc-westerlo/koninklijke-atletiek-associatie-gent/1122905/","https://uk.soccerway.com/matches/2011/08/20/belgium/pro-league/cercle-brugge-ksv/krc-genk/1122906/","https://uk.soccerway.com/matches/2011/08/21/belgium/pro-league/sv-zulte-waregem/club-brugge-kv/1122902/","https://uk.soccerway.com/matches/2011/08/21/belgium/pro-league/royal-sporting-club-anderlecht/raec-mons/1122907/","https://uk.soccerway.com/matches/2011/08/21/belgium/pro-league/standard-de-liege/kv-kortrijk/1122900/","https://uk.soccerway.com/matches/2011/08/27/belgium/pro-league/krc-genk/kv-mechelen/1122909/","https://uk.soccerway.com/matches/2011/08/27/belgium/pro-league/koninklijke-atletiek-associatie-gent/koninklijke-lierse-sportkring/1122910/","https://uk.soccerway.com/matches/2011/08/27/belgium/pro-league/sporting-lokeren-oost-vlaanderen/fc-oud-heverlee-leuven/1122912/","https://uk.soccerway.com/matches/2011/08/27/belgium/pro-league/kv-kortrijk/sint-truidense-vv/1122913/","https://uk.soccerway.com/matches/2011/08/27/belgium/pro-league/raec-mons/sv-zulte-waregem/1122914/","https://uk.soccerway.com/matches/2011/08/27/belgium/pro-league/kvc-westerlo/cercle-brugge-ksv/1122915/","https://uk.soccerway.com/matches/2011/08/28/belgium/pro-league/club-brugge-kv/royal-sporting-club-anderlecht/1122908/","https://uk.soccerway.com/matches/2011/08/28/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/standard-de-liege/1122911/","https://uk.soccerway.com/matches/2011/09/09/belgium/pro-league/sint-truidense-vv/krc-genk/1122918/","https://uk.soccerway.com/matches/2011/09/10/belgium/pro-league/standard-de-liege/kvc-westerlo/1122917/","https://uk.soccerway.com/matches/2011/09/10/belgium/pro-league/fc-oud-heverlee-leuven/raec-mons/1122920/","https://uk.soccerway.com/matches/2011/09/10/belgium/pro-league/koninklijke-lierse-sportkring/sporting-lokeren-oost-vlaanderen/1122922/","https://uk.soccerway.com/matches/2011/09/10/belgium/pro-league/cercle-brugge-ksv/kfc-germinal-beerschot-antwerp-nv/1122923/","https://uk.soccerway.com/matches/2011/09/11/belgium/pro-league/royal-sporting-club-anderlecht/kv-kortrijk/1122916/","https://uk.soccerway.com/matches/2011/09/11/belgium/pro-league/kv-mechelen/club-brugge-kv/1122921/","https://uk.soccerway.com/matches/2011/09/11/belgium/pro-league/sv-zulte-waregem/koninklijke-atletiek-associatie-gent/1122919/","https://uk.soccerway.com/matches/2011/09/17/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/sv-zulte-waregem/1122926/","https://uk.soccerway.com/matches/2011/09/17/belgium/pro-league/kv-kortrijk/fc-oud-heverlee-leuven/1122928/","https://uk.soccerway.com/matches/2011/09/17/belgium/pro-league/raec-mons/kv-mechelen/1122929/","https://uk.soccerway.com/matches/2011/09/17/belgium/pro-league/kvc-westerlo/sint-truidense-vv/1122930/","https://uk.soccerway.com/matches/2011/09/17/belgium/pro-league/cercle-brugge-ksv/koninklijke-lierse-sportkring/1122931/","https://uk.soccerway.com/matches/2011/09/18/belgium/pro-league/koninklijke-atletiek-associatie-gent/royal-sporting-club-anderlecht/1122925/","https://uk.soccerway.com/matches/2011/09/18/belgium/pro-league/krc-genk/standard-de-liege/1122924/","https://uk.soccerway.com/matches/2011/09/18/belgium/pro-league/sporting-lokeren-oost-vlaanderen/club-brugge-kv/1122927/","https://uk.soccerway.com/matches/2011/09/24/belgium/pro-league/kv-mechelen/sporting-lokeren-oost-vlaanderen/1122938/","https://uk.soccerway.com/matches/2011/09/24/belgium/pro-league/club-brugge-kv/raec-mons/1122932/","https://uk.soccerway.com/matches/2011/09/24/belgium/pro-league/sint-truidense-vv/koninklijke-atletiek-associatie-gent/1122935/","https://uk.soccerway.com/matches/2011/09/24/belgium/pro-league/sv-zulte-waregem/kv-kortrijk/1122936/","https://uk.soccerway.com/matches/2011/09/24/belgium/pro-league/fc-oud-heverlee-leuven/krc-genk/1122937/","https://uk.soccerway.com/matches/2011/09/24/belgium/pro-league/koninklijke-lierse-sportkring/kvc-westerlo/1122939/","https://uk.soccerway.com/matches/2011/09/25/belgium/pro-league/royal-sporting-club-anderlecht/kfc-germinal-beerschot-antwerp-nv/1122933/","https://uk.soccerway.com/matches/2011/09/25/belgium/pro-league/standard-de-liege/cercle-brugge-ksv/1122934/","https://uk.soccerway.com/matches/2011/10/01/belgium/pro-league/koninklijke-atletiek-associatie-gent/fc-oud-heverlee-leuven/1122942/","https://uk.soccerway.com/matches/2011/10/01/belgium/pro-league/kv-kortrijk/kv-mechelen/1122944/","https://uk.soccerway.com/matches/2011/10/01/belgium/pro-league/raec-mons/sporting-lokeren-oost-vlaanderen/1122945/","https://uk.soccerway.com/matches/2011/10/01/belgium/pro-league/kvc-westerlo/sv-zulte-waregem/1122946/","https://uk.soccerway.com/matches/2011/10/01/belgium/pro-league/cercle-brugge-ksv/sint-truidense-vv/1122947/","https://uk.soccerway.com/matches/2011/10/02/belgium/pro-league/standard-de-liege/koninklijke-lierse-sportkring/1122940/","https://uk.soccerway.com/matches/2011/10/02/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/club-brugge-kv/1122943/","https://uk.soccerway.com/matches/2011/10/02/belgium/pro-league/krc-genk/royal-sporting-club-anderlecht/1122941/","https://uk.soccerway.com/matches/2011/10/15/belgium/pro-league/sporting-lokeren-oost-vlaanderen/krc-genk/1122953/","https://uk.soccerway.com/matches/2011/10/15/belgium/pro-league/sint-truidense-vv/kfc-germinal-beerschot-antwerp-nv/1122950/","https://uk.soccerway.com/matches/2011/10/15/belgium/pro-league/sv-zulte-waregem/koninklijke-lierse-sportkring/1122951/","https://uk.soccerway.com/matches/2011/10/15/belgium/pro-league/fc-oud-heverlee-leuven/kvc-westerlo/1122952/","https://uk.soccerway.com/matches/2011/10/15/belgium/pro-league/kv-mechelen/cercle-brugge-ksv/1122954/","https://uk.soccerway.com/matches/2011/10/15/belgium/pro-league/raec-mons/kv-kortrijk/1122955/","https://uk.soccerway.com/matches/2011/10/16/belgium/pro-league/club-brugge-kv/koninklijke-atletiek-associatie-gent/1122948/","https://uk.soccerway.com/matches/2011/10/16/belgium/pro-league/royal-sporting-club-anderlecht/standard-de-liege/1122949/","https://uk.soccerway.com/matches/2011/10/22/belgium/pro-league/koninklijke-atletiek-associatie-gent/sporting-lokeren-oost-vlaanderen/1122958/","https://uk.soccerway.com/matches/2011/10/22/belgium/pro-league/krc-genk/raec-mons/1122957/","https://uk.soccerway.com/matches/2011/10/22/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/kv-mechelen/1122959/","https://uk.soccerway.com/matches/2011/10/22/belgium/pro-league/koninklijke-lierse-sportkring/sint-truidense-vv/1122961/","https://uk.soccerway.com/matches/2011/10/22/belgium/pro-league/cercle-brugge-ksv/fc-oud-heverlee-leuven/1122963/","https://uk.soccerway.com/matches/2011/10/23/belgium/pro-league/kvc-westerlo/royal-sporting-club-anderlecht/1122962/","https://uk.soccerway.com/matches/2011/10/23/belgium/pro-league/kv-kortrijk/club-brugge-kv/1122960/","https://uk.soccerway.com/matches/2011/10/23/belgium/pro-league/standard-de-liege/sv-zulte-waregem/1122956/","https://uk.soccerway.com/matches/2011/10/29/belgium/pro-league/club-brugge-kv/krc-genk/1122964/","https://uk.soccerway.com/matches/2011/10/29/belgium/pro-league/sv-zulte-waregem/cercle-brugge-ksv/1122967/","https://uk.soccerway.com/matches/2011/10/29/belgium/pro-league/fc-oud-heverlee-leuven/kfc-germinal-beerschot-antwerp-nv/1122968/","https://uk.soccerway.com/matches/2011/10/29/belgium/pro-league/sporting-lokeren-oost-vlaanderen/kv-kortrijk/1122969/","https://uk.soccerway.com/matches/2011/10/29/belgium/pro-league/kv-mechelen/kvc-westerlo/1122970/","https://uk.soccerway.com/matches/2011/10/29/belgium/pro-league/raec-mons/koninklijke-atletiek-associatie-gent/1122971/","https://uk.soccerway.com/matches/2011/10/30/belgium/pro-league/royal-sporting-club-anderlecht/koninklijke-lierse-sportkring/1122965/","https://uk.soccerway.com/matches/2011/10/30/belgium/pro-league/sint-truidense-vv/standard-de-liege/1122966/","https://uk.soccerway.com/matches/2011/11/04/belgium/pro-league/koninklijke-atletiek-associatie-gent/kv-mechelen/1122974/","https://uk.soccerway.com/matches/2011/11/05/belgium/pro-league/krc-genk/kv-kortrijk/1122973/","https://uk.soccerway.com/matches/2011/11/05/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/raec-mons/1122975/","https://uk.soccerway.com/matches/2011/11/05/belgium/pro-league/sv-zulte-waregem/sint-truidense-vv/1122976/","https://uk.soccerway.com/matches/2011/11/05/belgium/pro-league/koninklijke-lierse-sportkring/fc-oud-heverlee-leuven/1122977/","https://uk.soccerway.com/matches/2011/11/05/belgium/pro-league/kvc-westerlo/sporting-lokeren-oost-vlaanderen/1122978/","https://uk.soccerway.com/matches/2011/11/06/belgium/pro-league/cercle-brugge-ksv/royal-sporting-club-anderlecht/1122979/","https://uk.soccerway.com/matches/2011/11/06/belgium/pro-league/standard-de-liege/club-brugge-kv/1122972/","https://uk.soccerway.com/matches/2011/11/18/belgium/pro-league/fc-oud-heverlee-leuven/standard-de-liege/1122983/","https://uk.soccerway.com/matches/2011/11/19/belgium/pro-league/krc-genk/kvc-westerlo/1122982/","https://uk.soccerway.com/matches/2011/11/19/belgium/pro-league/sporting-lokeren-oost-vlaanderen/kfc-germinal-beerschot-antwerp-nv/1122984/","https://uk.soccerway.com/matches/2011/11/19/belgium/pro-league/kv-kortrijk/koninklijke-atletiek-associatie-gent/1122985/","https://uk.soccerway.com/matches/2011/11/19/belgium/pro-league/kv-mechelen/sv-zulte-waregem/1122986/","https://uk.soccerway.com/matches/2011/11/19/belgium/pro-league/raec-mons/koninklijke-lierse-sportkring/1122987/","https://uk.soccerway.com/matches/2011/11/20/belgium/pro-league/royal-sporting-club-anderlecht/sint-truidense-vv/1122981/","https://uk.soccerway.com/matches/2011/11/20/belgium/pro-league/club-brugge-kv/cercle-brugge-ksv/1122980/","https://uk.soccerway.com/matches/2011/11/25/belgium/pro-league/standard-de-liege/kv-mechelen/1122988/","https://uk.soccerway.com/matches/2011/11/26/belgium/pro-league/sv-zulte-waregem/royal-sporting-club-anderlecht/1122992/","https://uk.soccerway.com/matches/2011/11/26/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/kv-kortrijk/1122990/","https://uk.soccerway.com/matches/2011/11/26/belgium/pro-league/sint-truidense-vv/fc-oud-heverlee-leuven/1122991/","https://uk.soccerway.com/matches/2011/11/26/belgium/pro-league/koninklijke-lierse-sportkring/club-brugge-kv/1122993/","https://uk.soccerway.com/matches/2011/11/26/belgium/pro-league/kvc-westerlo/raec-mons/1122994/","https://uk.soccerway.com/matches/2011/11/26/belgium/pro-league/cercle-brugge-ksv/sporting-lokeren-oost-vlaanderen/1122995/","https://uk.soccerway.com/matches/2011/11/27/belgium/pro-league/koninklijke-atletiek-associatie-gent/krc-genk/1122989/","https://uk.soccerway.com/matches/2011/12/02/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/krc-genk/1122998/","https://uk.soccerway.com/matches/2011/12/03/belgium/pro-league/sint-truidense-vv/raec-mons/1122999/","https://uk.soccerway.com/matches/2011/12/03/belgium/pro-league/sv-zulte-waregem/sporting-lokeren-oost-vlaanderen/1123000/","https://uk.soccerway.com/matches/2011/12/03/belgium/pro-league/koninklijke-lierse-sportkring/kv-mechelen/1123001/","https://uk.soccerway.com/matches/2011/12/03/belgium/pro-league/kvc-westerlo/club-brugge-kv/1123002/","https://uk.soccerway.com/matches/2011/12/03/belgium/pro-league/cercle-brugge-ksv/kv-kortrijk/1123003/","https://uk.soccerway.com/matches/2011/12/04/belgium/pro-league/royal-sporting-club-anderlecht/fc-oud-heverlee-leuven/1122996/","https://uk.soccerway.com/matches/2011/12/04/belgium/pro-league/standard-de-liege/koninklijke-atletiek-associatie-gent/1122997/","https://uk.soccerway.com/matches/2011/12/09/belgium/pro-league/kv-mechelen/royal-sporting-club-anderlecht/1123010/","https://uk.soccerway.com/matches/2011/12/10/belgium/pro-league/sporting-lokeren-oost-vlaanderen/standard-de-liege/1123008/","https://uk.soccerway.com/matches/2011/12/10/belgium/pro-league/krc-genk/koninklijke-lierse-sportkring/1123005/","https://uk.soccerway.com/matches/2011/12/10/belgium/pro-league/koninklijke-atletiek-associatie-gent/kfc-germinal-beerschot-antwerp-nv/1123006/","https://uk.soccerway.com/matches/2011/12/10/belgium/pro-league/fc-oud-heverlee-leuven/sv-zulte-waregem/1123007/","https://uk.soccerway.com/matches/2011/12/10/belgium/pro-league/kv-kortrijk/kvc-westerlo/1123009/","https://uk.soccerway.com/matches/2011/12/10/belgium/pro-league/raec-mons/cercle-brugge-ksv/1123011/","https://uk.soccerway.com/matches/2011/12/11/belgium/pro-league/club-brugge-kv/sint-truidense-vv/1123004/","https://uk.soccerway.com/matches/2011/12/17/belgium/pro-league/sint-truidense-vv/kv-mechelen/1123014/","https://uk.soccerway.com/matches/2011/12/17/belgium/pro-league/sv-zulte-waregem/raec-mons/1123015/","https://uk.soccerway.com/matches/2011/12/17/belgium/pro-league/koninklijke-lierse-sportkring/kv-kortrijk/1123017/","https://uk.soccerway.com/matches/2011/12/17/belgium/pro-league/kvc-westerlo/kfc-germinal-beerschot-antwerp-nv/1123018/","https://uk.soccerway.com/matches/2011/12/18/belgium/pro-league/standard-de-liege/krc-genk/1123013/","https://uk.soccerway.com/matches/2011/12/18/belgium/pro-league/royal-sporting-club-anderlecht/sporting-lokeren-oost-vlaanderen/1123012/","https://uk.soccerway.com/matches/2011/12/18/belgium/pro-league/fc-oud-heverlee-leuven/club-brugge-kv/1123016/","https://uk.soccerway.com/matches/2011/12/18/belgium/pro-league/cercle-brugge-ksv/koninklijke-atletiek-associatie-gent/1123019/","https://uk.soccerway.com/matches/2011/12/26/belgium/pro-league/club-brugge-kv/sv-zulte-waregem/1123020/","https://uk.soccerway.com/matches/2011/12/26/belgium/pro-league/kv-kortrijk/standard-de-liege/1123025/","https://uk.soccerway.com/matches/2011/12/26/belgium/pro-league/krc-genk/cercle-brugge-ksv/1123021/","https://uk.soccerway.com/matches/2011/12/26/belgium/pro-league/koninklijke-atletiek-associatie-gent/kvc-westerlo/1123022/","https://uk.soccerway.com/matches/2011/12/26/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/koninklijke-lierse-sportkring/1123023/","https://uk.soccerway.com/matches/2011/12/26/belgium/pro-league/sporting-lokeren-oost-vlaanderen/sint-truidense-vv/1123024/","https://uk.soccerway.com/matches/2011/12/26/belgium/pro-league/kv-mechelen/fc-oud-heverlee-leuven/1123026/","https://uk.soccerway.com/matches/2011/12/27/belgium/pro-league/raec-mons/royal-sporting-club-anderlecht/1123027/","https://uk.soccerway.com/matches/2012/01/14/belgium/pro-league/standard-de-liege/kfc-germinal-beerschot-antwerp-nv/1123029/","https://uk.soccerway.com/matches/2012/01/14/belgium/pro-league/sint-truidense-vv/kv-kortrijk/1123030/","https://uk.soccerway.com/matches/2012/01/14/belgium/pro-league/fc-oud-heverlee-leuven/sporting-lokeren-oost-vlaanderen/1123032/","https://uk.soccerway.com/matches/2012/01/14/belgium/pro-league/kv-mechelen/raec-mons/1123033/","https://uk.soccerway.com/matches/2012/01/14/belgium/pro-league/koninklijke-lierse-sportkring/koninklijke-atletiek-associatie-gent/1123034/","https://uk.soccerway.com/matches/2012/01/14/belgium/pro-league/cercle-brugge-ksv/kvc-westerlo/1123035/","https://uk.soccerway.com/matches/2012/01/15/belgium/pro-league/royal-sporting-club-anderlecht/club-brugge-kv/1123028/","https://uk.soccerway.com/matches/2012/01/15/belgium/pro-league/sv-zulte-waregem/krc-genk/1123031/","https://uk.soccerway.com/matches/2012/01/20/belgium/pro-league/krc-genk/sint-truidense-vv/1123037/","https://uk.soccerway.com/matches/2012/01/21/belgium/pro-league/kv-kortrijk/royal-sporting-club-anderlecht/1123041/","https://uk.soccerway.com/matches/2012/01/21/belgium/pro-league/club-brugge-kv/kv-mechelen/1123036/","https://uk.soccerway.com/matches/2012/01/21/belgium/pro-league/koninklijke-atletiek-associatie-gent/sv-zulte-waregem/1123038/","https://uk.soccerway.com/matches/2012/01/21/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/cercle-brugge-ksv/1123039/","https://uk.soccerway.com/matches/2012/01/21/belgium/pro-league/sporting-lokeren-oost-vlaanderen/koninklijke-lierse-sportkring/1123040/","https://uk.soccerway.com/matches/2012/01/21/belgium/pro-league/raec-mons/fc-oud-heverlee-leuven/1123042/","https://uk.soccerway.com/matches/2012/01/22/belgium/pro-league/kvc-westerlo/standard-de-liege/1123043/","https://uk.soccerway.com/matches/2012/01/24/belgium/pro-league/royal-sporting-club-anderlecht/koninklijke-atletiek-associatie-gent/1123045/","https://uk.soccerway.com/matches/2012/01/25/belgium/pro-league/club-brugge-kv/sporting-lokeren-oost-vlaanderen/1123044/","https://uk.soccerway.com/matches/2012/01/25/belgium/pro-league/sint-truidense-vv/kvc-westerlo/1123047/","https://uk.soccerway.com/matches/2012/01/25/belgium/pro-league/sv-zulte-waregem/kfc-germinal-beerschot-antwerp-nv/1123048/","https://uk.soccerway.com/matches/2012/01/25/belgium/pro-league/fc-oud-heverlee-leuven/kv-kortrijk/1123049/","https://uk.soccerway.com/matches/2012/01/25/belgium/pro-league/kv-mechelen/krc-genk/1123050/","https://uk.soccerway.com/matches/2012/01/25/belgium/pro-league/koninklijke-lierse-sportkring/cercle-brugge-ksv/1123051/","https://uk.soccerway.com/matches/2012/01/26/belgium/pro-league/standard-de-liege/raec-mons/1123046/","https://uk.soccerway.com/matches/2012/01/28/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/royal-sporting-club-anderlecht/1123054/","https://uk.soccerway.com/matches/2012/01/28/belgium/pro-league/krc-genk/fc-oud-heverlee-leuven/1123052/","https://uk.soccerway.com/matches/2012/01/28/belgium/pro-league/koninklijke-atletiek-associatie-gent/sint-truidense-vv/1123053/","https://uk.soccerway.com/matches/2012/01/28/belgium/pro-league/sporting-lokeren-oost-vlaanderen/kv-mechelen/1123055/","https://uk.soccerway.com/matches/2012/01/28/belgium/pro-league/kv-kortrijk/sv-zulte-waregem/1123056/","https://uk.soccerway.com/matches/2012/01/28/belgium/pro-league/kvc-westerlo/koninklijke-lierse-sportkring/1123058/","https://uk.soccerway.com/matches/2012/01/29/belgium/pro-league/cercle-brugge-ksv/standard-de-liege/1123059/","https://uk.soccerway.com/matches/2012/01/29/belgium/pro-league/raec-mons/club-brugge-kv/1123057/","https://uk.soccerway.com/matches/2012/02/04/belgium/pro-league/koninklijke-lierse-sportkring/standard-de-liege/1123067/","https://uk.soccerway.com/matches/2012/02/04/belgium/pro-league/sint-truidense-vv/cercle-brugge-ksv/1123062/","https://uk.soccerway.com/matches/2012/02/04/belgium/pro-league/sv-zulte-waregem/kvc-westerlo/1123063/","https://uk.soccerway.com/matches/2012/02/04/belgium/pro-league/fc-oud-heverlee-leuven/koninklijke-atletiek-associatie-gent/1123064/","https://uk.soccerway.com/matches/2012/02/04/belgium/pro-league/sporting-lokeren-oost-vlaanderen/raec-mons/1123065/","https://uk.soccerway.com/matches/2012/02/04/belgium/pro-league/kv-mechelen/kv-kortrijk/1123066/","https://uk.soccerway.com/matches/2012/02/05/belgium/pro-league/royal-sporting-club-anderlecht/krc-genk/1123061/","https://uk.soccerway.com/matches/2012/02/05/belgium/pro-league/club-brugge-kv/kfc-germinal-beerschot-antwerp-nv/1123060/","https://uk.soccerway.com/matches/2012/02/11/belgium/pro-league/krc-genk/sporting-lokeren-oost-vlaanderen/1123069/","https://uk.soccerway.com/matches/2012/02/11/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/sint-truidense-vv/1123071/","https://uk.soccerway.com/matches/2012/02/11/belgium/pro-league/kv-kortrijk/raec-mons/1123072/","https://uk.soccerway.com/matches/2012/02/11/belgium/pro-league/koninklijke-lierse-sportkring/sv-zulte-waregem/1123073/","https://uk.soccerway.com/matches/2012/02/11/belgium/pro-league/kvc-westerlo/fc-oud-heverlee-leuven/1123074/","https://uk.soccerway.com/matches/2012/02/11/belgium/pro-league/cercle-brugge-ksv/kv-mechelen/1123075/","https://uk.soccerway.com/matches/2012/02/12/belgium/pro-league/standard-de-liege/royal-sporting-club-anderlecht/1123068/","https://uk.soccerway.com/matches/2012/02/12/belgium/pro-league/koninklijke-atletiek-associatie-gent/club-brugge-kv/1123070/","https://uk.soccerway.com/matches/2012/02/18/belgium/pro-league/sporting-lokeren-oost-vlaanderen/koninklijke-atletiek-associatie-gent/1123081/","https://uk.soccerway.com/matches/2012/02/18/belgium/pro-league/sint-truidense-vv/koninklijke-lierse-sportkring/1123078/","https://uk.soccerway.com/matches/2012/02/18/belgium/pro-league/fc-oud-heverlee-leuven/cercle-brugge-ksv/1123080/","https://uk.soccerway.com/matches/2012/02/18/belgium/pro-league/kv-mechelen/kfc-germinal-beerschot-antwerp-nv/1123082/","https://uk.soccerway.com/matches/2012/02/18/belgium/pro-league/raec-mons/krc-genk/1123083/","https://uk.soccerway.com/matches/2012/02/19/belgium/pro-league/royal-sporting-club-anderlecht/kvc-westerlo/1123077/","https://uk.soccerway.com/matches/2012/02/19/belgium/pro-league/club-brugge-kv/kv-kortrijk/1123076/","https://uk.soccerway.com/matches/2012/02/19/belgium/pro-league/sv-zulte-waregem/standard-de-liege/1123079/","https://uk.soccerway.com/matches/2012/02/25/belgium/pro-league/koninklijke-atletiek-associatie-gent/raec-mons/1123086/","https://uk.soccerway.com/matches/2012/02/25/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/fc-oud-heverlee-leuven/1123087/","https://uk.soccerway.com/matches/2012/02/25/belgium/pro-league/kv-kortrijk/sporting-lokeren-oost-vlaanderen/1123088/","https://uk.soccerway.com/matches/2012/02/25/belgium/pro-league/kvc-westerlo/kv-mechelen/1123090/","https://uk.soccerway.com/matches/2012/02/25/belgium/pro-league/cercle-brugge-ksv/sv-zulte-waregem/1123091/","https://uk.soccerway.com/matches/2012/02/26/belgium/pro-league/standard-de-liege/sint-truidense-vv/1123084/","https://uk.soccerway.com/matches/2012/02/26/belgium/pro-league/krc-genk/club-brugge-kv/1123085/","https://uk.soccerway.com/matches/2012/02/26/belgium/pro-league/koninklijke-lierse-sportkring/royal-sporting-club-anderlecht/1123089/","https://uk.soccerway.com/matches/2012/03/03/belgium/pro-league/kv-mechelen/koninklijke-atletiek-associatie-gent/1123098/","https://uk.soccerway.com/matches/2012/03/03/belgium/pro-league/sint-truidense-vv/sv-zulte-waregem/1123094/","https://uk.soccerway.com/matches/2012/03/03/belgium/pro-league/fc-oud-heverlee-leuven/koninklijke-lierse-sportkring/1123095/","https://uk.soccerway.com/matches/2012/03/03/belgium/pro-league/sporting-lokeren-oost-vlaanderen/kvc-westerlo/1123096/","https://uk.soccerway.com/matches/2012/03/03/belgium/pro-league/kv-kortrijk/krc-genk/1123097/","https://uk.soccerway.com/matches/2012/03/03/belgium/pro-league/raec-mons/kfc-germinal-beerschot-antwerp-nv/1123099/","https://uk.soccerway.com/matches/2012/03/04/belgium/pro-league/club-brugge-kv/standard-de-liege/1123092/","https://uk.soccerway.com/matches/2012/03/04/belgium/pro-league/royal-sporting-club-anderlecht/cercle-brugge-ksv/1123093/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/standard-de-liege/fc-oud-heverlee-leuven/1123100/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/koninklijke-atletiek-associatie-gent/kv-kortrijk/1123101/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/kfc-germinal-beerschot-antwerp-nv/sporting-lokeren-oost-vlaanderen/1123102/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/sint-truidense-vv/royal-sporting-club-anderlecht/1123103/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/sv-zulte-waregem/kv-mechelen/1123104/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/koninklijke-lierse-sportkring/raec-mons/1123105/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/kvc-westerlo/krc-genk/1123106/","https://uk.soccerway.com/matches/2012/03/18/belgium/pro-league/cercle-brugge-ksv/club-brugge-kv/1123107/"])

    p = SWLineupScraper("postgres://lhb:WashingMachine065@34.74.68.51/football",
                        "E0", "1920")
    p.runner(["https://uk.soccerway.com/matches/2021/08/28/england/premier-league/manchester-city-football-club/arsenal-fc/3517075/", "https://uk.soccerway.com/matches/2021/09/11/england/premier-league/crystal-palace-fc/tottenham-hotspur-football-club/3517084/"])

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")