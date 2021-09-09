import logging
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from difflib import SequenceMatcher

import psycopg2

logging.basicConfig(level = logging.INFO)


class MatchTableBuilder:
    """
    This class handles the process of scraping lineups from websites and adding them to the database.
    """
    def __init__(self, address, league, season):
        self._season = season
        self._league = league
        self._conn = self.connectToDB(address)
        self._club_ids = self.fetchClubIds()  # Fetch all clubs and their ids in that league
        self._player_ids = self.fetchPlayerIds()  # Fetch all players from that league


    def connectToDB(self, address : str):
        '''
        Obtain and return a connection object
        '''
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)

    def runner(self, match_info_list):
        '''
        Uses ThreadPoolExecutor to use multiprocessing to speed up lineup player matching
        '''

        counter = 1

        with ThreadPoolExecutor(max_workers=5) as executer:
            futures = [executer.submit(self.extractLineups, link) for link in match_info_list if link != None]

            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                status = future.result()
                logging.info("{} / {} complete".format(counter, len(match_info_list)))
                if status != 200:
                    raise Exception("ERROR: Lineup matching failed with: {}".format(status))
                counter += 1


    def extractLineups(self, match_info):
        if not len(self._club_ids):  # No club ids
            raise Exception("No Club IDs for {} - {}, perhaps you need to run the player scraper"
                            .format(self._season, self._league))

        if match_info["home_team"] in self._club_ids:
            home_id = self._club_ids[match_info["home_team"]]
        else:
            # If the home_id can't be matched use string similarity
            home_id = self.searchSimilar(self._club_ids, match_info["home_team"])

        if match_info["away_team"] in self._club_ids:
            away_id = self._club_ids[match_info["away_team"]]
        else:
            away_id = self.searchSimilar(self._club_ids, match_info["away_team"])

        # HOME
        home_squad_ids = dict(self._player_ids[home_id])
        home_lineup_ids = []

        for h_name in match_info["home_lineup"]:
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

        for a_name in match_info["away_lineup"]:
            if a_name is None:  # If lineup not available
                away_lineup_ids.append(None)
            elif a_name in away_squad_ids:
                away_lineup_ids.append(away_squad_ids[a_name])
            else:
                away_lineup_ids.append(self.searchSimilar(away_squad_ids, a_name))


        # Make calls to INSERT to database
        self.insertMatch(home_id, away_id, match_info["game_date"],
                         match_info["status"], match_info["link"], home_lineup_ids,
                         away_lineup_ids, match_info["home_goals"], match_info["away_goals"])

        return 200

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

    def fetchClubIds(self):
        cursor = self._conn.cursor()

        select_statement = '''SELECT club_name, club_id 
                              FROM club 
                              JOIN league ON league.league_id=club.league_id 
                              WHERE league.league='{}' AND league.season='{}';'''.format(self._league, self._season)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())

    def fetchPlayerIds(self):
        """
        Select every player in a given league and group by the club in a dictionary
        """
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
                            VALUES ({})
                            ON CONFLICT (home_id, away_id, game_date) DO NOTHING;'''.format(template)

        cursor.execute(match_insert_statement, (home_id, away_id, game_date, status, link, *home_lineup, *away_lineup, home_goals, away_goals))
        self._conn.commit()
