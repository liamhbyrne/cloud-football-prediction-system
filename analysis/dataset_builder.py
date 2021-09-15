import logging
import os
import time
from typing import List

import psycopg2
import numpy as np
import pandas as pd
from pandas import DataFrame

from analysis.player import Player, Team, Match


class DatasetBuilder:
    '''
        This class contains the functionality to add Odds data to the Match table
        '''

    def __init__(self, address):
        self._conn = self.connectToDB(address)

    def connectToDB(self, address):
        """
        Obtain and return a connection object
        """
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)

    def fetchMatches(self, start_date=None, end_date=None, status=None, league_id=None, home_win=None, away_win=None,
                     draw=None, season=None, league_code=None, players_and_lineups_available=None, odds_available=None):
        """
        Fetches match data from the database with queried with the optional parameters
        """
        select_statement = """SELECT *
                            FROM match
                            JOIN club as home ON match.home_id = home.club_id
                            JOIN club as away ON match.away_id = away.club_id
                            JOIN league ON home.league_id = league.league_id
                    
                            WHERE (match.game_date >= %(start_date)s OR %(start_date)s IS NULL)
                              AND (match.game_date <= %(end_date)s OR %(end_date)s IS NULL)
                              AND (match.status = %(status)s OR %(status)s IS NULL)
                              AND (league.league_id = %(league_id)s OR %(league_id)s IS NULL)
                              AND (match.home_goals > match.away_goals OR %(home_win)s IS NULL)
                              AND (match.home_goals < match.away_goals OR %(away_win)s IS NULL)
                              AND (match.home_goals = match.away_goals OR %(draw)s IS NULL)
                              AND (league.season = %(season)s OR %(season)s IS NULL)
                              AND (league.league = %(league_code)s OR %(league_code)s IS NULL)
                              AND ((league.players_location IS NOT NULL AND league.match_location IS NOT NULL) 
                                    OR %(players_and_lineups_available)s IS NULL)
                              AND (match.home_max IS NOT NULL OR %(odds_available)s IS NULL);"""

        parameters = {'start_date': start_date, 'end_date': end_date, 'status': status,
                      'league_id': league_id, 'home_win': home_win, 'away_win': away_win,
                      'draw': draw, 'season': season, 'league_code': league_code,
                      'players_and_lineups_available': players_and_lineups_available,
                      'odds_available': odds_available}

        df = pd.read_sql_query(select_statement, self._conn, params=parameters)
        del df['club_id']
        name_changes = {'club_name': ['home_name', 'away_name']}
        df = df.rename(columns=lambda i: name_changes[i].pop(0) if i in name_changes else i)

        return df.loc[:, ~df.columns.duplicated()]

    def fetchColumnNames(self) -> List[str]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT * FROM match LIMIT 0")
        columns = [desc[0] for desc in cursor.description]
        return columns

    def fetchPlayer(self, id: int) -> Player:
        cursor = self._conn.cursor()
        cursor.execute('''SELECT player_id, name, club_id, overall_rating, potential_rating,
                                position, age, value, country, total_rating
                          FROM player WHERE player_id = %s;''', (id,))
        results = cursor.fetchone()
        if results:
            return Player(*results)

        return Player(*[None for _ in range(10)])

    def factory(self, df: DataFrame) -> List[Match]:
        match_objects = []

        column_names = self.fetchColumnNames()
        for match_tuple in df.itertuples():
            if all(hasattr(match_tuple, attr) for attr in column_names):

                # HOME TEAM
                home_team = Team(match_tuple.home_id, match_tuple.home_name)
                for home_player_id in [match_tuple.h1_player_id, match_tuple.h2_player_id, match_tuple.h3_player_id,
                                       match_tuple.h4_player_id, match_tuple.h5_player_id, match_tuple.h6_player_id,
                                       match_tuple.h7_player_id, match_tuple.h8_player_id, match_tuple.h9_player_id,
                                       match_tuple.h10_player_id, match_tuple.h11_player_id]:
                    home_team.addPlayer(home_player_id)

                # AWAY TEAM
                away_team = Team(match_tuple.away_id, match_tuple.away_name)
                for away_player_id in [match_tuple.a1_player_id, match_tuple.a2_player_id, match_tuple.a3_player_id,
                                       match_tuple.a4_player_id, match_tuple.a5_player_id, match_tuple.a6_player_id,
                                       match_tuple.a7_player_id, match_tuple.a8_player_id, match_tuple.a9_player_id,
                                       match_tuple.a10_player_id, match_tuple.a11_player_id]:
                    away_team.addPlayer(away_player_id)

                # ODDS
                dict_conversion = dict(match_tuple._asdict())
                odds = {key: dict_conversion[key] for key in  ["home_max", "draw_max", "away_max", "broker_home_max",
                                           "broker_draw_max", "broker_away_max", "market_home_max", "market_draw_max",
                                           "market_away_max", "max_over_2_5","max_under_2_5"]
                                    if isinstance(dict_conversion[key], float) if not np.isnan(dict_conversion[key])}

                # MATCH INSTANTIATION
                match_objects.append(Match(match_id=match_tuple.match_id, home_team=home_team, away_team=away_team,
                                  game_date=match_tuple.game_date, status=match_tuple.status, link=match_tuple.link,
                                  home_goals=match_tuple.home_goals, away_goals=match_tuple.away_goals, odds_data=odds))

            else:
                logging.warning("dataframe row does not have all required columns")
        return match_objects

def main(request):
    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    builder = DatasetBuilder(address)
    df = builder.fetchMatches()
    print([x.getLink() for x in builder.factory(df)])

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
