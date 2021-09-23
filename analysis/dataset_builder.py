import logging
import random
from typing import List

import numpy as np
import pandas as pd
import psycopg2
from pandas import DataFrame

from analysis.player import Player, Team, Match

logging.basicConfig(level=logging.INFO)


class DatasetBuilder:
    '''
        This class contains the functionality to add Odds data to the Match table
        '''

    def __init__(self, address):
        self._conn = self.connectToDB(address)
        self._df: DataFrame
        self._cachedPlayers = {}

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
        df['game_date'] = pd.to_datetime(df['game_date'])
        del df['club_id']
        name_changes = {'club_name': ['home_name', 'away_name']}
        df = df.rename(columns=lambda i: name_changes[i].pop(0) if i in name_changes else i)
        self._df = df.loc[:, ~df.columns.duplicated()]

    def fetchRecentScores(self, club_id, match_date):
        cursor = self._conn.cursor()
        cursor.execute('''SELECT home_id, away_id, home_goals, away_goals
                          FROM match
                          WHERE (home_id = %(club_id)s OR away_id = %(club_id)s) AND
                           game_date >= date_trunc('day', %(match_date)s::timestamp - interval '1' month)
                        and game_date < date_trunc('day', %(match_date)s::timestamp)''',
                       {'club_id': club_id, 'match_date': match_date})

        return cursor.fetchall()

    def pdFetchRecentScores(self, club_id, match_date):
        start_date = match_date - pd.DateOffset(months=1)
        mask = (self._df['game_date'] > start_date) & (self._df['game_date'] < match_date) & (
                (self._df['home_id'] == club_id) | (self._df['away_id'] == club_id))
        return [tuple(x) for x in self._df.loc[mask][['home_id', 'away_id', 'home_goals', 'away_goals']].to_numpy()]

    def fetchColumnNames(self) -> List[str]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT * FROM match LIMIT 0")
        columns = [desc[0] for desc in cursor.description]
        return columns

    def fetchPlayers(self, league_id: int):
        cursor = self._conn.cursor()
        select_statement = '''SELECT player_id, name, player.club_id, overall_rating, potential_rating,
                                position, age, value, country, total_rating FROM player
                                JOIN club ON player.club_id = club.club_id
                                JOIN league ON club.league_id = league.league_id
                                WHERE league.league_id = %s;'''
        cursor.execute(select_statement, (league_id,))
        results = cursor.fetchall()

        players = {}
        for row in results:
            players[row[0]] = Player(*row)

        self._cachedPlayers[league_id] = players

    def factory(self) -> List[Match]:
        if self._df is None:
            raise Exception("Dataframe has not been created")

        match_objects = []

        column_names = self.fetchColumnNames()

        for match_tuple in self._df.itertuples():
            #print(match_tuple)
            if all(hasattr(match_tuple, attr) for attr in column_names):

                if match_tuple.league_id not in self._cachedPlayers:
                    self.fetchPlayers(match_tuple.league_id)

                # HOME TEAM
                home_team = Team(match_tuple.home_id, match_tuple.home_name)
                try:
                    for player_id in [match_tuple.h1_player_id, match_tuple.h2_player_id, match_tuple.h3_player_id,
                                      match_tuple.h4_player_id, match_tuple.h5_player_id, match_tuple.h6_player_id,
                                      match_tuple.h7_player_id, match_tuple.h8_player_id, match_tuple.h9_player_id,
                                      match_tuple.h10_player_id, match_tuple.h11_player_id]:

                            home_team.addPlayer(self._cachedPlayers[match_tuple.league_id][player_id])

                    # AWAY TEAM
                    away_team = Team(match_tuple.away_id, match_tuple.away_name)

                    for player_id in [match_tuple.a1_player_id, match_tuple.a2_player_id, match_tuple.a3_player_id,
                                      match_tuple.a4_player_id, match_tuple.a5_player_id, match_tuple.a6_player_id,
                                      match_tuple.a7_player_id, match_tuple.a8_player_id, match_tuple.a9_player_id,
                                      match_tuple.a10_player_id, match_tuple.a11_player_id]:
                        away_team.addPlayer(self._cachedPlayers[match_tuple.league_id][player_id])

                except KeyError:
                    logging.warning("No lineup data for {}".format(match_tuple.link))
                    continue

                home_team.calculateRecentForm(self.pdFetchRecentScores(match_tuple.home_id, match_tuple.game_date))
                home_team.calculatePositionMetrics()

                away_team.calculateRecentForm(self.pdFetchRecentScores(match_tuple.away_id, match_tuple.game_date))
                away_team.calculatePositionMetrics()

                # ODDS
                dict_conversion = dict(match_tuple._asdict())
                odds = {key: dict_conversion[key] for key in ["home_max", "draw_max", "away_max", "broker_home_max",
                                                              "broker_draw_max", "broker_away_max", "market_home_max",
                                                              "market_draw_max",
                                                              "market_away_max", "max_over_2_5", "max_under_2_5"]
                        if isinstance(dict_conversion[key], float) if not np.isnan(dict_conversion[key])}

                # MATCH INSTANTIATION
                match_objects.append(Match(match_id=match_tuple.match_id, home_team=home_team, away_team=away_team,
                                           game_date=match_tuple.game_date, status=match_tuple.status,
                                           link=match_tuple.link,
                                           home_goals=match_tuple.home_goals, away_goals=match_tuple.away_goals,
                                           odds_data=odds))

            else:
                logging.warning("dataframe row does not have all required columns")

        return match_objects

    def buildDataset_v0(self, match_objects : List[Match], training_split : float):
        features = [x.aggregateFeatures() for x in match_objects]
        random.shuffle(features)
        training_size = round(len(match_objects) * training_split)
        training_set = features[:training_size]
        testing_set = features[training_size:]

        x_train = np.array([x[:-1] for x in training_set])
        y_train = np.array([y[-1] for y in training_set])

        x_test = np.array([x[:-1] for x in testing_set])
        y_test = np.array([y[-1] for y in testing_set])

        logging.info("Training set has {} matches, this will be tested on {} matches".format(len(training_set), len(testing_set)))
        return x_train, y_train, x_test, y_test

    def buildSeasonTest(self, match_objects : List[Match]):
        features = [x.aggregateOddsFeatures() for x in match_objects]
        test_features = np.array([x[:-2] for x in features])
        test_labels = np.array([y[-2] for y in features])
        odds_data = np.array([y[-1] for y in features])
        return test_features, test_labels, odds_data
