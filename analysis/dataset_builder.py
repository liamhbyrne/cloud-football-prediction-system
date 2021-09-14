import logging
import os
import time

import psycopg2


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
        cursor = self._conn.cursor()

        select_statement = """SELECT *
                            FROM match
                            JOIN club ON match.home_id = club.club_id
                            JOIN league ON club.league_id = league.league_id
                    
                            WHERE (match.game_date >= %(start_date)s OR %(start_date)s IS NULL)
                              AND (match.game_date <= %(end_date)s OR %(end_date)s IS NULL)
                              AND (match.status = %(status)s OR %(status)s IS NULL)
                              AND (league.league_id = %(league_id)s OR %(league_id)s IS NULL)
                              AND (match.home_goals > match.away_goals OR %(home_win)s IS NULL)
                              AND (match.home_goals < match.away_goals OR %(away_win)s IS NULL)
                              AND (match.home_goals < match.away_goals OR %(draw)s IS NULL)
                              AND (league.season = %(season)s OR %(season)s IS NULL)
                              AND (league.league = %(league_code)s OR %(league_code)s IS NULL)
                              AND ((league.players_location IS NOT NULL AND league.match_location IS NOT NULL) 
                                    OR %(players_and_lineups_available)s IS NULL)
                              AND (league.odds_location IS NOT NULL OR %(players_and_lineups_available)s IS NULL);"""

        cursor.execute(select_statement, {'start_date' : start_date, 'end_date' : end_date, 'status' : status,
                                          'league_id' : league_id, 'home_win' : home_win, 'away_win' : away_win,
                                          'draw' : draw, 'season' : season, 'league_code' : league_code,
                                          'players_and_lineups_available' : players_and_lineups_available,
                                          'odds_available' : odds_available})
        print(cursor.fetchall())



def main(request):
    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    builder = DatasetBuilder(address)
    builder.fetchMatches(start_date='2017-01-01')
    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
