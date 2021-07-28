import pandas as pd
import numpy as np
import psycopg2
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# Enables Info logging to be displayed on console
logging.basicConfig(level = logging.INFO)


class MatchTableBuilder:

    def __init__(self, address):
        self._conn = self.connectToDB(address)


    def connectToDB(self, address):
        '''
        Obtain and return a connection object
        '''
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)


    def lookupClubId(self, season, name) -> int:
        cursor = self._conn.cursor()
        select_statement = '''SELECT club_id FROM club
                              JOIN league ON club.league_id = league.league_id 
                              WHERE league.season = '{}' AND club.club_name = '{}';'''.format(season, name)
        cursor.execute(select_statement)

        return cursor.fetchone()[0]


    def parseCSV(self, url, league_id):
        '''
        Requests the CSV file of each season/league. Iterates each line to amend errors and converts
        to a dataframe.
        '''
        response = None
        try:
            response = requests.get("https://www.football-data.co.uk/" + url)  # Get page
        except requests.exceptions.ConnectionError:
            logging.error("ConnectionError: Likely too many simultaneous connections")
            exit(1)

        lines = response.text.splitlines()  # Split based on new lines
        headers = lines[0].split(',')  # Get column headers

        csv = []
        for line in lines[1:]:  # For each line in csv
            split_line = line.split(',')  # Split row on commas

            while len(split_line) < len(headers):  # If there are fewer fields than headers
                split_line.append('')

            csv.append(split_line[:len(headers)])  # Add each field up to the length of the headers

        # Create new dataframe from the list of rows
        df = pd.DataFrame(csv, columns=headers)

        # Combine date and time into a single column
        if ('Time' in df):
            df['Date_Time'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        else:  # Add a 15:00 timestamp if time is not provided
            df['Date_Time'] = pd.to_datetime(df['Date'] + ' ' + '15:00', errors = 'coerce')

        df['Date_Time'] = df['Date_Time'].astype(str)

        # Rename columns
        df = df.rename(
            columns={'FTHG': 'home_goals', 'FTAG': 'away_goals', 'MaxH': 'market_home_max', 'MaxD': 'market_draw_max',
                     'MaxA': 'market_away_max', 'HT': 'HomeTeam', 'AT': 'AwayTeam'})

        # Check the following columns are present, if not set as NaN
        for col in ['home_max', 'draw_max', 'away_max', 'broker_home_max', 'broker_draw_max', 'broker_away_max',
                    'market_home_max', 'market_draw_max', 'market_away_max', 'Max>2.5', 'Max<2.5']:
            if col not in df:
                df[col] = np.nan

        # Brokers
        home_brokers = ['B365H', 'BSH', 'BWH', 'GBH', 'IWH', 'LBH',
                        'PSH', 'SOH', 'SBH', 'SJH', 'SYH', 'VCH', 'WHH']

        draw_brokers = ['B365D', 'BSD', 'BWD', 'GBD', 'IWD', 'LBD',
                        'PSD', 'SOD', 'SBD', 'SJD', 'SYD', 'VCD', 'WHD']

        away_brokers = ['B365A', 'BSA', 'BWA', 'GBA', 'IWA', 'LBA',
                        'PSA', 'SOA', 'SB', 'SJA', 'SYA', 'VCA', 'WHA']

        # List of headers
        headers = list(df)

        # Gather odds in each result group and convert to numerical values
        available_home_brokers = df[[broker for broker in home_brokers if broker in headers]].apply(pd.to_numeric)
        available_draw_brokers = df[[broker for broker in draw_brokers if broker in headers]].apply(pd.to_numeric)
        available_away_brokers = df[[broker for broker in away_brokers if broker in headers]].apply(pd.to_numeric)

        # Find max of each result
        if (not available_home_brokers.empty):  # If there are brokers available
            df = df.assign(home_max=available_home_brokers.max(axis=1))
            df = df.assign(broker_home_max=available_home_brokers.idxmax(axis=1))

        if (not available_draw_brokers.empty):
            df = df.assign(draw_max=available_draw_brokers.max(axis=1))
            df = df.assign(broker_draw_max=available_draw_brokers.idxmax(axis=1))

        if (not available_away_brokers.empty):
            df = df.assign(away_max=available_away_brokers.max(axis=1))
            df = df.assign(broker_away_max=available_away_brokers.idxmax(axis=1))

        # Select the columns which are useful
        filteredData = df[[ 'HomeTeam', 'AwayTeam', 'Date_Time', 'home_goals', 'away_goals', 'home_max',
                           'draw_max', 'away_max', 'broker_home_max', 'broker_draw_max', 'broker_away_max',
                           'market_home_max', 'market_draw_max', 'market_away_max', 'Max>2.5', 'Max<2.5']]

        # Get unique clubs
        unique_clubs = df['HomeTeam'].append(df['AwayTeam'], ignore_index=True).unique()
        club_table_rows = [(str(league_id), club_name) for club_name in unique_clubs.tolist() if club_name != '']

        # Map the team names to their club IDs
        club_ids = {name:id for id, name in self.insertClubs(club_table_rows)}
        filteredData = filteredData.replace({'HomeTeam': club_ids, 'AwayTeam': club_ids})

        # Cleaning erroneous data points
        filteredData = filteredData.replace(to_replace='', value=None)
        filteredData = filteredData.where(pd.notnull(filteredData), None)

        filteredData = filteredData[filteredData.HomeTeam is not None]
        filteredData = filteredData[filteredData.home_goals is not None]

        # Convert to list of tuples compatible with psycopg2
        tuple_rows = filteredData.to_records(index=False).tolist()
        self.insertMatches(tuple_rows)

        return 0


    def runner(self, collected_leagues):
        '''
        Uses multithreading to speed up the CSV parsing
        '''
        with ThreadPoolExecutor(max_workers=10) as executer:
            futures = [executer.submit(self.parseCSV, url, league_id) for league_id, url in collected_leagues]

            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                status = future.result()


    def fetchLeagues(self):
        cursor = self._conn.cursor()
        select_statement = '''SELECT league_id, results_location FROM league;'''
        cursor.execute(select_statement)

        return cursor.fetchall()


    def insertClubs(self, clubs):
        cursor = self._conn.cursor()
        template = ','.join(['%s'] * len(clubs))
        insert_statement = '''INSERT INTO club (league_id, club_name)
                                        VALUES {}
                                        RETURNING club.club_id, club.club_name;'''.format(template)

        cursor.execute(insert_statement, clubs)
        self._conn.commit()

        return cursor.fetchall()


    def insertMatches(self, matches):
        cursor = self._conn.cursor()
        template = ','.join(['%s'] * len(matches))
        insert_statement = '''INSERT INTO match (home_id, away_id, game_date, home_goals, away_goals,
                    home_max, draw_max, away_max, broker_home_max, broker_draw_max, broker_away_max, market_home_max,
                    market_draw_max, market_away_max, max_over_2_5, max_under_2_5)
                    VALUES {} RETURNING match_id;'''.format(template)

        cursor.execute(insert_statement, matches)
        self._conn.commit()



def main(request):
    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    builder = MatchTableBuilder(address)
    leagues = builder.fetchLeagues()

    builder.runner(leagues)
    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return end


# Call to main, GCP does this implicitly
main("")
