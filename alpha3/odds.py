import random
import re
from difflib import SequenceMatcher

import pandas as pd
import numpy as np
import psycopg2
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

# Enables Info logging to be displayed on console
logging.basicConfig(level = logging.INFO)


class OddsBuilder:
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


    def requestPage(self, url: str):
        '''
        HTTP GET page with rotating user agents
        '''
        user_agent_list = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ]
        next_user_agent = random.choice(user_agent_list)

        try:
            header = {'user-agent': next_user_agent}
            response = requests.get(url, headers=header)  # Get page with random user agent
        except requests.exceptions.ConnectionError:
            logging.error("ConnectionError: Likely too many simultaneous connections")
            logging.warning("Not all pages have been scraped")
            return None

        if response.status_code != 200:
            logging.error("RESPONSE {} ON >>> {}".format(response.status_code, url))
            logging.warning("Not all pages have been scraped")
            return None

        return response

    def toSoup(self, response):
        '''
        Make sure lxml parser has been installed
        '''
        return BeautifulSoup(response.text, "lxml")

    def fetchCSVlinks(self, url: str):
        """
        This method handles scraping the CSV links from football-data.com
        """
        response = self.requestPage(url)
        if not response:
            logging.error("Failed to get a response from {}".format(url))
            return None

        soup = self.toSoup(response)

        links = soup.find_all("a", href=re.compile(".+.csv\Z"))

        if not links:
            logging.error("No csv links found on " + url)
            return None

        received = []

        for link in links:
            name = link.get_text()
            href = link['href']

            season_search = re.search(r"/(\d\d\d\d)/([A-Z]+\d).csv", href)

            if season_search:
                league = season_search.group(2)
                season = season_search.group(1)
            else:
                logging.debug("Season and League not found with RegEx")
                continue

            received.append((league, season, name, href))

        return received

    def csvFileLocationRunner(self, pages):
        with ThreadPoolExecutor(max_workers=15) as executer:
            futures = [executer.submit(self.fetchCSVlinks, url) for url in pages]
            received_data = []
            for future in as_completed(futures):
                received_data += future.result()

        return received_data

    def writeToDB(self, datasets):

        with self._conn:
            cursor = self._conn.cursor()

            template = ','.join(['%s'] * len(datasets))
            statement = '''INSERT INTO league (league, season, league_name, odds_location)
                            VALUES {} ON CONFLICT (league, season) 
                            DO UPDATE SET odds_location = EXCLUDED.odds_location,
                                            league_name = EXCLUDED.league_name'''.format(template)
            cursor.execute(statement, datasets)

    def fetchLeagues(self):
        cursor = self._conn.cursor()
        select_statement = '''SELECT league_id, odds_location FROM league 
                              WHERE players_location IS NOT NULL AND 
						            match_location IS NOT NULL AND
						            odds_location IS NOT NULL;'''
        cursor.execute(select_statement)

        return cursor.fetchall()

    def parseCSV(self, url, league_id):
        '''
        Requests the CSV file of each season/league. Iterates each line to amend errors and converts
        to a dataframe.
        '''

        response = self.requestPage("https://www.football-data.co.uk/" + url)
        if not response:
            logging.error("[Odds.py] Failed to get response on {}".format(url))
            return None

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

        # Rename columns
        df = df.rename(
            columns={'MaxH': 'market_home_max', 'MaxD': 'market_draw_max',
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

        # Convert date format
        df['Date'] = pd.to_datetime(df.Date).dt.strftime('%Y-%m-%d')

        # Remove erroneous spaces on club names
        df['HomeTeam'] = df['HomeTeam'].str.strip()
        df['AwayTeam'] = df['AwayTeam'].str.strip()

        # Select the columns which are useful
        filteredData = df[['home_max', 'draw_max', 'away_max', 'broker_home_max',
                           'broker_draw_max', 'broker_away_max', 'market_home_max', 'market_draw_max',
                           'market_away_max', 'Max>2.5', 'Max<2.5', 'HomeTeam', 'AwayTeam', 'Date']]

        # LIST OF CLUBS IN THE CSV FILES
        unique_clubs = df['HomeTeam'].append(df['AwayTeam'], ignore_index=True).unique()
        clubs_in_csv_file = list(set([club_name.strip() for club_name in unique_clubs.tolist() if club_name != '']))

        # CLUB NAMES : ID OF CLUBS IN DB
        db_club_ids = self.fetchClubIds(league_id)
        if league_id == 39:
            db_club_ids.pop("Bury", None)  # Bury FC was expelled in the 19/20 season

        if len(unique_clubs) != len(db_club_ids):
            logging.debug("Unbalanced number of club names between season and in DB, league : {}".format(league_id))

        # EDIT KNOWN ANOMALIES
        known_anomalies = {"RAEC Mons": "Bergen", "Leeds United": "Leeds",
                           "Wolverhampton Wanderers": "Wolves",
                           "Sporting CP" : "Sp Lisbon", "União de Leiria" : "Leiria",
                           "Queens Park Rangers" : "QPR", "Stade Rennais FC": "Rennes",
                           }  # sofifa.com : football-data.com

        for db_club_name in db_club_ids:
            if db_club_name in known_anomalies:
                db_club_ids[known_anomalies[db_club_name]] = db_club_ids.pop(db_club_name)

        # PERFECT NAME MATCHES
        matched_club_ids = {club_name : id for club_name, id in db_club_ids.items() if club_name in clubs_in_csv_file}

        # REMAINING CLUBS NEED STRING SIMILARITY MATCHING
        remaining_clubs_in_csv = [x for x in clubs_in_csv_file if x not in matched_club_ids]
        unmatched_club_ids = {club_name: id for club_name, id in db_club_ids.items() if club_name not in matched_club_ids}

        DEBUG_NAME_CONV = {}  # debug dictionary to check which names matched up

        for unmatched_db_name, id in unmatched_club_ids.items():
            closest_name = self.findMostSimilarClubName(unmatched_db_name, remaining_clubs_in_csv)
            matched_club_ids[closest_name] = id
            remaining_clubs_in_csv.remove(closest_name)

            DEBUG_NAME_CONV[unmatched_db_name] = closest_name

        logging.debug(DEBUG_NAME_CONV)

        # Map the team names to their club IDs
        filteredData = filteredData.replace({'HomeTeam': matched_club_ids, 'AwayTeam': matched_club_ids})

        # Cleaning erroneous data points
        filteredData = filteredData.replace({np.nan: None, '': None})

        filteredData = filteredData[filteredData.HomeTeam != None]

        filteredData.to_csv("sample.csv")

        # Convert to list of tuples compatible with psycopg2
        tuple_rows = filteredData.to_records(index=False).tolist()
        self.insertMatches(tuple_rows)


    def parserRunner(self, collected_leagues):
        '''
        Uses multithreading to speed up the CSV parsing
        '''
        with ThreadPoolExecutor(max_workers=5) as executer:
            futures = [executer.submit(self.parseCSV, url, league_id) for league_id, url in collected_leagues]

            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                status = future.exception()
                if status:
                    logging.error(status)

    def fetchClubIds(self, league_id):
        cursor = self._conn.cursor()

        select_statement = '''SELECT club_name, club_id 
                              FROM club 
                              JOIN league ON league.league_id=club.league_id 
                              WHERE league.league_id={};'''.format(league_id)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())

    def findMostSimilarClubName(self, club_name, club_ids):
        if not len(club_ids):
            raise Exception("Cannot do similarity matching when the param:club_ids has length 0")
        closest = ("", 0.0)
        for key in club_ids:
            similarity = SequenceMatcher(None, key, club_name).ratio()
            if closest[1] < similarity:
                closest = (key, similarity)
        return closest[0]

    def insertMatches(self, matches):
        cursor = self._conn.cursor()

        template = ','.join(['%s'] * len(matches))
        insert_statement = '''UPDATE match 
                SET home_max = payload.home_max::real, draw_max = payload.draw_max::real,
                away_max = payload.away_max::real, broker_home_max = payload.broker_home_max,
                broker_draw_max = payload.broker_draw_max, broker_away_max = payload.broker_away_max,
                market_home_max = payload.market_home_max::real, market_draw_max = payload.market_draw_max::real,
                market_away_max = payload.market_away_max::real, max_over_2_5 = payload.max_over_2_5::real,
                max_under_2_5 = payload.max_under_2_5::real
                FROM (VALUES {}) AS payload (home_max, draw_max, away_max, broker_home_max,
                broker_draw_max, broker_away_max, market_home_max, market_draw_max,
                market_away_max, max_over_2_5, max_under_2_5, home_id, away_id, game_date)
                WHERE match.home_id = payload.home_id AND match.away_id = payload.away_id
                AND match.game_date = date(payload.game_date);'''.format(template)

        cursor.execute(insert_statement, matches)
        self._conn.commit()
