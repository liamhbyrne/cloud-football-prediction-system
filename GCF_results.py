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


def parseCSV(url, league_id, season):
    '''
    Requests the CSV file of each season/league. Iterates each line to amend errors and converts
    to a dataframe.
    '''
    response = requests.get("https://www.football-data.co.uk/" + url)  # Get page
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
    filteredData = df[['Date_Time', 'HomeTeam', 'AwayTeam', 'home_goals', 'away_goals', 'home_max',
                       'draw_max', 'away_max', 'broker_home_max', 'broker_draw_max', 'broker_away_max',
                       'market_home_max', 'market_draw_max', 'market_away_max', 'Max>2.5', 'Max<2.5']]

    # Get unique clubs
    unique_clubs = df['HomeTeam'].append(df['AwayTeam'], ignore_index=True).unique()
    club_table_rows = [(league_id, season, club_name) for club_name in unique_clubs.tolist()]

    return filteredData, club_table_rows


def runner(collected_leagues):
    '''
    Uses multithreading to speed up the CSV parsing
    '''
    with ThreadPoolExecutor(max_workers=10) as executer:
        futures = [executer.submit(parseCSV, url, league_id, season) for league_id, url, season in collected_leagues]
        club_rows  = []
        match_rows = []

        # Ensures the program does not continue until all have completed
        for future in as_completed(futures):
            clubs, matches = future.result()
            club_rows += clubs
            match_rows += matches

    return (match_rows, club_rows)


def connectToDB():
    '''
    Obtain and return a connection object
    '''
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    try:
        return psycopg2.connect(address)
    except psycopg2.OperationalError as e:
        logging.error("Failed to connect to DB", e)
        exit(1)


def main(request):
    # TIMER START
    start = time.time()

    conn = connectToDB()
    with conn:
        cursor = conn.cursor()
        select_statement = '''SELECT league_id, results_location, season FROM league;'''
        cursor.execute(select_statement)

        leagues = cursor.fetchall()

        print(runner(leagues))

        #template = ','.join(['%s'] * len(leagues))
        #insert_statement = '''INSERT INTO club (league_id, club_name)
        #                        VALUES {}
        #                        RETURNING club.club_id;'''.format(template)

        #cursor.execute(insert_statement, clubs)


    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")


# Call to main, GCP does this implicitly
main("")
