import pandas as pd
import psycopg2
import os
import logging


def parseCSV(url):
    df = pd.read_csv("https://www.football-data.co.uk/" + url)

    if ('Time' in df):
        df['Date_Time'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    else:
        df['Date_Time'] = pd.to_datetime(df['Date'] + ' ' + '15:00')



    df = df.rename(
        columns={'FTHG': 'home_goals', 'FTAG': 'away_goals', 'MaxH': 'market_home_max', 'MaxD': 'market_draw_max',
                 'MaxA': 'market_away_max'})

    home_brokers = ['B365H', 'BSH', 'BWH', 'GBH', 'IWH', 'LBH',
                    'PSH', 'SOH', 'SBH', 'SJH', 'SYH', 'VCH', 'WHH']

    draw_brokers = ['B365D', 'BSD', 'BWD', 'GBD', 'IWD', 'LBD',
                    'PSD', 'SOD', 'SBD', 'SJD', 'SYD', 'VCD', 'WHD']

    away_brokers = ['B365A', 'BSA', 'BWA', 'GBA', 'IWA', 'LBA',
                    'PSA', 'SOA', 'SBA', 'SJA', 'SYA', 'VCA', 'WHA']

    headers = list(df)

    available_home_brokers = df[[broker for broker in home_brokers if broker in headers]]
    available_draw_brokers = df[[broker for broker in draw_brokers if broker in headers]]
    available_away_brokers = df[[broker for broker in away_brokers if broker in headers]]

    df['home_max'] = available_home_brokers.max(axis=1)
    df['draw_max'] = available_draw_brokers.max(axis=1)
    df['away_max'] = available_away_brokers.max(axis=1)

    df['broker_home_max'] = available_home_brokers.idxmax(axis=1)
    df['broker_draw_max'] = available_draw_brokers.idxmax(axis=1)
    df['broker_away_max'] = available_away_brokers.idxmax(axis=1)

    filteredData = df[['Date_Time', 'HomeTeam', 'AwayTeam', 'home_goals', 'away_goals', 'home_max',
                       'draw_max', 'away_max', 'broker_home_max', 'broker_draw_max', 'broker_away_max',
                       'market_home_max', 'market_draw_max', 'market_away_max', 'Max>2.5', 'Max<2.5']]

    filteredData.to_csv('out.csv')

    print(filteredData['HomeTeam'].append(filteredData['AwayTeam'], ignore_index=True).unique())


address: str = os.environ.get('DB_ADDRESS')
conn = None
try:
    conn = psycopg2.connect(address)
except psycopg2.OperationalError as e:
    logging.error("Failed to connect to DB", e)
    exit(1)

with conn:
    cursor = conn.cursor()
    select_statement = '''SELECT league_id, results_location FROM league;'''
    cursor.execute(select_statement)


    leagues = cursor.fetchall()
    for league_id, url in leagues:
        parseCSV(url)


    template = ','.join(['%s'] * len(leagues))
    insert_statement = '''INSERT INTO club (league_id, club_name)
                            VALUES {}
                            RETURNING club.club_id;'''.format(template)
    print(insert_statement)
    #cursor.execute(insert_statement, leagues)
    #print(cursor.fetchall)
