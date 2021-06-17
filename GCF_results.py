import pandas as pd
import psycopg2
import os
import logging


address: str = os.environ.get('DB_ADDRESS')
conn = None
try:
    conn = psycopg2.connect(address)
except psycopg2.OperationalError as e:
    logging.error("Failed to connect to DB", e)
    exit(1)

with conn:
    cursor = conn.cursor()
    statement = '''SELECT season, league, results_location FROM dataset;'''
    cursor.execute(statement)
    leagues = cursor.fetchall()

print(leagues)

df = pd.read_csv("https://www.football-data.co.uk/mmz4281/2021/E0.csv", parse_dates=[['Date', 'Time']])

df = df.rename(columns={'FTHG' : 'home_goals', 'FTAG' : 'away_goals', 'MaxH' : 'market_home_max', 'MaxD' : 'market_draw_max',
                   'MaxA' : 'market_away_max'})

home_brokers = ['B365H','BSH','BWH','GBH','IWH','LBH',
                'PSH','SOH','SBH','SJH','SYH','VCH','WHH']

draw_brokers = ['B365D','BSD','BWD','GBD','IWD','LBD',
                'PSD','SOD','SBD','SJD','SYD','VCD','WHD']

away_brokers = ['B365A','BSA','BWA','GBA','IWA', 'LBA',
                'PSA','SOA','SBA','SJA','SYA','VCA','WHA']

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
