import requests
from bs4 import BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import logging
from typing import *
import os


def fetchLinks(url: str):
    '''
    Requests webpage from football-data.co.uk and scrapes the link to the dataset for each league/season
    '''
    page = requests.get(url)

    if page.status_code != 200:
        logging.error("Failed to request page " + url)
        return False

    soup = BeautifulSoup(page.content, 'lxml')
    links = soup.find_all("a", href=re.compile(".+.csv\Z"))  # Find all <a> tags with a href of a CSV file

    if not links:  # If none are found
        logging.debug("No csv links found on " + url)
        return False

    received = []

    for link in links:
        name = link.get_text()  # name of league
        href = link['href']  # the link itself

        season_search = re.search(r"/(\d\d\d\d)/([A-Z]\d).csv", href)  # RegEx match

        if season_search:
            league = season_search.group(2)  # second group matches the code of the league e.g. E1
            season = season_search.group(1)  # first group refers to the season e.g. 1920 for 2019-2020
        else:
            logging.debug("Season and League not found with RegEx")
            continue

        received.append((league, season, name, href))

    return received


def runner(pages):
    '''
    Enables the webscraping to be performed in parallel to give a significant speed boost
    '''
    with ThreadPoolExecutor(max_workers=15) as executer:
        futures = [executer.submit(fetchLinks, url) for url in pages]
        received_data = []
        for future in as_completed(futures):
            received_data += future.result()

    return received_data


def writeToDB(datasets: List):
    '''
    Obtains a connection to the database and inserts to the league table
    '''
    address: str = os.environ.get('DB_ADDRESS')  # DB Address is stored in environment
    conn = None
    try:
        conn = psycopg2.connect(address)
    except psycopg2.OperationalError as e:
        logging.error("Failed to connect to DB", e)
        exit(1)

    with conn:
        logging.info(datasets)
        logging.debug(conn)
        cursor = conn.cursor()

        template = ','.join(['%s'] * len(datasets))  # Creates placeholders for query based on quantity of values
        statement = '''INSERT INTO league (league, season, league_name, results_location)
                        VALUES {} ON CONFLICT (league, season) 
                        DO UPDATE SET results_location = EXCLUDED.results_location'''.format(template)
        cursor.execute(statement, datasets)


def main(request):
    # TIMER START
    start = time.time()

    # URLs for country results pages
    country_pages = ["https://www.football-data.co.uk/englandm.php",
                     "https://www.football-data.co.uk/scotlandm.php",
                     "https://www.football-data.co.uk/germanym.php",
                     "https://www.football-data.co.uk/italym.php",
                     "https://www.football-data.co.uk/spainm.php",
                     "https://www.football-data.co.uk/francem.php",
                     "https://www.football-data.co.uk/netherlandsm.php",
                     "https://www.football-data.co.uk/belgiumm.php",
                     "https://www.football-data.co.uk/portugalm.php",
                     "https://www.football-data.co.uk/turkeym.php",
                     "https://www.football-data.co.uk/greecem.php"]

    # Run the scraper
    datasets = runner(country_pages)

    # Write to DB if results were returned
    if len(datasets) > 0:
        writeToDB(datasets)
    else:
        logging.error("No results scraped, nothing will be written to database")

    # TIMER DONE
    end = time.time()
    logging.info(end - start, "seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
main("")
