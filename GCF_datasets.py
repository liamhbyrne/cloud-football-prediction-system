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
    page = requests.get(url)
    if page.status_code != 200:
        logging.error("Failed to request page " + url)
        return False

    soup = BeautifulSoup(page.content, 'lxml')
    links = soup.find_all("a", href=re.compile(".+.csv\Z"))

    if not links:
        logging.debug("No csv links found on " + url)
        return False

    received = []

    for link in links:
        name = link.get_text()
        href = link['href']

        season_search = re.search(r"/(\d\d\d\d)/([A-Z]\d).csv", href)

        if season_search:
            league = season_search.group(2)
            season = season_search.group(1)
        else:
            logging.debug("Season and League not found with RegEx")
            continue

        received.append((league, season, name, href))

    return received


def runner(pages):
    with ThreadPoolExecutor(max_workers=15) as executer:
        futures = [executer.submit(fetchLinks, url) for url in pages]
        received_data = []
        for future in as_completed(futures):
            received_data += future.result()

    return received_data


def writeToDB(datasets: List):
    address: str = os.environ.get('DB_ADDRESS')
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

        template = ','.join(['%s'] * len(datasets))
        statement = '''INSERT INTO dataset (league, season, league_name, results_location)
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
    return end - start

main("")