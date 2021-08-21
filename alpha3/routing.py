import logging
import os
import time
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from flask import request, Flask

from .createTablesFlask import setUpDatabase
from .playersFlask import PlayerScraper
from .SWLinkGenFlask import SWLinkGenerator
from .SWFixtureLinkFlask import SWFixtureLinkScraper

app = Flask(__name__)

# Enables Info logging to be displayed on console
logging.basicConfig(level=logging.INFO)


@app.route("/create-table")
def createTableRoute():
    return setUpDatabase()

@app.route("/players")
def playerTableBuilder():

    request_json = request.get_json()
    # GET
    if request.args and 'edition' in request.args and 'league' in request.args:
        edition_numbers_json = request.args.get('edition')
        league_numbers_json = request.args.get('league')
    # POST
    elif request_json and 'edition' in request_json and 'league' in request_json:
        edition_numbers_json = request_json['edition']
        league_numbers_json = request_json['league']
    else:
        return "No or bad parameters were passed", 400

    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    scraper = PlayerScraper(address)

    edition_numbers = {x["season"]: x["code"] for x in edition_numbers_json}
    league_numbers = {x["short_league"]: x["code"] for x in league_numbers_json}

    links = scraper.linkGenerator(edition_numbers, league_numbers)
    scraper.insertLinksToDB(links)
    scraper.runner(links)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + " seconds")
    return "players inserted", 200

@app.route("/results")
def matchTableBuilder():
    # TIMER START
    start = time.time()

    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    scraper = SWLinkGenerator(address)

    request_json = request.get_json()
    # GET
    if request.args and 'leagues' in request.args:
        league_links = request.args.get('leagues')
    # POST
    elif request_json and 'leagues' in request_json:
        league_links = request_json['leagues']
    else:
        return "No or bad parameters were passed", 400

    leagues = {x["identifier"]: x["link"] for x in league_links}

    links = scraper.seasonLinkFetcher(leagues)
    scraper.insertLinkIntoDB(links)

    print(len(links))
    fixture_scrapers = [SWFixtureLinkScraper(link, league, season) for league, season, link in links]

    fixtures = []
    with ThreadPoolExecutor(max_workers=1) as executer:
        futures = [executer.submit(scraper.traverse) for scraper in fixture_scrapers]

        for future in as_completed(futures):
            print(future.result())
            fixtures.append(future.result())

    print(fixtures)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")


    return "matches inserted", 200

