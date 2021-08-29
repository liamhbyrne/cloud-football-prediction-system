import asyncio
import json
import logging
import os
import time

from flask import request, Flask

from .SWLinkGenFlask import SWLinkGenerator
from .createTablesFlask import setUpDatabase
from .lineupFlask import SWLineupScraper
from .playersFlask import PlayerScraper
from .triggerCloudRun import runner

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
    logging.info("request received on /results")
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


    fixture_links = asyncio.run(runner(links))

    print(fixture_links)

    for league_season in fixture_links:
        parsed_json = json.loads(league_season)
        league = parsed_json["league"]
        season = parsed_json["season"]
        links  = parsed_json["links"]
        lineup_scraper = SWLineupScraper(address, league, season)
        lineup_scraper.runner(links)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")


    return "matches inserted", 200
