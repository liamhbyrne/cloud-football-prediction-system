import asyncio
import json
import logging
import os
import time

from flask import request, Flask

from .soccerway_link_generator import SWLinkGenerator
from .create_tables import setUpDatabase
from .lineup_matcher import MatchTableBuilder
from .odds import OddsBuilder
from .players import PlayerScraper
from .trigger_cloud_run import runner

# Enables Info logging to be displayed on console
logging.basicConfig(level=logging.INFO)

"""
routing.py is a simple Flask server which is intended to run on a VM (e2-micro Compute Engine).
Each route can be triggered with HTTP GET with payload.
"""
app = Flask(__name__)


@app.route("/create-table")
def createTableRoute():
    """
    Creates Tables from tables.sql file
    """
    logging.info("request received on /create-table")

    setUpDatabase()
    return "tables created", 200


@app.route("/players")
def playerTableBuilder():
    """
    Scrapes sofifa.com and inserts player data into DB.
    This should be run before /results otherwise lineups cannot be inserted.
    It works by generating all URLs for each page and conventionally scraping with bs4.
    This only needs to be refreshed once a year (early October).
    """
    logging.info("request received on /players")

    # ARGUMENTS
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
    scraper.runner(links)  # starts multi-threaded process to scrape sofifa.com

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + " seconds")

    return "players inserted", 200

@app.route("/results")
def matchTableBuilder():
    """
    DESCRIPTION: Scrapes soccerway.com and inserts match/lineup data into DB.
    ORDER: This is run after /players.
    TECHNICAL: It works by generating the URL of each league/season page, then triggering a Google Cloud Run container
    with selenium on each URL. Each container instance then scrapes and returns the lineup names and match data
    from each fixture page. Each lineup name is then associated with a player_id from the Player table.
    REFRESH: every month on current seasons to account for new fixtures.
    """
    logging.info("request received on /results")

    # TIMER START
    start = time.time()

    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    if address is None:
        return "DB address not provided in environment", 400

    # ARGUMENTS
    request_json = request.get_json()
    # GET
    if request.args and 'leagues' in request.args:
        league_links = request.args.get('leagues')
    # POST
    elif request_json and 'leagues' in request_json:
        league_links = request_json['leagues']
    else:
        return "No or bad parameters were passed", 400

    leagues = {x["identifier"]: x["link"] for x in league_links}  # e.g. {'E0' : 'soccerway.com/...'}

    # Generate league/season soccerway URLs
    link_generator = SWLinkGenerator(address)

    links = link_generator.linkGenerator(leagues)
    link_generator.insertLinkIntoDB(links)

    # Trigger several Google Cloud Run containers simultaneously
    match_info = asyncio.run(runner(links))

    # For each Cloud Run response
    for league_season in match_info:
        parsed_json = json.loads(league_season)
        league = parsed_json["league"]
        season = parsed_json["season"]
        match_info_list = parsed_json["match_data"]
        # Match scraped club/lineup names with DB values
        lineup_scraper = MatchTableBuilder(address, league, season)
        lineup_scraper.runner(match_info_list)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")


    return "matches inserted", 200

@app.route("/odds")
def addOddsToMatchTable():
    """
        DESCRIPTION: Scrapes football-data.co.uk and inserts odds data into DB.
        ORDER: This is run after /results.
        TECHNICAL:
        REFRESH: every month on current seasons to account for new fixtures.
        """
    logging.info("request received on /odds")

    # TIMER START
    start = time.time()

    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    if address is None:
        return "DB address not provided in environment", 400

    # ARGUMENTS
    request_json = request.get_json()
    # GET
    if request.args and 'countries' in request.args:
        country_links = request.args.get('countries')
    # POST
    elif request_json and 'countries' in request_json:
        country_links = request_json['countries']
    else:
        return "No or bad parameters were passed", 400

    builder = OddsBuilder(address)

    datasets = builder.csvFileLocationRunner([x['link'] for x in country_links])
    builder.writeToDB(datasets)

    leagues = builder.fetchLeagues()
    builder.parserRunner(leagues)

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + "seconds")
    return "odds inserted"