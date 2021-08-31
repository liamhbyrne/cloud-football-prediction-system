import re
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
import logging
import random
import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from flask import request, Flask, jsonify
import chromedriver_binary  # Adds chromedriver binary to path

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)


class SWFixtureLinkScraper:
    '''
    This class handles the process of scraping links to the fixture pages on soccerway.
    '''

    def __init__(self, link, league, season):
        self._link = link
        self._season = season
        self._league = league

    def getBrowser(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("log-level=3")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        browser = webdriver.Chrome(options=chrome_options)
        browser.get(self._link)
        '''
        WebDriverWait(browser, 10).until(expected_conditions.presence_of_element_located((
            By.XPATH, r'//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]'
        )))
        '''
        browser.maximize_window()
        '''
        gdpr_button = browser.find_element_by_xpath(
            r'//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]')  # the button is located by xPath
        browser.execute_script("arguments[0].click();", gdpr_button)  # executes JavaScript to click button
        '''
        return browser

    def traverse(self):
        fixture_links = []

        self._browser = self.getBrowser()
        table_list = self._browser.find_element_by_id(
            "page_competition_1_block_competition_matches_summary_11_page_dropdown")
        for option in table_list.find_elements_by_tag_name('option'):
            time.sleep(2)
            result_table = self.findResultsTable()
            soup = BeautifulSoup(result_table.get_attribute('innerHTML'), 'lxml')

            for match in soup.findAll('td', {'class': 'score-time'}):
                href = match.a.get('href')
                fixture_links.append("https://uk.soccerway.com" + href)

            option.click()

        self._browser.quit()
        return fixture_links

    def findResultsTable(self):
        WebDriverWait(self._browser, 25).until(expected_conditions.presence_of_element_located((
            By.XPATH, r'//*[@id="page_competition_1_block_competition_matches_summary_11"]/div[3]/table/tbody')))
        return self._browser.find_element_by_xpath(
            r'//*[@id="page_competition_1_block_competition_matches_summary_11"]/div[3]/table/tbody')

    def requestPage(self, url: str):
        '''
        HTTP GET each fixture page with an alternating user agent.
        '''
        user_agent_list = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ]
        next_user_agent = random.choice(user_agent_list)

        response = None
        try:
            header = {'user-agent': next_user_agent}
            response = requests.get(url, headers=header)  # Get page
        except requests.exceptions.ConnectionError as e:
            logging.error("ConnectionError: Likely too many simultaneous connections")

        if response.status_code != 200:
            raise Exception("RESPONSE {} ON >>> {}".format(response.status_code, url))

        return response

    def toSoup(self, response):
        return BeautifulSoup(response.text, "lxml")

    def extractMatchInfo(self, link: str):
        soup = self.toSoup(self.requestPage(link))
        match_info = {}

        match_details = soup.find('div', {'class': 'match-info'})
        if not match_details:
            return None

        # LINK
        match_info["link"] = link

        # DATE
        date = match_details.find('div', {'class': 'details'}).a.get_text()
        match_info['game_date'] = datetime.strptime(date, '%d/%m/%Y').strftime("%Y-%m-%d")

        # TEAMS
        match_info["home_team"] = match_details.find('div', {'class': 'container left'}) \
            .find('a', {'class': 'team-title'}).get_text()

        match_info["away_team"] = match_details.find('div', {'class': 'container right'}) \
            .find('a', {'class': 'team-title'}).get_text()

        # Check if game has not happened yet
        KO_box = match_details.find("div", {'class': 'container middle'}).span
        if KO_box:
            if match_details.find("div", {'class': 'container middle'}).span.get_text() == "KO":
                match_info["status"] = "UPCOMING"

        # GAME STATUS
        if match_details.find("h3", {'class': 'thick scoretime'}):
            game_state = match_details.find("h3", {'class': 'thick scoretime'}).span.get_text()

            # if game is finished or in progress
            if "status" not in match_info:
                if game_state in ["FT", "AET"]:
                    match_info["status"] = "FT"
                else:
                    match_info["status"] = "STARTED"

                # SCORELINE
                scoreline = re.search(r'(\d) - (\d)', match_details.find("h3", {'class': 'thick scoretime'}).get_text())
                if scoreline:
                    match_info['home_goals'] = scoreline.group(1)
                    match_info['away_goals'] = scoreline.group(2)
                else:
                    logging.error("RegEx did not find scoreline.")

            else:  # No scoreline given to upcoming games
                match_info['home_goals'] = None
                match_info['away_goals'] = None

        else:
            # If the match does not have a scoreline, it may of been cancelled or erroneous (continue)
            return None

        # LINEUPS
        lineups_containers = soup.find('div', {'class': 'combined-lineups-container'})

        if lineups_containers:
            home_lineup_box = lineups_containers.find('div', {'class': 'container left'}).table.tbody
            away_lineup_box = lineups_containers.find('div', {'class': 'container right'}).table.tbody

            # HOME
            match_info["home_lineup"] = [player.find('td', {'class': 'player large-link'}).a.get_text()
                                         for player in home_lineup_box.find_all('tr')[:11]]

            # AWAY
            match_info["away_lineup"] = [player.find('td', {'class': 'player large-link'}).a.get_text()
                                         for player in away_lineup_box.find_all('tr')[:11]]

        else:  # If the lineups are not available
            match_info["home_lineup"] = [None for _ in range(11)]
            match_info["away_lineup"] = [None for _ in range(11)]

        if all(key in match_info for key in ["home_team", "away_team", "game_date", "status", "home_lineup",
                                             "away_lineup", "home_goals", "away_goals"]):
            return match_info
        else:
            logging.error("Not all keys present")

    def runner(self, links):
        '''
        Makes appropriate function calls
        '''

        with ThreadPoolExecutor(max_workers=5) as executer:
            futures = [executer.submit(self.extractMatchInfo, link) for link in links]

            match_data = []
            # Ensures the program does not continue until all have completed
            for future in as_completed(futures):
                result = future.result()
                match_data.append(result)

        return {"league": self._league, "season": self._season, "match_data": match_data}


@app.route("/", methods=["POST", "GET"])
def hello_world():
    # league, link, season as inputs
    request_json = request.get_json()
    # GET
    if request.args and 'league' in request.args and 'link' in request.args and 'season' in request.args:
        league = request.args.get('league')
        link = request.args.get('link')
        season = request.args.get('season')
    # POST
    elif request_json and 'league' in request_json and 'link' in request_json and 'season' in request_json:
        league = request_json['league']
        link = request_json['link']
        season = request_json['season']
    else:
        return "No or bad parameters were passed", 400

    scraper = SWFixtureLinkScraper(link, league, season)
    result = scraper.traverse()

    return jsonify(scraper.runner(result))
