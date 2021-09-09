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
    """
    This class handles scraping the lineups and match info of every match in a season.
    """

    def __init__(self, link, league, season):
        self._link = link
        self._season = season
        self._league = league

    def getBrowser(self):
        """
        Setup webdriver
        """
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")  # headless for Cloud Run
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("log-level=3") # min level logging
        chrome_options.add_argument("--no-sandbox")  # Disable sandboxing, works better on VM
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])  # further suppress logging

        browser = webdriver.Chrome(options=chrome_options)
        browser.get(self._link)

        # The following 2 comment blocks contain code for pressing the privacy notice, not needed when run on GCP
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
            time.sleep(3.5)
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
        user_agent_list = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2866.71 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux i686 on x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2820.59 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2762.73 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2656.18 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/44.0.2403.155 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36",
            "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/4E423F",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.517 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.16 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1623.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36",
            "Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.2 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1467.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1500.55 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.90 Safari/537.36",
            "Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36",
            "Mozilla/5.0 (X11; CrOS i686 3912.101.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.15 (KHTML, like Gecko) Chrome/24.0.1295.0 Safari/537.15",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.14 (KHTML, like Gecko) Chrome/24.0.1292.0 Safari/537.14",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.13 (KHTML, like Gecko) Chrome/24.0.1290.1 Safari/537.13",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.13 (KHTML, like Gecko) Chrome/24.0.1290.1 Safari/537.13",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.13 (KHTML, like Gecko) Chrome/24.0.1290.1 Safari/537.13",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.13 (KHTML, like Gecko) Chrome/24.0.1290.1 Safari/537.13",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.13 (KHTML, like Gecko) Chrome/24.0.1284.0 Safari/537.13",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.6 Safari/537.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.6 Safari/537.11",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.26 Safari/537.11",
            "Mozilla/5.0 (Windows NT 6.0) yi; AppleWebKit/345667.12221 (KHTML, like Gecko) Chrome/23.0.1271.26 Safari/453667.1221",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.17 Safari/537.11",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.94 Safari/537.4",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_0) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.79 Safari/537.4",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.2 (KHTML, like Gecko) Chrome/22.0.1216.0 Safari/537.2",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
            "Mozilla/5.0 (X11; FreeBSD amd64) AppleWebKit/536.5 (KHTML like Gecko) Chrome/19.0.1084.56 Safari/1EA69",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.22 (KHTML, like Gecko) Chrome/19.0.1047.0 Safari/535.22",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21",
            "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1041.0 Safari/535.21",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
            "Mozilla/5.0 (Macintosh; AMD Mac OS X 10_8_2) AppleWebKit/535.22 (KHTML, like Gecko) Chrome/18.6.872",
            "Mozilla/5.0 (X11; CrOS i686 1660.57.0) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.46 Safari/535.19"]
        next_user_agent = random.choice(user_agent_list)

        try:
            header = {'user-agent': next_user_agent}
            response = requests.get(url, headers=header)  # Get page
        except requests.exceptions.ConnectionError as e:
            logging.warning("Failed to get a response on {}".format(url))
            return None

        # Some pages are inaccessible due to server, these can be passed but ideally a minimum
        if response.status_code == 500:
            logging.warning("Response 500 on {}".format(url))
            return None

        if response.status_code != 200:
            raise Exception("RESPONSE {} ON >>> {}".format(response.status_code, url))

        return response

    def toSoup(self, response):
        """
        Make sure lxml html parser is installed
        """
        return BeautifulSoup(response.text, "lxml")

    def extractMatchInfo(self, link: str):
        '''
        Content extraction method for match info and lineups
        '''
        response = self.requestPage(link)
        if not response:
            return None

        soup = self.toSoup(response)
        match_info = {}

        match_details = soup.find('div', {'class': 'match-info'})
        if not match_details:
            return None

        # LINK
        match_info["link"] = link

        # DATE
        date = match_details.find('div', {'class': 'details'}).a.get_text()
        match_info['game_date'] = datetime.strptime(date, '%d/%m/%Y').strftime("%Y-%m-%d")  # date in postgreSQL format

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

            #If a full lineup is not provided, ignore the match
            if len(home_lineup_box.find_all('tr')) < 12 or len(away_lineup_box.find_all('tr')) < 12:
                return None

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
        The rate of content extraction can be increased through multiprocessing
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
def main():
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
