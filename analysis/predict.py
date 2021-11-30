import os
import re
import time
from datetime import datetime
import logging
import random
from difflib import SequenceMatcher

import psycopg2
import requests
from bs4 import BeautifulSoup
import numpy as np

from analysis.model_runner import ModelRunner
from analysis.player import Match, Team, Player


class Predict:
    def __init__(self, address, link, league, season, home_max_odds, draw_max_odds, away_max_odds):
        self._address = address
        self._conn = self.connectToDB(address)
        self._link = link
        self._league = league
        self._season = season
        self._club_ids = self.fetchClubIds()  # Fetch all clubs and their ids in that league
        self._player_ids = self.fetchPlayerIds()  # Fetch all players from that league
        self._home_max_odds = home_max_odds
        self._draw_max_odds = draw_max_odds
        self._away_max_odds = away_max_odds
        self._model = None

    def connectToDB(self, address):
        """
        Obtain and return a connection object
        """
        try:
            return psycopg2.connect(address)
        except psycopg2.OperationalError:
            logging.error("Failed to connect to DB, likely poor internet connection or bad DB address")
            exit(1)

    def requestPage(self, url: str):
        '''
        HTTP GET each fixture page with an alternating user agent.
        '''
        user_agent_list = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
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
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36"]
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

    def extractMatchInfo(self):
        '''
        Content extraction method for match info and lineups
        '''
        response = self.requestPage(self._link)
        if not response:
            return None

        soup = self.toSoup(response)
        match_info = {}

        match_details = soup.find('div', {'class': 'match-info'})
        if not match_details:
            return None

        # DATE
        date = match_details.find('div', {'class': 'details'}).a.get_text()
        match_info['game_date'] = datetime.strptime(date, '%d/%m/%Y').strftime("%Y-%m-%d")  # date in postgreSQL format

        # TEAMS
        match_info["home_team"] = match_details.find('div', {'class': 'container left'}) \
            .find('a', {'class': 'team-title'}).get_text()

        match_info["away_team"] = match_details.find('div', {'class': 'container right'}) \
            .find('a', {'class': 'team-title'}).get_text()

        # LINEUPS
        lineups_containers = soup.find('div', {'class': 'combined-lineups-container'})

        if lineups_containers:
            home_lineup_box = lineups_containers.find('div', {'class': 'container left'}).table.tbody
            away_lineup_box = lineups_containers.find('div', {'class': 'container right'}).table.tbody

            # If a full lineup is not provided, ignore the match
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
            logging.warning("Lineups not found! Proceeding with None")

        if all(key in match_info for key in ["home_team", "away_team", "game_date", "home_lineup",
                                             "away_lineup"]):
            return match_info
        else:
            logging.error("Not all keys present")

    def extractLineups(self, match_info):
        if not len(self._club_ids):  # No club ids
            raise Exception("No Club IDs for {} - {}, perhaps you need to run the player scraper"
                            .format(self._season, self._league))

        if match_info["home_team"] in self._club_ids:
            match_info["home_id"] = self._club_ids[match_info["home_team"]]
        else:
            # If the home_id can't be matched use string similarity
            match_info["home_id"] = self.searchSimilar(self._club_ids, match_info["home_team"])

        if match_info["away_team"] in self._club_ids:
            match_info["away_id"] = self._club_ids[match_info["away_team"]]
        else:
            match_info["away_id"] = self.searchSimilar(self._club_ids, match_info["away_team"])

        # HOME
        home_squad_ids = dict(self._player_ids[match_info["home_id"]])
        home_lineup_ids = []

        for h_name in match_info["home_lineup"]:
            if h_name is None:  # If lineup not available
                home_lineup_ids.append(None)
            elif h_name in home_squad_ids:
                home_lineup_ids.append(home_squad_ids[h_name])
            else:
                # If the h_name can't be matched use string similarity
                home_lineup_ids.append(self.searchSimilar(home_squad_ids, h_name))

        # AWAY
        away_squad_ids = dict(self._player_ids[match_info["away_id"]])
        away_lineup_ids = []

        for a_name in match_info["away_lineup"]:
            if a_name is None:  # If lineup not available
                away_lineup_ids.append(None)
            elif a_name in away_squad_ids:
                away_lineup_ids.append(away_squad_ids[a_name])
            else:
                away_lineup_ids.append(self.searchSimilar(away_squad_ids, a_name))

        match_info["home_lineup_ids"] = home_lineup_ids
        match_info["away_lineup_ids"] = away_lineup_ids
        return match_info

    def searchSimilar(self, name_ids_dict, name):
        """
        Finds the most similar string to name in name_ids_dict
        """
        closest = ("", 0.0)
        for key in name_ids_dict:
            similarity = SequenceMatcher(None, key, name).ratio()
            if closest[1] < similarity:
                closest = (key, similarity)
        return name_ids_dict[closest[0]]

    def fetchClubIds(self):
        cursor = self._conn.cursor()

        select_statement = '''SELECT club_name, club_id 
                              FROM club 
                              JOIN league ON league.league_id=club.league_id 
                              WHERE league.league='{}' AND league.season='{}';'''.format(self._league, self._season)
        cursor.execute(select_statement)

        return dict(cursor.fetchall())

    def fetchPlayerIds(self):
        """
        Select every player in a given league and group by the club in a dictionary
        """
        cursor = self._conn.cursor()

        select_statement = '''SELECT club.club_id, player.name, player.player_id FROM player
                                JOIN club ON player.club_id=club.club_id
                                JOIN league ON league.league_id=club.league_id
                                WHERE league.season='{}' AND league.league='{}';
                            '''.format(self._season, self._league)
        cursor.execute(select_statement)

        query_result_set = cursor.fetchall()

        club_player_ids = {}
        for club_id, player_name, player_id in query_result_set:
            if club_id in club_player_ids:
                club_player_ids[club_id].append((player_name, player_id))
            else:
                club_player_ids[club_id] = [(player_name, player_id)]

        return club_player_ids

    def fetchRecentScores(self, club_id, match_date):
        cursor = self._conn.cursor()
        cursor.execute('''SELECT home_id, away_id, home_goals, away_goals
                          FROM match
                          WHERE (home_id = %(club_id)s OR away_id = %(club_id)s) AND
                           game_date >= date_trunc('day', %(match_date)s::timestamp - interval '1' month)
                        and game_date < date_trunc('day', %(match_date)s::timestamp)''',
                       {'club_id': club_id, 'match_date': match_date})

        return cursor.fetchall()

    def fetchPlayer(self, player_id: int):
        cursor = self._conn.cursor()
        select_statement = '''SELECT player_id, name, player.club_id, overall_rating, potential_rating,
                                position, age, value, country, total_rating FROM player
                                WHERE player.player_id=%s'''
        cursor.execute(select_statement, (player_id,))
        results = cursor.fetchone()
        return Player(*results)

    def factory(self, match_info_with_ids):
        home_obj = Team(match_info_with_ids["home_id"], match_info_with_ids["home_team"])
        away_obj = Team(match_info_with_ids["away_id"], match_info_with_ids["away_team"])
        for home_p_id, away_p_id in zip(match_info_with_ids["home_lineup_ids"], match_info_with_ids["away_lineup_ids"]):
            home_obj.addPlayer(self.fetchPlayer(home_p_id))
            away_obj.addPlayer(self.fetchPlayer(away_p_id))

        home_obj.calculateRecentForm(
            self.fetchRecentScores(match_info_with_ids["home_id"], match_info_with_ids["game_date"]))
        home_obj.calculatePositionMetrics()

        away_obj.calculateRecentForm(
            self.fetchRecentScores(match_info_with_ids["away_id"], match_info_with_ids["game_date"]))
        away_obj.calculatePositionMetrics()

        match_obj = Match(game_date=match_info_with_ids["game_date"], home_team=home_obj, away_team=away_obj)
        return np.array(match_obj.aggregateFeatures())

    def trainForPredictions(self, save: bool):
        if save:
            save_location = r"C:\Users\Liam\PycharmProjects\football2\model_files\{}-{}.h5".format(
                self._league, datetime.now().strftime("%Y-%m-%d"))
        else:
            save_location = None

        model = ModelRunner(self._address).train_v0_for_predictions(
            save_to=save_location)

        self._model = model

    def loadForPredictions(self, location):
        self._model = ModelRunner().load_v0_NeuralNet(location)

    def predict(self, features):
        assert self._model
        probabilities, outcome = self._model.predictOutcome(features[:-1])
        return probabilities, outcome[0]

    def bet(self, probabilities, outcome, kelly):
        probability = probabilities[outcome]
        odds = [self._draw_max_odds, self._home_max_odds, self._away_max_odds][outcome]
        return kelly * (((odds - 1) * probability) - (1 - probability)) / (odds - 1)


def predictOne():
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment

    p = Predict(address,
                "https://uk.soccerway.com/matches/2021/11/27/england/championship/preston-north-end-fc/fulham-football-club/3523326/",
                "E1", "2122", home_max_odds=4, draw_max_odds=3.4, away_max_odds=1.91)

    match_info = p.extractMatchInfo()
    match_info_with_ids = p.extractLineups(match_info)
    features = p.factory(match_info_with_ids)
    #p.trainForPredictions(save=True)
    p.loadForPredictions(r"C:\Users\Liam\PycharmProjects\football2\model_files\E1-2021-11-24.h5")
    probabilities, outcome = p.predict(features)
    print(outcome, p.bet(probabilities, outcome, 0.5))


def predictMany():
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment



# Call to main, GCP does this implicitly
if __name__ == '__main__':
    # TIMER START
    start = time.time()
    predictOne()
    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + " seconds")
