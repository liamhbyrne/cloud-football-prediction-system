import logging
import os
import random
import time
from datetime import datetime

from analysis.dataset_builder import DatasetBuilder
from models.NeuralNet import NeuralNet
import matplotlib.pyplot as plt
import numpy as np

# Enables Info logging to be displayed on console
logging.basicConfig(level=logging.INFO)


class ModelRunner:
    def __init__(self, address: str = None):
        self._address = address
        self._builder = self.setBuilder()

    def setBuilder(self):
        if not self._address:
            logging.info("Model runner proceeding without database access . . .")
            return None
        return DatasetBuilder(self._address)

    def load_v0_NeuralNet(self, model_path):
        nn = NeuralNet()
        nn.loadModel(model_path)
        return nn

    def train_v0_NeuralNet(self):
        self._builder.fetchMatches(status='FT', players_and_lineups_available=True, league_code='E0')
        objs = self._builder.factory()
        x_train, y_train, x_test, y_test = self._builder.buildDataset_v0(objs, 0.75)
        nn = NeuralNet()
        nn.compileModel()
        nn.fitModel(x_train, y_train, 50)

        print("MODEL ACCURACY {}%".format(round(nn.evaluateAccuracy(x_test, y_test) * 100, 5)))
        return nn

    def train_v0_for_predictions(self, save_to=None):
        self._builder.fetchMatches(status='FT', players_and_lineups_available=True, league_code='E0')
        objs = self._builder.factory()
        x_train, y_train, x_test, y_test = self._builder.buildDataset_v0(objs, 1)
        nn = NeuralNet()
        nn.compileModel()
        nn.fitModel(x_train, y_train, 50)
        if save_to:
            nn.saveModel(save_to)
        return nn

    def payout_v0_NeuralNet(self, league, load_path=None):
        # TRAIN MODEL BEFORE 20/21
        if load_path:
            nn = NeuralNet()
            nn.loadModel(r"C:\Users\Liam\PycharmProjects\football2\model_files\E1-2021-11-29.h5")
        else:
            self._builder.fetchMatches(status='FT', players_and_lineups_available=True, league_code="E1",
                                       )#end_date='2021-07-27')
            objs = self._builder.factory()

            x_train, y_train, x_test, y_test = self._builder.buildDataset_v0(objs, 0.9)

            nn = NeuralNet()
            nn.compileModel()
            nn.fitModel(x_train, y_train, epochs=50)
            #nn.saveModel(r"C:/Users/Liam/PycharmProjects/football2/model_files/{}-{}.h5".format(
            #       league, datetime.now().strftime("%Y-%m-%d")))

            logging.info("MODEL ACCURACY {}%".format(round(nn.evaluateAccuracy(x_test, y_test) * 100, 5)))


        # GET 20/21 MATCH/ODDS DATA
        self._builder = self.setBuilder()
        self._builder.fetchMatches(status='FT', players_and_lineups_available=True, league_code=league,
                                   )#season='2122')

        objs = self._builder.factory()

        test_features, test_labels, odds_data = self._builder.buildSeasonTest(objs)

        games_predicted_correctly = 0
        balance = 1000
        KELLY = 0.2

        correct_odds_series = []
        correct_prob_series = []
        wrong_odds_series = []
        wrong_prob_series = []

        balance_series = [balance]

        for feature_vector, label, odds in zip(test_features, test_labels, odds_data):
            # Make prediction
            probabilities, predicted_outcome = nn.predictOutcome(feature_vector)

            predicted_outcome[0] = random.choice([0,1,2])

            # Check if its correct
            correct = False
            if predicted_outcome[0] == label:
                games_predicted_correctly += 1
                correct = True

            if balance >= 0.01:
                # Bet
                odds_column = ['draw_max', 'home_max', 'away_max'][predicted_outcome[0]]
                if odds_column not in odds:
                    print(odds)
                    logging.info("skipped on a match as odds data unavailable")
                    continue

                # APPEND TO ODDS/PROBABILITY DATASET
                odds_series = correct_odds_series if correct else wrong_odds_series
                prob_series = correct_prob_series if correct else wrong_prob_series
                odds_series.append(1 / odds[odds_column])
                prob_series.append(probabilities[predicted_outcome[0]])

                kelly_proportion = KELLY * self.calculateKellyCriterion(odds[odds_column],
                                                                        probabilities[predicted_outcome[0]])

                if kelly_proportion > 0.0:
                    stake = round(kelly_proportion * balance, 1)
                    #stake = 10
                    balance -= stake
                    if correct:
                        balance += (stake * odds[odds_column])
                    print(correct, "ODDS", odds[odds_column], "BALANCE", balance, "STAKE", stake, "KELLY",
                          kelly_proportion)

                balance_series.append(balance)

                # logging.info("balance update: {}".format(balance))

        # RESULTS
        logging.info("FINAL BALANCE: {}".format(balance))
        logging.info("Accuracy for the 20/21 season is: {}".format(games_predicted_correctly / len(test_labels)))

        # PLOT ODDS/PROB RATIO
        overall_odds_series = np.array(correct_odds_series + wrong_odds_series)
        overall_prob_series = np.array(correct_prob_series + wrong_prob_series)
        logging.info("Odds/Prob MSE {}".format(np.square(np.subtract(overall_odds_series, overall_prob_series)).mean()))
        fig1, ax1 = plt.subplots()
        ax1.scatter(correct_odds_series, correct_prob_series, c='green')
        ax1.scatter(wrong_odds_series, wrong_prob_series, c='red')
        plt.plot(np.unique(overall_odds_series),
                 np.poly1d(np.polyfit(overall_odds_series, overall_prob_series, 1))(np.unique(overall_odds_series)))
        ax1.set_title("Odds to Probability distribution for {}".format(league))
        ax1.set_xlabel("odds")
        ax1.set_ylabel("probability")

        # BALANCE GRAPH
        fig2, ax2 = plt.subplots()
        ax2.plot(balance_series[:])
        ax2.set_title("Balance by game {}".format(league))
        ax2.set_xlabel("game")
        ax2.set_ylabel("balance")

        # SHOW GRAPH
        plt.show()

    def calculateKellyCriterion(self, odds, probability) -> float:
        return (((odds - 1) * probability) - (1 - probability)) / (odds - 1)



def main(request):
    # TIMER START
    start = time.time()
    address: str = os.environ.get('DB_ADDRESS')  # Address stored in environment
    model_runner = ModelRunner(address)
    model_runner.payout_v0_NeuralNet("E0")

    # TIMER DONE
    end = time.time()
    logging.info(str(end - start) + " seconds")
    return str(end - start)


# Call to main, GCP does this implicitly
if __name__ == '__main__':
    main("")
