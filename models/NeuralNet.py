import logging

import tensorflow as tf
from tensorflow import keras
import numpy as np

logging.basicConfig(level=logging.INFO)


class NeuralNet:

    def __init__(self):
        self._model = tf.keras.Sequential([
            keras.layers.Dense(units=6, input_shape=(6,)),
            keras.layers.Dense(units=50, activation=tf.nn.leaky_relu),
            keras.layers.Dense(units=25, activation=tf.nn.leaky_relu),
            keras.layers.Dense(units=3, activation=tf.nn.softmax)
        ])
        self._model.summary()  # Outputs schema of model to console

    def compileModel(self, loss_function: str = 'sparse_categorical_crossentropy', metrics: str = 'accuracy'):
        logging.info("Compiling model . . .")
        optimizer = tf.keras.optimizers.Adam()
        self._model.compile(optimizer=optimizer,
                            loss=loss_function,
                            metrics=[metrics])
        logging.info("Compilation complete.")

    def fitModel(self, x_train, y_train, epochs):
        logging.info("Training . . .")
        self._model.fit(x_train, y_train, epochs=epochs, batch_size=32)

    def evaluateAccuracy(self, x_test, y_test) -> float:
        logging.info("Evaluating . . .")
        loss, test_statistic = self._model.evaluate(x_test, y_test)
        return test_statistic

    def predictOutcome(self, features):
        """
        Feeds the model a feature vector to make a prediction on
        :param features: NumPy array [10,] feature vector
        :return: Integer of the result [0,1,2]
        """
        probability = self._model.predict(features.reshape(1, 6))
        prediction = np.argmax(probability,axis=1)

        return probability[0], prediction
