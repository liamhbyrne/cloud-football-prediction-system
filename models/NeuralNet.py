import logging

import tensorflow as tf
from tensorflow import keras

logging.basicConfig(level=logging.INFO)

class NeuralNet:

    def __init__(self):
        self._model = tf.keras.Sequential([
            keras.layers.Dense(units=8, input_shape=(8,)),
            keras.layers.Dense(units=50, activation=tf.nn.leaky_relu),
            keras.layers.Dense(units=25, activation=tf.nn.leaky_relu),
            keras.layers.Dense(units=3, activation=tf.nn.softmax)
        ])
        self._model.summary()  # Outputs schema of model to console

    def compileModel(self, loss_function : str = 'sparse_categorical_crossentropy', metrics : str = 'accuracy'):
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
