import logging

from flask import request, Flask


app = Flask(__name__)

# Enables Info logging to be displayed on console
logging.basicConfig(level=logging.INFO)


@app.route("/create-table")
def createTableRoute():
    createTablesFlask.setUpDatabase()



