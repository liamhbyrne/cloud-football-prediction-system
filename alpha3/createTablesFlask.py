import psycopg2
import logging
import os

def setUpDatabase():
    """
    Creates all tables in database, only called manually
    """
    address: str = os.environ.get('DB_ADDRESS')
    conn = None
    try:
        conn = psycopg2.connect(address)
    except psycopg2.OperationalError:
        logging.error("Failed to connect to DB")
        exit(1)

    with conn:
        with conn.cursor() as cursor:
            cursor.execute(open("tables.sql", "r").read())
            conn.commit()

    return "tables created"

