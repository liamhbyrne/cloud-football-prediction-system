import psycopg2
import os

def setUpDatabase():
    """
    Creates all tables in database, only called manually
    """
    address: str = os.environ.get('DB_ADDRESS')
    if address is None:
        return "DB address not provided in environment", 400

    try:
        conn = psycopg2.connect(address)
    except psycopg2.OperationalError:
        return "Failed to connect to DB", 500

    with conn:  # with keyword closes connection after execution
        with conn.cursor() as cursor:
            cursor.execute(open("tables.sql", "r").read())
            conn.commit()
