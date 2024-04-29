import argparse
from datetime import datetime, timedelta
import os
import logging

import dateutil.parser
import oracledb as cx_Oracle
from sodapy import Socrata

from queries import query_template
import utils

# Maximo data warehouse DB Credentials
HOST = os.getenv("MAXIMO_HOST")
PORT = os.getenv("MAXIMO_PORT")
SERVICE_NAME = os.getenv("MAXIMO_SERVICE_NAME")
USER = os.getenv("MAXIMO_DB_USER")
PASSWORD = os.getenv("MAXIMO_DB_PASS")

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = "hjym-dxqr"

SOCRATA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def process_date_arguments(args):
    if args.start:
        start = dateutil.parser.parse(args.start)
    if not args.start:
        # default to 7 days ago
        start = datetime.now() - timedelta(days=7)

    if args.end:
        end = dateutil.parser.parse(args.end)
    if not args.end:
        # default to today
        end = datetime.now()

    return datetime.strftime(start, "%m/%d/%Y"), datetime.strftime(end, "%m/%d/%Y")


def get_conn():
    """
    Get connected to the Maximo data warehouse database

    Returns
    -------
    cx_Oracle Connection Object

    """
    dsn_tns = cx_Oracle.makedsn(HOST, PORT, service_name=SERVICE_NAME)
    return cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn_tns)


def row_factory(cursor):
    """
    Define cursor row handler which returns each row as a dict
    h/t https://stackoverflow.com/questions/35045879/cx-oracle-how-can-i-receive-each-row-as-a-dictionary

    Parameters
    ----------
    cursor : cx_Oracle Cursor object

    Returns
    -------
    function: the rowfactory.

    """
    return lambda *args: dict(zip([d[0] for d in cursor.description], args))


def transform_datetime_columns(data):
    """
    Transform datetime columns to the format expected by Socrata.
    Parameters
    ----------
    data: list of dicts of the data fetched from Maximo.

    Returns
    -------
    data: list of dicts with the dates modified.

    """
    for row in data:
        for key in row:
            # converts all datetime objects to the correct format.
            if isinstance(row[key], datetime):
                row[key] = row[key].strftime(SOCRATA_DATE_FORMAT)

    return data


def data_to_socrata(soda, data):
    """
    Replaces all the data in the socrata dataset with data in the dataframe.

    Parameters
    ----------
    soda: sodapy client object
    data : List of dicts from Oracle DB

    """
    res = soda.upsert(DATASET, data)
    return res


def main(args):
    # process CLI args
    start, end = process_date_arguments(args)
    logger.info(f"Getting work orders with start: {start}, end: {end}")
    query_formatted = query_template.format(start=start, end=end)

    # Connect to Maximo data warehouse
    conn = get_conn()
    cursor = conn.cursor()

    # Execute query
    cursor.execute(query_formatted)
    cursor.rowfactory = row_factory(cursor)
    rows = cursor.fetchall()
    conn.close()

    # Convert datetime fields to format accepted by Socrata
    rows = transform_datetime_columns(rows)
    if rows:
        logger.info(f"{len(rows)} work orders found")

        # Upsert to Socrata
        soda = Socrata(SO_WEB, SO_TOKEN, username=SO_KEY, password=SO_SECRET, timeout=500, )
        res = data_to_socrata(soda, rows)
        logger.info(res)
    else:
        logger.info("No work orders found")


# CLI argument definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--start",
    type=str,
    required=False,
    help="Start modified date of the work orders to download, defaults to 7 days ago.",
)

parser.add_argument(
    "--end",
    type=str,
    required=False,
    help="End changed date of the work orders to download, defaults to now.",
)

args = parser.parse_args()

logger = utils.get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main(args)
