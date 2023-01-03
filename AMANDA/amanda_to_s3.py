import argparse
from io import StringIO
import os

import cx_Oracle
import pandas as pd
import boto3

# For local dev:
# from dotenv import load_dotenv
# load_dotenv(".env")

# AMANDA RR DB Credentials
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
SERVICE_NAME = os.getenv("SERVICE_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

QUERIES = {
    "permit_requests": "SELECT Foldertype,subcode,TO_CHAR(ROUND(issuedate, 'DAY'), 'YYYY-MM-DD'),COUNT(1) IssuedROWPermits FROM folder WHERE foldertype IN('DS', 'RW', 'EX') AND issuedate >= TO_DATE('10-01-2018', 'mm-dd-yyyy') GROUP BY TO_CHAR(ROUND(issuedate, 'DAY'), 'YYYY-MM-DD'), Foldertype, subcode",
}


def get_conn():
    """
    Get connected to the AMANDA Read replica database

    Returns
    -------
    cx_Oracle Connection Object

    """
    # Need to run this once if you want to work locally
    # Change lib_dir to your cx_Oracle library location
    # https://stackoverflow.com/questions/56119490/cx-oracle-error-dpi-1047-cannot-locate-a-64-bit-oracle-client-library
    # lib_dir = r"/Users/charliehenry/instantclient_19_8"
    # cx_Oracle.init_oracle_client(lib_dir=lib_dir)

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


def df_to_s3(df, resource, filename):
    """
    Send pandas dataframe to an S3 bucket as a CSV
    h/t https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python
    
    Parameters
    ----------
    df : Pandas Dataframe
    resource : boto3 s3 resource
    filename : String of the file that will be created in the S3 bucket ex:
    
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    resource.Object(BUCKET, f"{filename}.csv").put(Body=csv_buffer.getvalue())


def main(args):
    # Connect to AMANDA RR DB
    conn = get_conn()
    cursor = conn.cursor()

    # Execute our query
    cursor.execute(QUERIES[args.query])
    cursor.rowfactory = row_factory(cursor)
    rows = cursor.fetchall()
    conn.close()

    # Upload to S3
    s3_resource = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )
    df = pd.DataFrame(rows)
    df_to_s3(df, s3_resource, args.query)


# CLI argument definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--query",
    choices=list(QUERIES.keys()),
    required=True,
    help="Name of the query defined at the top of this script. Ex: permit_requests",
)

args = parser.parse_args()

if __name__ == "__main__":
    main(args)
