import argparse
import csv
from datetime import datetime
from io import StringIO
import os
import logging

import boto3
from sodapy import Socrata

from config import EXPENSES_FIELD_MAPPING
from config import EXPENSES_ID_COLUMN
from config import EXPENSES_NUMERIC_COLS
from config import REVENUE_FIELD_MAPPING
from config import REVENUE_ID_COLUMN
from config import REVENUE_NUMERIC_COLS

import utils

# AWS Credentials
AWS_ACCESS_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET_NAME")

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
EXP_DATASET = os.getenv("EXP_DATASET")
REV_DATASET = os.getenv("REV_DATASET")

def select_month(year, month):
    """
    Parameters
    ----------
    year : Int
        Argument provided value for year.
    month : Int
        Argument provided value for month.
    Returns
    -------
    f_year : int
        Selected Year
    f_month : int
        Selected Month
    """
    # If args are missing, default to current month and/or year
    if not year:
        f_year = datetime.now().year
    else:
        f_year = year

    if not month:
        f_month = datetime.now().month
    else:
        f_month = month

    return f_year, f_month


def list_s3_files(s3_client, subdir):
    file_names = []

    # List objects in the bucket
    response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=subdir)

    # Add file names to the list
    for obj in response.get("Contents", []):
        file_names.append(obj["Key"])

    return file_names


def get_csv_data(s3_client, filename, field_mapping):
    response = s3_client.get_object(Bucket=BUCKET, Key=filename)
    csv_content = response["Body"].read().decode("utf-8")
    csv_data = csv.DictReader(StringIO(csv_content))

    # Field mapping to Socrata columns
    mapped_data = []
    for row in csv_data:
        mapped_row = {}
        for original_key, new_key in field_mapping.items():
            mapped_row[new_key] = row.get(original_key)
        mapped_data.append(mapped_row)

    return mapped_data

def get_fiscal_year(year, month):
    if month >= 10:
        fiscal_year = year + 1
    else:
        fiscal_year = year

    return fiscal_year


def get_fiscal_month(month):
    if month >= 10:
        fiscal_month = month - 9
    else:
        fiscal_month = month + 3

    return fiscal_month


def create_row_identifier(row, id_cols):
    output = ""
    for col in id_cols:
        output = f"{output}{str(row[col])}"
    return output


def main(args):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    soda = Socrata(SO_WEB, SO_TOKEN, username=SO_KEY, password=SO_SECRET, timeout=60 * 15, )

    year, month = select_month(args.year, args.month)
    if args.replace:
        logger.info(f"doing full replacement of data in Socrata")
    else:
        logger.info(f"args: year = {year}, month = {month}")
    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year

    selected_months = [f"{year}-{month}", f"{prev_year}-{prev_month}"]

    # Expenses
    payload = []
    files = list_s3_files(s3_client, "expenses/")
    for f in files:
        f_year = int(f[9:13])
        f_month = int(f[14:16])
        if f"{f_year}-{f_month}" in selected_months or args.replace:
            data = get_csv_data(s3_client, f, EXPENSES_FIELD_MAPPING)

            for row in data:
                row["year"] = f_year
                row["month"] = f_month

                row["department"] = int(f[-8:-4])

                # Derived fields
                row["month_name"] = datetime(2022, row["month"], 1).strftime("%b")
                row["fiscal_year"] = get_fiscal_year(row["year"], row["month"])
                row["fiscal_month"] = get_fiscal_month(row["month"])
                row["row_identifier"] = create_row_identifier(row, EXPENSES_ID_COLUMN)

                # Convert empty strings to None for numeric fields
                for key in EXPENSES_NUMERIC_COLS:
                    if row[key] == "":
                        row[key] = None

            res = soda.upsert(EXP_DATASET, data)
            logger.info(f"Expenses Socrata Response: {f},{res}")

    # Revenue
    files = list_s3_files(s3_client, "revenue/")
    for f in files:
        f_year = int(f[8:12])
        f_month = int(f[13:15])
        if f"{f_year}-{f_month}" in selected_months or args.replace:
            data = get_csv_data(s3_client, f, REVENUE_FIELD_MAPPING)
            for row in data:
                row["year"] = f_year
                row["month"] = f_month
                row["department"] = int(f[-8:-4])

                # Derived fields
                row["month_name"] = datetime(2022, row["month"], 1).strftime("%b")
                row["fiscal_year"] = get_fiscal_year(row["year"], row["month"])
                row["fiscal_month"] = get_fiscal_month(row["month"])
                row["row_identifier"] = create_row_identifier(row, REVENUE_ID_COLUMN)

                # Convert empty strings to None for numeric fields
                for key in REVENUE_NUMERIC_COLS:
                    if row[key] == "":
                        row[key] = None

            res = soda.upsert(REV_DATASET, data)
            logger.info(f"Revenue Socrata Response: {f},{res}")


if __name__ == "__main__":
    # CLI arguments definition
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--year",
        type=int,
        help=f"Calendar year of report to run, defaults to current year",
    )

    parser.add_argument(
        "--month",
        type=int,
        help=f"Calendar month of report. defaults to current month",
    )

    parser.add_argument(
        "--replace",
        type=bool,
        help="Replaces all of the data in Socrata",
        default=False,
    )

    args = parser.parse_args()

    logger = utils.get_logger(__name__, level=logging.INFO,)

    main(args)

