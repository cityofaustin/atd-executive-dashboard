import csv
from io import StringIO
import os
import logging

import boto3
from sodapy import Socrata

from config import EXPENSES_FIELD_MAPPING
from config import EXPENSES_NUMERIC_COLS
from config import MONTH_LOOKUP
from config import REVENUE_FIELD_MAPPING
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


def main():
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    soda = Socrata(SO_WEB, SO_TOKEN, username=SO_KEY, password=SO_SECRET, timeout=60*15,)

    # Expenses
    payload = []
    files = list_s3_files(s3_client, "expenses/")
    for f in files:
        data = get_csv_data(s3_client, f, EXPENSES_FIELD_MAPPING)

        for row in data:
            row["year"] = int(f[9:13])
            row["month"] = int(f[14:16])
            row["department"] = int(f[-8:-4])

            # Derived fields
            row["month_name"] = MONTH_LOOKUP[row["month"]]
            row["fiscal_year"] = get_fiscal_year(row["year"], row["month"])
            row["fiscal_month"] = get_fiscal_month(row["month"])

            # Convert empty strings to None for numeric fields
            for key in EXPENSES_NUMERIC_COLS:
                if row[key] == "":
                    row[key] = None

        payload.extend(data)

    res = soda.replace(EXP_DATASET, payload)
    logger.info(f"Expenses Socrata Response: {res}")

    # Revenue
    payload = []
    files = list_s3_files(s3_client, "revenue/")
    for f in files:
        data = get_csv_data(s3_client, f, REVENUE_FIELD_MAPPING)

        for row in data:
            row["year"] = int(f[8:12])
            row["month"] = int(f[13:15])
            row["department"] = int(f[-8:-4])

            # Derived fields
            row["month_name"] = MONTH_LOOKUP[row["month"]]
            row["fiscal_year"] = get_fiscal_year(row["year"], row["month"])
            row["fiscal_month"] = get_fiscal_month(row["month"])

            # Convert empty strings to None for numeric fields
            for key in REVENUE_NUMERIC_COLS:
                if row[key] == "":
                    row[key] = None

        payload.extend(data)

    res = soda.replace(REV_DATASET, payload)
    logger.info(f"Revenue Socrata Response: {res}")


if __name__ == "__main__":
    logger = utils.get_logger(__name__, level=logging.INFO,)
    main()
