"""
Downloads flex question/answers for CSRs data from a CSV report endpoint and then uploads the data to a Socrata dataset
"""

import os
import logging

import pandas as pd
from sodapy import Socrata

import utils

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("FLEX_NOTES_DATASET")

# CSR CSV data endpoint
ENDPOINT = os.getenv("FLEX_NOTE_ENDPOINT")

FIELD_MAPPING = {
    "Flex Notes ID": "flex_note_id",
    "Service Request Number": "service_request_number",
    "Flex Notes Definition ID": "flex_notes_definition_id",
    "Service Request ID": "service_request_id",
    "Flex Question": "flex_question",
    "Flex Answer": "flex_answer",
}

# Questions we do not want to store
IGNORED_QUESTIONS = [
    "Mobile Apps Reporter Information",
    "Provide license plate number of vehicle, if known.",
    "*What are the last 4 digits of the card number that you used for this transaction?",
    "Enter APD  # if known (Case #, Officer #, etc.).",
    "Provide User ID from ParkATX App (If user ID is unavailable, type N/A)",
    "What are the last 4 digits of the card number that you used for this transaction?",
    "What is the 9-digit citation number (top line)?",
]


def extract():
    try:
        df = pd.read_excel(ENDPOINT)
    except UnicodeError as e:
        logger.info("Unexpected file type returned from the report endpoint. Check that you are on the city network. "
                    "It's likely that your request is getting flagged as a bot by the web app firewall.")
        raise e
    except Exception as e:
        raise e
    logger.info(f"Downloaded {len(df)} Flex Notes from endpoint")
    return df


def transform(df):
    logger.info("Transforming Flex Notes")

    # Field mapping
    df = df[list(FIELD_MAPPING.keys())]
    df.rename(columns=FIELD_MAPPING, inplace=True)

    # Dropping unwanted questions from our dataset
    df = df[~df["flex_question"].isin(IGNORED_QUESTIONS)]

    # Replacing missing values with None instead of the default NaN pandas uses
    df.replace({pd.NA: None}, inplace=True)

    payload = df.to_dict("records")
    return payload


def load(client, data):
    logger.info("Uploading CSR data to Socrata")
    res = client.upsert(DATASET, data)
    logger.info(res)
    return res


def main():
    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=500,
    )

    data = extract()
    data = transform(data)
    res = load(soda, data)

    return res


if __name__ == "__main__":
    logger = utils.get_logger(
        __name__,
        level=logging.INFO,
    )
    main()
