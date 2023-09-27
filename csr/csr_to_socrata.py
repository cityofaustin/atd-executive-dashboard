"""
Downloads CSR data from a CSV report endpoint and then uploads the data to a Socrata dataset
"""

import os
import logging

import pandas as pd
import geopandas as gpd
import numpy as np
from sodapy import Socrata

import utils

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("CSR_DATASET")

# CSR CSV data endpoint
ENDPOINT = os.getenv("CSR_ENDPOINT")

FIELD_MAPPING = {
    "Service Request (SR) Number": "service_request_sr_number",
    "Department": "department",
    "Group Description": "group_description",
    "SR Description": "sr_description",
    "Method Received": "method_received",
    "SR Status": "sr_status",
    "Is Duplicate? (1/0)": "is_duplicate_1_0",
    "Status Change Date": "status_change_date",
    "Created Date": "created_date",
    "Overdue On Date": "overdue_on_date",
    "Last Update Date": "last_update_date",
    "Close Date": "close_date",
    "SR Age (days)": "sr_age_days",
    "Response Days": "response_days",
    "Open Count": "open_count",
    "Closed Count": "closed_count",
    "Overdue Count": "overdue_count",
    "Closed on Time": "closed_on_time",
    "Closed Late": "closed_late",
    "# of Days Late": "of_days_late",
    "SR Location": "sr_location",
    "State Plane X Coordinate": "state_plane_x_coordinate",
    "State Plane Y Coordinate": "state_plane_y_coordinate",
    "location": "location",
    "fiscal_year": "fiscal_year",
}


def extract():
    df = pd.read_csv(ENDPOINT, sep="\t", encoding="utf_16")
    logger.info(f"Downloaded {len(df)} CSRs from endpoint")
    return df


def convert_from_state_plane(df):
    """
    Adds WGS-84 lat/long columns to the dataframe based on the state plane coordinates.
    """
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(
            x=df["State Plane X Coordinate"], y=df["State Plane Y Coordinate"]
        ),
        crs="ESRI:102739",
    )
    gdf = gdf.to_crs("EPSG:4326")

    # Get wgs84 location columns
    df["latitude"] = gdf["geometry"].y
    df["longitude"] = gdf["geometry"].x
    df["location"] = df.apply(build_point_data, axis=1)
    return df


def build_point_data(row):
    """

    Parameters
    ----------
    row: dict of data from a pandas dataframe

    Returns
    -------
    None if there is no location data given, or point datatype formatted as expected by Socrata.

    """
    if pd.isna(row["longitude"]) or pd.isna(row["latitude"]):
        return None
    return f"POINT ({row['longitude']} {row['latitude']})"


def get_fiscal_year(row):
    """
    Returns the fiscal year based on the created date of the record
    """
    year = row["datetime"].year
    month = row["datetime"].month
    if month >= 10:
        fiscal_year = year + 1
    else:
        fiscal_year = year

    return fiscal_year


def transform(df):
    logger.info("Transforming CSR data")
    df = convert_from_state_plane(df)

    # Converting datetime to correct format for socrata
    date_cols = [
        "Status Change Date",
        "Created Date",
        "Overdue On Date",
        "Last Update Date",
        "Close Date",
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
        df[col] = df[col].where(pd.notnull(df[col]), None)

    df["datetime"] = pd.to_datetime(df["Created Date"])
    df["fiscal_year"] = df.apply(get_fiscal_year, axis=1)

    # Field mapping
    df = df[list(FIELD_MAPPING.keys())]
    df.rename(columns=FIELD_MAPPING, inplace=True)

    # Setting these NaN values to None, because Socrata will be mad otherwise
    df["state_plane_x_coordinate"] = df["state_plane_x_coordinate"].replace(
        {np.nan: None}
    )
    df["state_plane_y_coordinate"] = df["state_plane_y_coordinate"].replace(
        {np.nan: None}
    )
    df["sr_location"] = df["sr_location"].replace({np.nan: None})

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
