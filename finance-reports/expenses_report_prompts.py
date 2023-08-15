import argparse
import calendar
from datetime import datetime
from io import StringIO
import os

import boto3
from mstrio.api.reports import report_instance, get_prompted_instance
from mstrio.project_objects.report import Report
from mstrio.connection import Connection

BASE_URL = os.getenv("BASE_URL")
MSTRO_USERNAME = os.getenv("MSTRO_USERNAME")
MSTRO_PASSWORD = os.getenv("MSTRO_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")

# AWS Credentials
AWS_ACCESS_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET_NAME")

EXP_REPORT_ID = "1C804F8891479811944EF68F99835649"
REV_REPORT_ID = "FBC5E5F30744717D7079ADADB956C3BC"

conn = Connection(
    base_url=BASE_URL,
    username=MSTRO_USERNAME,
    password=MSTRO_PASSWORD,
    project_id=PROJECT_ID,
    login_mode=1,
)

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

def get_month_date(year, month):
    """
    Returns the last day of the month as a string for the given year-month integers
    """
    # Get last day of the month
    last_day = calendar.monthrange(year, month)[1]
    # Convert to string
    return datetime(year, month, last_day).strftime("%Y-%m-%d")

def get_fiscal_year(year, month):
    if month >= 10:
        fiscal_year = year + 1
    else:
        fiscal_year = year

    return fiscal_year

def build_todos(year, month, date_str):
    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year

    prev_date = get_month_date(prev_year, prev_month)
    # ATD = 2400, TPW = 6200
    departments = ["2400", "6200"]
    months = [prev_month, month]
    fys = [get_fiscal_year(prev_year, prev_month), get_fiscal_year(year, month)]
    dates = [prev_date, date_str]

    todos = []
    item = {}
    for dept in departments:
        for i in range(0, 2):
            item["department"] = dept
            item["month"] = months[i]
            item["fy"] = fys[i]
            item["date"] = dates[i]
            todos.append(item)
            item = {}

    return todos


def expenses_prompts(fy, date, dept, prompts):
    """
    date must be in the form of yyyy-mm-dd
    """

    prompt_answers = {
        "prompts": [
            # Department
            {"id": prompts[0]["id"], "type": "VALUE", "answers": dept},
            # Fiscal Year
            {
                "id": prompts[1]["id"],
                "type": "ELEMENTS",
                "answers": [
                    {"id": f"h{fy};{prompts[1]['source']['id']}", "name": f"{fy}"}
                ],
            },
            # Date
            {
                "id": prompts[2]["id"],
                "type": "VALUE",
                "answers": f"{date}T05:00:00.000+0000",
            },
            # Budget Fiscal Year
            {
                "id": prompts[3]["id"],
                "type": "ELEMENTS",
                "answers": [
                    {"id": f"h{fy};{prompts[3]['source']['id']}", "name": f"{fy}"}
                ],
            },
        ]
    }
    return prompt_answers

def revenue_prompts(fy, date, dept, prompts):
    prompt_answers = {
        "prompts": [
            # Department
            {"id": prompts[0]["id"], "type": "VALUE", "answers": dept},
            # Fiscal Year
            {
                "id": prompts[1]["id"],
                "type": "ELEMENTS",
                "answers": [
                    {"id": f"h{fy};{prompts[1]['source']['id']}", "name": f"{fy}"}
                ],
            },
            # Date
            {
                "id": prompts[2]["id"],
                "type": "VALUE",
                "answers": f"{date}T05:00:00.000+0000",
            },
            # Budget Fiscal Year
            {
                "id": prompts[4]["id"],
                "type": "ELEMENTS",
                "answers": [
                    {"id": f"h{fy};{prompts[4]['source']['id']}", "name": f"{fy}"}
                ],
            },
        ]
    }

    return prompt_answers

def get_report_data(prompt_answers, report_id, instance_id):
    # Send answers
    res = conn.put(
        url=conn.base_url
        + f"/api/reports/{report_id}/instances/{instance_id}/prompts/answers",
        json=prompt_answers,
    )

    # Download report results to dataframe
    report = Report(conn, id=report_id, instance_id=instance_id)
    df = report.to_dataframe()
    return df


def expense_data(fy, date_str, dept):
    # Expense data
    instance_id = report_instance(conn, report_id=EXP_REPORT_ID).json()["instanceId"]
    # Get the prompts required by this report
    prompts = get_prompted_instance(
        conn, report_id=EXP_REPORT_ID, instance_id=instance_id
    ).json()
    # Get prompt answers
    prompt_answers = expenses_prompts(fy, date_str, dept, prompts)
    df = get_report_data(prompt_answers, EXP_REPORT_ID, instance_id)

    return df

def revenue_data(fy, date_str, dept):
    # Expense data
    instance_id = report_instance(conn, report_id=REV_REPORT_ID).json()["instanceId"]
    # Get the prompts required by this report
    prompts = get_prompted_instance(
        conn, report_id=REV_REPORT_ID, instance_id=instance_id
    ).json()
    # Get prompt answers
    prompt_answers = revenue_prompts(fy, date_str, dept, prompts)
    df = get_report_data(prompt_answers, REV_REPORT_ID, instance_id)

    return df


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
    year, month = select_month(args.year, args.month)
    date_str = get_month_date(year, month)
    todos = build_todos(year, month, date_str)

    s3_resource = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    for task in todos:

        filename = f"expenses/{task['date']}_{task['department']}"
        df = expense_data(task["fy"], task["date"], task["department"])
        df_to_s3(df, s3_resource, filename)

        filename = f"revenue/{task['date']}_{task['department']}"
        df = revenue_data(task["fy"], task["date"], task["department"])
        df_to_s3(df, s3_resource, filename)

# CLI arguments definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--year", type=int, help=f"Year of folder to select, defaults to current year",
)

parser.add_argument(
    "--month", type=int, help=f"Month of folder to select. defaults to current month",
)

args = parser.parse_args()

main(args)