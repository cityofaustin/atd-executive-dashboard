from mstrio.api.reports import report_instance, get_prompted_instance
from mstrio.project_objects.report import Report
from mstrio.connection import Connection

import os

BASE_URL = os.getenv("BASE_URL")
MSTRO_USERNAME = os.getenv("MSTRO_USERNAME")
MSTRO_PASSWORD = os.getenv("MSTRO_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")

EXP_REPORT_ID = "1C804F8891479811944EF68F99835649"
REV_REPORT_ID = "FBC5E5F30744717D7079ADADB956C3BC"

conn = Connection(
    base_url=BASE_URL,
    username=MSTRO_USERNAME,
    password=MSTRO_PASSWORD,
    project_id=PROJECT_ID,
    login_mode=1,
)

def expenses_prompts(fy, date, prompts):
    """
    date must be in the form of yyyy-mm-dd
    """

    prompt_answers = {
        "prompts": [
            # Department
            {"id": prompts[0]["id"], "type": "VALUE", "answers": "2400"},
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

def revenue_prompts(fy, date, prompts):
    prompt_answers = {
        "prompts": [
            # Department
            {"id": prompts[0]["id"], "type": "VALUE", "answers": "2400"},
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


def expense_data():
    # Expense data
    instance_id = report_instance(conn, report_id=EXP_REPORT_ID).json()["instanceId"]
    # Get the prompts required by this report
    prompts = get_prompted_instance(
        conn, report_id=EXP_REPORT_ID, instance_id=instance_id
    ).json()
    # Get prompt answers
    prompt_answers = expenses_prompts(2023, "2023-06-30", prompts)
    df = get_report_data(prompt_answers, EXP_REPORT_ID, instance_id)

    return df

def revenue_data():
    # Expense data
    instance_id = report_instance(conn, report_id=REV_REPORT_ID).json()["instanceId"]
    # Get the prompts required by this report
    prompts = get_prompted_instance(
        conn, report_id=REV_REPORT_ID, instance_id=instance_id
    ).json()
    # Get prompt answers
    prompt_answers = revenue_prompts(2023, "2023-06-30", prompts)
    df = get_report_data(prompt_answers, REV_REPORT_ID, instance_id)

    return df


def main():
    print(expense_data())
    print(revenue_data())

main()