from mstrio.api.reports import report_instance, get_prompted_instance
from mstrio.project_objects.report import Report
from mstrio.connection import Connection

import os

BASE_URL = os.getenv("BASE_URL")
MSTRO_USERNAME = os.getenv("MSTRO_USERNAME")
MSTRO_PASSWORD = os.getenv("MSTRO_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")

REPORT_ID = "E1156650EC43C0F647D1C89E51E52852"

conn = Connection(
    base_url=BASE_URL,
    username=MSTRO_USERNAME,
    password=MSTRO_PASSWORD,
    project_id=PROJECT_ID,
    login_mode=1,
)

instance_id = report_instance(conn, report_id=REPORT_ID).json()["instanceId"]
prompts = get_prompted_instance(
    conn, report_id=REPORT_ID, instance_id=instance_id
).json()
print(prompts)

prompt_answers = {
    "prompts": [
        {"id": "71AFE887BE4A4FF88CBE3A8E79B692B8", "type": "VALUE", "answers": "2400"},
        {
            "id": "30BB5363448007F721216B95C55709B3",
            "type": "ELEMENTS",
            "answers": [
                {"id": "h2022;7CC8145F4591C84C567D60B236792F33", "name": "2022"}
            ],
        },
        {
            "id": "5554FDDB4204EA8EBAD8D9A1DC46D8FE",
            "type": "VALUE",
            "answers": "2022-08-31T05:00:00.000+0000",
        },
        {
            "id": "F4435FAF4C2DC68383051CB1CE2B19A0",
            "type": "ELEMENTS",
            "answers": [
                {"id": "h2022;502D5DEB40B89AA9B51DB2B48F84D1E1", "name": "2022"}
            ],
        },
    ]
}

res = conn.put(
    url=conn.base_url
    + f"/api/reports/{REPORT_ID}/instances/{instance_id}/prompts/answers",
    json=prompt_answers,
)

# res = conn.get(url=conn.base_url + f'/api/v2/reports/{REPORT_ID}/instances/{instance_id}')
report = Report(conn, id=REPORT_ID, instance_id=instance_id)
df = report.to_dataframe()
print(res)
