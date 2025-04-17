import requests, json, csv, tomllib
from datetime import datetime
from models import Time, Employee, Timesheet_Entry
from sqlalchemy import select
import sqlalchemy
from pathlib import Path
from posting_log_ingest import get_posted_keys


def post_timesheets_to_CMiC():
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)

    #establish CMiC API
    host_url = endpoint["CMiC_Base"]["base_url"]
    username = endpoint["CMiC_Base"]["username"]
    password = endpoint["CMiC_Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)

    posted_entries = get_posted_keys()

    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    results = []

    with sqlalchemy.orm.Session(engine) as session:
        emp_stmt = select(Employee).where(Employee.PaygroupId == 18)#.where(Employee.EmpId == "000223310-PQCD4")
        employees: list[Employee] = session.execute(emp_stmt).scalars()
        with requests.Session() as s:
            for employee in employees:
                if not employee.time_entries:
                    print(f"Employee {employee.EmpId} {employee.LastName}, {employee.FirstName} has no time in current pay period")
                    continue
                # joined_stmt = select(Time).where(Time.EmpId == employee.EmpId)
                # timesheets = session.execute(joined_stmt)
                s.auth = my_auth
                endpoint = r'hcm-rest-api/rest/1/pyemptimesheet'

                for t in employee.time_entries:
                    entry = Timesheet_Entry(t)
                    entry_key = (entry.TshEmpNo.strip(), entry.TshDate.strip(), entry.TshPrnCode.strip())

                    if entry_key in posted_entries:
                        print(f"Already Posted, {entry_key} - skipping")
                        results.append({
                            "EmpNo": entry.TshEmpNo,
                            "WorkDate": entry.TshDate,
                            "PrnCode": entry.TshPrnCode,
                            "Status": "SKIPPED",
                            "Response": "{}"
                        })
                        continue

                    payload = entry.__dict__.copy()

                    try:
                        r = s.post(f"{host_url}/{endpoint}", json=payload)
                        status = r.status_code
                        resp = r.text
                    except Exception as e:
                        status = "ERROR"
                        resp = str(e)

                    print(json.dumps(payload, indent=4))

                    results.append({
                        "EmpNo": entry.TshEmpNo,
                        "WorkDate": entry.TshDate,
                        "PrnCode": entry.TshPrnCode,
                        "Status": status,
                        "Response": resp
                    })

    timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
    output_file = f"DataFiles/PostResult/PostResults_{timestamp}.csv"

    source_name = output_file.split("/")[-1]

    with open(output_file, mode="w", newline="") as csvfile:
        fieldnames = ["EmpNo", "WorkDate", "PrnCode", "Status", "Response", "SourceFile"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            row["SourceFile"] = source_name
            writer.writerow(row)
            

    print(f"Post log written to {output_file}")


if __name__ == "__main__":
    post_timesheets_to_CMiC()
