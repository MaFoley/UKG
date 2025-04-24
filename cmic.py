import requests, json, csv, tomllib
import pandas as pd
from datetime import datetime
from models import Time, Employee, Timesheet_Entry, JCJobCategory
from sqlalchemy import select
import sqlalchemy
from pathlib import Path
from posting_log_ingest import get_posted_keys
from CMiC_Project_Import import cmic_api_results

UKGHJRCOMPANYCODE = 'PQCD4'
def create_session() -> tuple[str, requests.auth]:
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    with open("secrets.toml","rb") as f:
        endpoint.update(tomllib.load(f))

    #establish CMiC API
    host_url = endpoint["CMiC_Base"]["host_url"]
    username = endpoint["CMiC_Base"]["username"]
    password = endpoint["CMiC_Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    return host_url, my_auth



def post_timesheets_to_CMiC(testing: bool=False) -> pd.DataFrame:
    #establish CMiC API
    host_url, my_auth = create_session()
    posted_entries = get_posted_keys() if testing == False else {}

    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    results = []
    retry_time_entries = []
    with sqlalchemy.orm.Session(engine) as session:
        emp_stmt = select(Employee).where(Employee.Active == 'A').where(Employee.PaygroupId == 18).where(Employee.OrgLevel3Id !=2)#.where(Employee.EmpId == "000223310-PQCD4")
        employees: list[Employee] = session.execute(emp_stmt).scalars()
        with requests.Session() as s:
            s.auth = my_auth
            endpoint = r'hcm-rest-api/rest/1/pyemptimesheet'
            for employee in employees:
                if not employee.time_entries:
                    print(f"Employee {employee.EmpId} {employee.LastName}, {employee.FirstName} has no time in current pay period")
                    continue
                elif employee.companyCode() != UKGHJRCOMPANYCODE:
                    print(f"Employee {employee.EmpId} not in HJR")
                    continue
                # joined_stmt = select(Time).where(Time.EmpId == employee.EmpId)
                # timesheets = session.execute(joined_stmt)
                for t in employee.time_entries:
                    entry = Timesheet_Entry(t)
                    entry_key = (entry.TshEmpNo.strip(), entry.TshDate.strip(), entry.TshPrnCode.strip())

                    if entry_key in posted_entries:
                        # print(f"Already Posted, {entry_key} - skipping")
                        results.append({
                            "EmpNo": entry.TshEmpNo,
                            "WorkDate": entry.TshDate,
                            "PrnCode": entry.TshPrnCode,
                            "Status": "SKIPPED",
                            "Response": "{}",
                            "Hours": entry.TshNormalHours
                        })
                        continue

                    payload = entry.__dict__.copy()

                    try:
                        # jobcodecostcode = JCJobCategory(entry)
                        # print(jobcodecostcode)
                        r = s.post(f"{host_url}/{endpoint}", json=payload)
                        status = r.status_code
                        resp = r.text
                    except Exception as e:
                        status = "ERROR"
                        resp = str(e)
                    if status == 400 and "Cost Code Code".lower() in resp.lower():
                        retry_time_entries.append(entry)
                    # print(json.dumps(payload,indent=None))

                    results.append({
                        "EmpNo": entry.TshEmpNo,
                        "WorkDate": entry.TshDate,
                        "PrnCode": entry.TshPrnCode,
                        "Status": status,
                        "Response": resp,
                        "Hours": entry.TshNormalHours
                    })
            for retry_entry in retry_time_entries:
                jobcodecostcode = JCJobCategory(retry_entry)
                r = s.post(f"{host_url}/jc-rest-api/rest/1/jcjobcategory"
                            , json=jobcodecostcode.__dict__.copy())
                entry_key = (retry_entry.TshEmpNo.strip(), retry_entry.TshDate.strip(), retry_entry.TshPrnCode.strip())

                if entry_key in posted_entries:
                    # print(f"Already Posted, {entry_key} - skipping")
                    results.append({
                        "EmpNo": retry_entry.TshEmpNo,
                        "WorkDate": retry_entry.TshDate,
                        "PrnCode": retry_entry.TshPrnCode,
                        "Status": "SKIPPED",
                        "Response": "{}",
                        "Hours": entry.TshNormalHours
                    })
                    continue

                payload = retry_entry.__dict__.copy()

                try:
                    # jobcodecostcode = JCJobCategory(entry)
                    # print(jobcodecostcode)
                    r = s.post(f"{host_url}/{endpoint}", json=payload)
                    status = r.status_code
                    resp = r.text
                except Exception as e:
                    status = "ERROR"
                    resp = str(e)
                # if status == 400 and "Cost Code Code".lower() in resp.lower():
                #     retry_time_entries.append(entry)
                # print(json.dumps(payload,indent=None))

                results.append({
                    "EmpNo": retry_entry.TshEmpNo,
                    "WorkDate": retry_entry.TshDate,
                    "PrnCode": retry_entry.TshPrnCode,
                    "Status": status,
                    "Response": resp,
                    "Hours": retry_entry.TshNormalHours
                })
    timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
    output_file = f"DataFiles/PostResult/PostResults_{timestamp}.csv"

    source_name = output_file.split("/")[-1]

    with open(output_file, mode="w", newline="") as csvfile:
        fieldnames = ["EmpNo", "WorkDate", "PrnCode", "Status", "Response","Hours", "SourceFile"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            row["SourceFile"] = source_name
            writer.writerow(row)
            

    print(f"Post log written to {output_file}")
    df_results = pd.read_csv(output_file)
    return df_results

def jobCodeCostCode():
    #establish CMiC API
    host_url, my_auth = create_session()
    posted_entries = get_posted_keys()

    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    results = []

    with sqlalchemy.orm.Session(engine) as session:
        emp_stmt = select(Employee).where(Employee.Active == 'A')#.where(Employee.EmpId == "000223310-PQCD4")
        employees: list[Employee] = session.execute(emp_stmt).scalars()
        host_url, my_auth = create_session()
        with requests.Session() as s:
            s.auth = my_auth
            for employee in employees:
                for entry in employee.time_entries:
                    cmic_entry = Timesheet_Entry(entry)
                    jccc = JCJobCategory(cmic_entry)
                    finder_str = f"?finder=selectJobCategory;jobCode={cmic_entry.TshJobdeptwoId}"
                    endpoint_url = f"{host_url}/jc-rest-api/rest/1/jcjobcategory{finder_str}"
                    valid_combos = [ (jccc.get("JcatJobCode",""),jccc.get("JcatPhsCode",""), jccc.get("JcatCode"))
                                    for jccc in cmic_api_results(endpoint_url=endpoint_url,s=s)
                    ]
                    if (jccc.JcatJobCode, jccc.JcatPhsCode,jccc.JcatCode) not in valid_combos:
                        print(f"invalid combo   {jccc!r}")
                    else:
                        print("combo found")
        print(valid_combos)
if __name__ == "__main__":
    # jobCodeCostCode()
    post_timesheets_to_CMiC()
