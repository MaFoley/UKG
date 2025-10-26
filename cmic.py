from collections.abc import Generator
import requests, json, csv, tomllib, os
import pandas as pd
from datetime import datetime
from functools import wraps
from collections.abc import Callable
from sqlalchemy.orm import Session
from models import CMiC_Employee, CMiCTradeCode, Time, Employee, CMiC_Timesheet_Entry, JCJobCategory
from sqlalchemy import select
import sqlalchemy
from pathlib import Path
from posting_log_ingest import get_posted_keys
import logging, sys
OUTPUT_FILE_PATH = './DataFiles'
logger = logging.getLogger('cmic')
logger.level = logging.INFO
logger.propagate = False
if not logger.hasHandlers():
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    sh, fh = logging.StreamHandler(sys.stdout),logging.FileHandler(f"{OUTPUT_FILE_PATH}/middleware.log")
    sh.setFormatter(formatter)
    sh.setLevel(logger.level)
    fh.setFormatter(formatter)
    sh.setLevel(logger.level)
    logger.addHandler(sh)
    logger.addHandler(fh)


UKGHJRCOMPANYCODE = 'PQCD4'
class CMiCAPIClient:
    def __init__(self, host_url, auth=None):
        self.logger = logging.getLogger('cmic.CMiCAPIClient')
        self.logger.info('creating instance of CMiCAPIClient')
        self.host_url = host_url
        self.session = requests.Session()
        if auth:
            self.session.auth = auth
    @classmethod
    def create_session(cls) -> tuple[str, requests.auth.HTTPBasicAuth]:
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

    def get_cmic_api_results(self, endpoint_url: str, limit: int = 100)-> Generator[dict]:
        """
        Parameters:
        param: endpoint_url is the full path to api endpoint
        param: s is a valid Requests Session with basic auth for CMiC estbalished.

        This is a generator function for paginated cmic endpoints that follow the structure of 
        {items, count, hasMore, limit, offset}
        """
        # === CONFIG ===
        headers = {
            "Accept": "application/json"
        }

        # === PAGINATION LOOP ===
        offset = 0
        max_offset_limit = 50000  # safeguard
        self.logger.info(f"requesting records from {endpoint_url} with {limit=}")
        while True:
            logger.info(f"Requesting {offset=}...")
            params = {"offset": offset, "limit": limit}
            response = self.session.get(f"{self.host_url}/{endpoint_url}", headers=headers, params=params)

            if response.status_code != 200:
                self.logger.info(f"Error {response.status_code}")
                self.logger.info(response.text)
                break

            try:
                data = response.json()
            except json.JSONDecodeError:
                self.logger.error("JSON decode fail")
                self.logger.error(response.text)
                break

            items = data.get("items", data.get("value", []))
            has_more = data.get("hasMore", False)
            for item in items:
                yield item
            self.logger.info(f"Retrieved {len(items)} records — {has_more=}")

            offset += len(items)
            if not has_more:
                self.logger.info(f"No more pages. Loaded {offset} items")
                break

            if offset > max_offset_limit:
                self.logger.info("Max offset reached.")
                break
    @wraps(requests.Session.post)
    def post(self, endpoint_url: str, **kwargs):
        return self.session.post(url=f"{self.host_url}/{endpoint_url}",**kwargs)
    def __del__(self):
        if self.session:
            self.session.close()
def create_multi_part_payload(path: str, operation: str, idfunc: Callable[[dict], str], payload_dicts: list[object]) -> dict:
    """Builds up multi part payload according to CMiC's documentation"""
    result_dict = {"parts": list()}
    for payload in payload_dicts:
        part = {"path": path,
                "operation": operation,
                "payload": payload.__dict__,
                "id": idfunc(payload)}
        result_dict["parts"].append(part)
    return result_dict
def employee_push(effective_date: str):
    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    results = []
    cmic_employees = []
    s = CMiCAPIClient(*CMiCAPIClient.create_session())

    #TODO: compare list of employees to those already in CMiC. For any already in CMiC do not post.
    #TODO: cmic get request of paginated employee ids
    with Session(engine) as session:
        # Step 1: Get all employee IDs
        emp_stmt = select(Employee).where(Employee.EmpId.in_(emp_ids_in_csv))#.where(Employee.EmpId == "000223310-PQCD4")
        all_employees = session.execute(emp_stmt).scalars().all()

        # Step 2: Filter those ending in '-PQCD4'
        endpoint = "hcm-rest-api/rest/1/pytrades"
        field_param = "?fields=TrdCode"
        existing_trade_codes = [ tc["TrdCode"]
                                for tc in \
                                s.get_cmic_api_results(f"{endpoint}{field_param}",limit=500)]
        #filtered_employees = [emp for emp in all_employees if emp.companyCode() =='PQCD4']
        cmic_employees = [CMiC_Employee(emp, effective_date) for emp in all_employees
                           if emp.companyCode() == 'PQCD4']
        trade_codes_to_post = [CMiCTradeCode(emp) for emp in all_employees
                                if emp.job.name not in existing_trade_codes]

        for trade_code in trade_codes_to_post:
            payload = trade_code.__dict__.copy()
            r = s.post(f"{endpoint}",json=payload)
            # logger.info(r)
    if not cmic_employees:
        logger.info("No matching employees found.")
        return

    # Step 3: Loop and post each
    logger.info(f"Found {len(cmic_employees)} matching employees. Starting POSTs...\n")

    endpoint = "hcm-rest-api/rest/1/pyemployee"
    fields = "?fields=EmpChargeOutRate,EmpNo"
    existing_employees = [emp
                            for emp in \
                            s.get_cmic_api_results(f"{endpoint+fields}", limit=500)]
    emp_df = pd.DataFrame(existing_employees)
    #set precision to allow comparison
    emp_df["EmpChargeOutRate"] = emp_df["EmpChargeOutRate"].round(2)
    for cmic_emp in cmic_employees:
        #early exit: if empNo exists and charge rate is the same, can skip
        is_present = emp_df['EmpNo'].isin([cmic_emp.EmpNo]).any()
        rate_to_compare = round(cmic_emp.EmpChargeOutRate,2) if cmic_emp.EmpChargeOutRate is not None else 0
        if is_present and rate_to_compare == emp_df["EmpChargeOutRate"].loc[emp_df["EmpNo"] == cmic_emp.EmpNo].item():
            continue

        if is_present:
            cmic_emp.EmhActionCode = "CH"
        else:
            cmic_emp.EmhActionCode = "NR"
        payload = cmic_emp.__dict__.copy()
        if not payload:
            logger.info(f"️Skipping {cmic_emp} no data returned.")
            continue

        try:
            r = s.post(f"{endpoint}", json=payload)
            results.append({
                "EmpNo": cmic_emp.EmpNo,
                "Status": r.status_code,
                "Response":r.json(),
                "Payload":payload
            })
        except Exception as e:
            results.append({
                "EmpNo": cmic_emp.EmpNo,
                "Status": "ERROR",
                "Response": str(e),
                "Payload":payload
            })

    # After loop:
    with open("DataFiles/employee_post_results.csv", "w", newline="") as f:
        logger.info(f"writing {len(results)} records to file")
        writer = csv.DictWriter(f, fieldnames=["EmpNo", "Status", "Response", "Payload"])
        writer.writeheader()
        writer.writerows(results)

df_time = pd.read_csv("DataFiles/Time.csv")
emp_ids_in_csv = set(df_time["EmpId"].dropna().astype(str).str.strip())


def post_timesheets_to_CMiC(cmic_payrun: str, testing: bool=False) -> pd.DataFrame:
    payrun_to_paygroup = {
        'B' : 18,
        'W' : 43
    }
    try:
        ukg_paygroup_id = payrun_to_paygroup[cmic_payrun[0].upper()]
    except IndexError as error:
        logger.error("invalid payrun. Must be B or W\n", error)

    #establish CMiC API
    s = CMiCAPIClient(*CMiCAPIClient.create_session())
    posted_entries = get_posted_keys() if testing == False else {}

    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    results = []
    retry_time_entries: list[CMiC_Timesheet_Entry] = []
    with sqlalchemy.orm.Session(engine) as session:
        
        emp_stmt = select(Employee).where(Employee.EmpId.in_(emp_ids_in_csv))#.where(Employee.EmpId == "000223310-PQCD4")
        # emp_stmt = select(Employee).where(Employee.EmpId == "000020828-PQCD4")
        employees: list[Employee] = session.execute(emp_stmt).scalars()
        endpoint = r'hcm-rest-api/rest/1/pyemptimesheet'
        for employee in employees:
            if not employee.time_entries:
                logger.info(f"Employee {employee.EmpId} {employee.LastName}, {employee.FirstName} has no time in current pay period")
                continue
            elif employee.companyCode() != UKGHJRCOMPANYCODE:
                logger.info(f"Employee {employee.EmpId} not in HJR")
                continue
            # joined_stmt = select(Time).where(Time.EmpId == employee.EmpId)
            # timesheets = session.execute(joined_stmt)
            for t in employee.time_entries:
                entry = CMiC_Timesheet_Entry(t)
                if entry.TshJobdeptwoId == None:
                    results.append({
                        "EmpNo": entry.TshEmpNo,
                        "WorkDate": entry.TshDate,
                        "PrnCode": entry.TshPrnCode,
                        "Status": "SKIPPED",
                        "Response": "No Job Code for employee",
                        "Hours": entry.TshNormalHours,
                        "Time_Id": entry.TshUserField5
                    })
                    continue


                entry_key = (entry.TshEmpNo.strip(), entry.TshDate.strip(), entry.TshPrnCode.strip())

                if entry_key in posted_entries:
                    # logger.info(f"Already Posted, {entry_key} - skipping")
                    results.append({
                        "EmpNo": entry.TshEmpNo,
                        "WorkDate": entry.TshDate,
                        "PrnCode": entry.TshPrnCode,
                        "Status": "SKIPPED",
                        "Response": "{}",
                        "Hours": entry.TshNormalHours,
                        "Time_Id": entry.TshUserField5
                    })
                    continue

                payload = entry.payload_dict()

                try:
                    # jobcodecostcode = JCJobCategory(entry)
                    # logger.info(jobcodecostcode)
                    r = s.post(f"{endpoint}", json=payload)
                    status = r.status_code
                    resp = r.text
                except Exception as e:
                    status = "ERROR"
                    resp = str(e)
                if status == 400 and "Cost Code Code".lower() in resp.lower():
                    retry_time_entries.append(entry)
                # logger.info(json.dumps(payload,indent=None))

                results.append({
                    "EmpNo": entry.TshEmpNo,
                    "WorkDate": entry.TshDate,
                    "PrnCode": entry.TshPrnCode,
                    "Status": status,
                    "Response": resp,
                    "Hours": entry.TshNormalHours,
                    "Time_Id": entry.TshUserField5
                })
        for retry_entry in retry_time_entries:
            jobcodecostcode = JCJobCategory(retry_entry)
            r = s.post(f"jc-rest-api/rest/1/jcjobcategory"
                        , json=jobcodecostcode.__dict__.copy())
            entry_key = (retry_entry.TshEmpNo.strip(), retry_entry.TshDate.strip(), retry_entry.TshPrnCode.strip())

            if entry_key in posted_entries:
                # logger.info(f"Already Posted, {entry_key} - skipping")
                results.append({
                    "EmpNo": retry_entry.TshEmpNo,
                    "WorkDate": retry_entry.TshDate,
                    "PrnCode": retry_entry.TshPrnCode,
                    "Status": "SKIPPED",
                    "Response": "{}",
                    "Hours": entry.TshNormalHours,
                    "Time_Id": entry.TshUserField5
                })
                continue

            payload = retry_entry.payload_dict()

            try:
                # jobcodecostcode = JCJobCategory(entry)
                # logger.info(jobcodecostcode)
                r = s.post(f"{endpoint}", json=payload)
                status = r.status_code
                resp = r.text
            except Exception as e:
                status = "ERROR"
                resp = str(e)
            # if status == 400 and "Cost Code Code".lower() in resp.lower():
            #     retry_time_entries.append(entry)
            # logger.info(json.dumps(payload,indent=None))

            results.append({
                "EmpNo": retry_entry.TshEmpNo,
                "WorkDate": retry_entry.TshDate,
                "PrnCode": retry_entry.TshPrnCode,
                "Status": status,
                "Response": resp,
                "Hours": retry_entry.TshNormalHours,
                "Time_Id": entry.TshUserField5
            })
    timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
    output_file = f"DataFiles/PostResult/PostResults_{timestamp}.csv"

    source_name = output_file.split("/")[-1]

    with open(output_file, mode="w", newline="") as csvfile:
        fieldnames = ["EmpNo", "WorkDate", "PrnCode", "Status", "Response","Hours", "Time_Id", "SourceFile"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            row["SourceFile"] = source_name
            writer.writerow(row)
            

    logger.info(f"Post log written to {output_file}")
    df_results = pd.read_csv(output_file)
    return df_results

def jobCodeCostCode():
    #establish CMiC API
    posted_entries = get_posted_keys()

    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    results = []

    with sqlalchemy.orm.Session(engine) as session:
        emp_stmt = select(Employee).where(Employee.EmpId.in_(emp_ids_in_csv))#.where(Employee.EmpId == "000223310-PQCD4")
        employees: list[Employee] = session.execute(emp_stmt).scalars()
        s = CMiCAPIClient(*CMiCAPIClient.create_session())
        for employee in employees:
            for entry in employee.time_entries:
                cmic_entry = CMiC_Timesheet_Entry(entry)
                jccc = JCJobCategory(cmic_entry)
                finder_str = f"?finder=selectJobCategory;jobCode={cmic_entry.TshJobdeptwoId}"
                endpoint_url = f"jc-rest-api/rest/1/jcjobcategory{finder_str}"
                valid_combos = [ (jccc.get("JcatJobCode",""),jccc.get("JcatPhsCode",""), jccc.get("JcatCode"))
                                for jccc in s.get_cmic_api_results(endpoint_url=endpoint_url)
                ]
                if (jccc.JcatJobCode, jccc.JcatPhsCode,jccc.JcatCode) not in valid_combos:
                    logger.info(f"invalid combo   {jccc!r}")
                else:
                    logger.info("combo found")
        logger.info(valid_combos)
def load_cmic_projects():

    s = CMiCAPIClient(*CMiCAPIClient.create_session())
    # === FILTER & SAVE ===
    filtered = [
        {
            "JobCode": job.get("JobCode"),
            "JobName": job.get("JobName"),
            "JobDefaultDeptCode": job.get("JobDefaultDeptCode")
        }
        for job in s.get_cmic_api_results(f"jc-rest-api/rest/1/jcjob", limit=500)
    ]

    # Create DataFrame
    df = pd.DataFrame.from_records(filtered, index="JobCode")

    # Add new column that strips periods from JobCode
    df["JobCodeNoDots"] = df.index.str.replace(".", "", regex=False)

    # Get current working directory
    cwd = os.path.dirname(os.path.abspath(__file__))

    # Save path
    csv_path = "DataFiles/CMiC_Project_Summary.csv"

    #setup sqlite db file
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    df.to_csv(csv_path, index=True)
    df.to_sql("CMiC_Project",engine,if_exists='replace')
    logger.info(f"Saved filtered project data to {csv_path}")


if __name__ == "__main__":
    # jobCodeCostCode()
    # post_timesheets_to_CMiC(testing=True)
    employee_push("2025-09-21")
    #load_cmic_projects()


