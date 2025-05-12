from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
from models import Employee, Timesheet_Entry
from cmic import CMiCAPIClient
from collections.abc import Callable
def create_multi_part_payload(path: str, operation: str, idfunc: Callable[[dict], str], payload_dicts: list[dict]) -> dict:
    """Builds up multi part payload according to CMiC's documentation"""
    result_dict = {"parts": list()}
    for payload in payload_dicts:
        part = {"path": path,
                "operation": operation,
                "payload": payload.__dict__,
                "id": idfunc(payload)}
        result_dict["parts"].append(part)
    return result_dict
engine = create_engine("sqlite:///DataFiles/utm.db", echo=False)
results = []
cmic_employees = []
s = CMiCAPIClient(*CMiCAPIClient.create_session())

#TODO: compare list of employees to those already in CMiC. For any already in CMiC do not post.
#TODO: cmic get request of paginated employee ids
with Session(engine) as session:
    # Step 1: Get all employee IDs
    emp_stmt = select(Employee).where(Employee.Active == 'A').where(Employee.PaygroupId == 18).where(Employee.OrgLevel3Id !=2)#.where(Employee.EmpId == "000223310-PQCD4")
    all_employees = session.execute(emp_stmt).scalars().all()

    # Step 2: Filter those ending in '-PQCD4'
    endpoint = "hcm-rest-api/rest/1/pytrades"
    field_param = "?fields=TrdCode"
    existing_trade_codes = [ tc["TrdCode"]
                            for tc in \
                            s.get_cmic_api_results(f"{endpoint}{field_param}",limit=500)]
    
    for emp in all_employees:
        timesheets = [Timesheet_Entry(t) for t in emp.time_entries]
        r = create_multi_part_payload("path", "operation", lambda time_entry: time_entry.TshDocumentNo, timesheets)
        for d in r["parts"]:
            print(d)