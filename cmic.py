import requests, json, csv, tomllib
from datetime import datetime
from models import Time, Employee, Timesheet_Entry
from sqlalchemy import select
import sqlalchemy
def main():
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)

    #establish CMiC API
    host_url = endpoint["CMiC_Base"]["base_url"]
    username = endpoint["CMiC_Base"]["username"]
    password = endpoint["CMiC_Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    with requests.Session() as s:
        s.auth = my_auth
        endpoint = r'hcm-rest-api/rest/1/pypayrun'
        print(f'host_url: {host_url},\nusername:{username}, password:{password}')
        payload = {'finder': 'PrimaryKey;PrnCode=B'}
        r = s.get(f"{host_url}/{endpoint}",params=payload)
        dict_payruns = r.json(object_hook=lambda d: SimpleNamespace(**d))
        payruns = dict_payruns.items
    return payruns
if __name__ == "__main__":
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    with sqlalchemy.orm.Session(engine) as session:
        emp_stmt = select(Employee).where(Employee.EmpId == "000020840-PQCD4")
        employee = session.execute(emp_stmt).scalar_one_or_none()
        if not employee:
            print("❌ Employee not found.")
            return

        joined_stmt = select(Time).where(Time.EmpId == employee.EmpId)
        timesheets = session.execute(joined_stmt)
        # example = Time()
        # example.WorkDate = "2025-04-06"
        # t = Timesheet_Entry(example)
        for t in timesheets.scalars():
           entry = Timesheet_Entry(t)
           print(json.dumps(entry.__dict__,indent=4))