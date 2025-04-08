import requests, json
from pprint import pprint
from types import SimpleNamespace
from datetime import datetime, timedelta
from dateutil import parser, tz
from models import Time, Employee, Timesheet_Entry
from sqlalchemy import select
import sqlalchemy
def main():
    host_url = r'https://nova-api.cmiccloud.com/cmicprod'
    username = r'HJR||NSMITH'
    password = input('please enter pass:')
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
        joined_stmt = select(Time).where(Time.EmpId== "000223224-PQCD4")
        timesheets = session.execute(joined_stmt)
        # example = Time()
        # example.WorkDate = "2025-04-06"
        # t = Timesheet_Entry(example)
        for t in timesheets.scalars():
           entry = Timesheet_Entry(t)
           print(json.dumps(entry.__dict__,indent=4))