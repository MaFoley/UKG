import requests, json
from pprint import pprint
from types import SimpleNamespace
from datetime import datetime, timedelta
from dateutil import parser, tz
from models import Time, Employee
from sqlalchemy import select
import sqlalchemy
class Timesheet_Entry:
    WEEKLYPAYGROUP = 'HJRU'
    BIWEEKLYPAYGROUP = 'HJRC'
    def __init__(self, time_entry: Time):
        """
        TshDate expected in yyyy-mm-dd format
        """
        self.TshPrnCode = self._findPayRun(time_entry.paygroup.name)
        self.TshDate: str = parser.parse(time_entry.WorkDate).strftime('%Y-%m-%d')
        self.TshPprYear = parser.parse(time_entry.WorkDate).year
        self.TshPprPeriod = self._TshPprPeriod(parser.parse(time_entry.WorkDate))
        self.TshDocumentNo = None
        self.TshEmpNo = time_entry.employee.shortEmpId()
        self.TshTypeCode = 'J'
        self.TshUnionCode = None
        if time_entry.job != None:
            self.TshTradeCode = time_entry.job.name #time->emp->jobid->job.name
            self.TshJobDeptWOID = time_entry.project.name #time->projectid->project.name
            self.TshPHSACCTWIID = time_entry.orglevel3.name
            self.TshNormalHours: float = time_entry.RegHr
        self.TshCompCode = time_entry.companyCode()
        self.TshCATEXPID = 'L'
        self.TshOTHours: float = time_entry.Overt1
        self.TshDOTHours:float = time_entry.Overt2

    def _findPayRun(self, paygroup: str) -> str:
        if paygroup.upper().strip() == self.BIWEEKLYPAYGROUP:
            return 'B'
        elif paygroup.upper().strip() == self.WEEKLYPAYGROUP:
            return 'W'
        else:
            return 'New Paygroup'
    def _TshPprPeriod(self, date: datetime) -> int:
        #if tshdate > prior period start date and <= period end date, then return pay period
        periodEndDate = datetime(2024,12,29)
        delta = date - periodEndDate
        return delta.days // 14 + 1
            
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        repr_str = f"{self.__class__.__name__}"
        repr_str += '('
        
        for key, val in self.__dict__.items():
            val       = f"'{val}'" if isinstance(val, str) else val
            repr_str += f"{key}={val}\n, "
        
        return repr_str.strip(", ") + ')'
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
           pprint(entry) 
           print(json.dumps(entry.__dict__,indent=4))