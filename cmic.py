import requests, json
from types import SimpleNamespace
from datetime import datetime
from dateutil import parser, tz
class Timesheet_Entry:
    def __init__(self, TshDate: str):
        """
        TshDate expected in yyyy-mm-dd format
        """
        self.TshDate = parser.parse(TshDate)
    def TshPprYear(self) -> int:
        return self.TshDate.year
    def TshPprPeriod(self) -> int:
        #TODO: build logic for pay periods
        #if tshdate > prior period start date and <= period end date, then return pay period
        return 7
    def __str__(self):
        return f'Timesheet Entry for: {self.TshDate}'
    def __repr__(self):
        return f'Timesheet Entry for: {self.TshDate}'
def main():
    host_url = r'https://nova-api.cmiccloud.com/cmicprod'
    username = r'HJR||NSMITH'
    password = 'cX9-_L77i>'
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
    # payruns=    main()
    # print(payruns[0])
    t = Timesheet_Entry("2025-04-07")
    print(repr(t), t.TshPprPeriod())