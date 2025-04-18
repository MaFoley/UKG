
import tomllib, requests
from datetime import datetime
import pandas as pd
import requests.auth
import sqlalchemy, models
#Construction's ID in UKG tables
ORGLEVEL1ID = 263
OUTPUT_FILE_PATH = './DataFiles'
def load_ukg(startDate: str|datetime, endDate: str|datetime):
    if isinstance(startDate, datetime):
        filterStartDate = get_date_string(startDate)
        filterEndDate = get_date_string(endDate)
    else:
        filterStartDate = startDate
        filterEndDate = endDate
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)

    #establish UKG API
    host_url = endpoint["Base"]["base_url"]
    username = endpoint["Base"]["username"]
    password = endpoint["Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    s = requests.Session()
    s.auth = my_auth
    attributes = endpoint["attribute_endpoints"]

    #setup sqlite db file
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    models.Base.metadata.drop_all(engine)
    models.Base.metadata.create_all(engine)

    load_attributes(s,attributes=attributes,host_url=host_url,engine=engine)

    print(f'trying Employee...',end="")
    r = s.get(f'{host_url}/Employee')
    r.raise_for_status()
    employee_df = pd.DataFrame.from_records(r.json()["value"], index="EmpId")
    employee_df.to_sql("Employee",engine,if_exists='replace')

    #time must be filtered or else will timeout endpoint
    payload = f'$filter=WorkDate ge {filterStartDate} and WorkDate le {filterEndDate}&OrgLevel1Id eq {ORGLEVEL1ID}'
    try:
        print(f'trying Time...')
        r = s.get(f'{host_url}/Time',params=payload)
        time_df = pd.DataFrame.from_records(r.json()["value"], index="Id")
        time_df.to_sql("Time",engine,if_exists='replace')
        print(f"record counts for Time:{len(time_df.index)}")
    except:
        print("ERR")
    #join attributes to employees and time before sending to .csv for easy inspection
    for ep, idColumn in attributes.items():
        content = pd.read_csv(f'{OUTPUT_FILE_PATH}/{ep}.csv', index_col="Id")
        employee_df = employee_df.merge(content, left_on= [idColumn]
                                        , right_index=True
                                        )
        time_df = time_df.merge(content, left_on= [idColumn]
                                        , right_index=True
                                        )
    employee_df.to_csv(f'{OUTPUT_FILE_PATH}/Employee.csv')
    time_df.to_csv(f'{OUTPUT_FILE_PATH}/Time.csv')
    s.close()
    engine.dispose()
def get_date_string(d: datetime):
    format_string = '%Y-%m-%d'
    return d.strftime(format_string)
def load_attributes(s: requests.Session, attributes: dict, host_url: str, engine: sqlalchemy.Engine):
    #idColumn is the column name in the employee table
    for ep in attributes.keys():
        try:
            print(f'trying {ep}...',end="")
            r = s.get(f'{host_url}/{ep}')
            r.raise_for_status()
        except requests.HTTPError:
            print(f'ERR status_code: {r.status_code}')
        except requests.ConnectionError:
            print("ERR Connection issue")
        except:
            print("unspecified error")
        if r.status_code != requests.codes.ok:
            continue #ignore bad responses
        content = pd.DataFrame.from_records(r.json()["value"], index="Id")
        content.to_sql(f"{ep}",engine,if_exists='replace', method='multi')
        content = content.rename(columns={"Name": f'{ep}_Name'
                                ,"Description":f'{ep}_Description'
                                }
                                ,errors='raise')
        content.to_csv(f'{OUTPUT_FILE_PATH}/{ep}.csv')
        print(f"record counts for {ep}:{len(content.index)}")
if __name__ == "__main__":
    startdate = datetime(2025,4,6)
    enddate = datetime(2025,4,12)
    load_ukg(startdate, enddate)