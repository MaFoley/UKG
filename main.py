
import tomllib, requests 
from urllib.parse import quote_plus
import pandas as pd
import requests.auth
import sqlalchemy, models
#Construction's ID in UKG tables
ORGLEVEL1ID = 263
def main(startDate: str, endDate: str):
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
    print(f'host_url: {host_url},\nusername:{username}, password:{password}')

    #setup sqlite db file
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    models.Base.metadata.drop_all(engine)
    models.Base.metadata.create_all(engine)

    print(f'trying Employee...',end="")
    r = s.get(f'{host_url}/Employee')
    r.raise_for_status()
    employee_df = pd.DataFrame.from_records(r.json()["value"], index="EmpId")
    employee_df.to_sql("Employee",engine,if_exists='replace')

    #time must be filtered or else will timeout endpoint
    payload = f'$filter=WorkDate ge {startDate} and WorkDate le {endDate}&OrgLevel1Id eq {ORGLEVEL1ID}'
               
    
    try:
        print(f'trying Time...',end="")
        r = s.get(f'{host_url}/Time',params=payload)
        time_df = pd.DataFrame.from_records(r.json()["value"], index="Id")
        time_df.to_sql("Time",engine,if_exists='replace')
    except:
        print("ERR")
    #idColumn is the column name in the employee table
    for ep, idColumn in attributes.items():
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
        content.to_csv(f'./DataFiles/{ep}.csv')
        employee_df = employee_df.merge(content, left_on= [idColumn]
                                        , right_index=True
                                        )
        time_df = time_df.merge(content, left_on= [idColumn]
                                        , right_index=True
                                        )
    employee_df.to_csv(f'./DataFiles/Employee.csv')
    time_df.to_csv(f'./DataFiles/Time.csv')
    s.close()
if __name__ == "__main__":
    main("2025-04-01", "2025-04-07")
  