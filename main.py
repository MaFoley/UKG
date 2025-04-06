
import tomllib, requests 
import pandas as pd
import sqlalchemy, models
def main(startDate: str, endDate: str):
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    host_url = endpoint["Base"]["base_url"]
    username = endpoint["Base"]["username"]
    password = endpoint["Base"]["password"]
    attributes = endpoint["attribute_endpoints"]
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=True)
    models.Base.metadata.drop_all(engine)
    models.Base.metadata.create_all(engine)
    print(f'host_url: {host_url},\nusername:{username}, password:{password}')

    print(f'trying Employee...',end="")
    r = requests.get(f'{host_url}/Employee', auth=(username, password))
    r.raise_for_status()
    employee_df = pd.DataFrame.from_records(r.json()["value"], index="EmpId")
    employee_df.to_sql("Employee",engine,if_exists='replace')

    #Time must be filtered or else will timeout endpoint
    payload = {'$filter' : [f'WorkDate ge {startDate} and WorkDate le {endDate}']}
    try:
        print(f'trying Time...',end="")
        r = requests.get(f'{host_url}/Time', auth=(username, password), params= payload)
        print(r.url)
        time_df = pd.DataFrame.from_records(r.json()["value"], index="Id")
        time_df.to_sql("Time",engine,if_exists='replace')
    except:
        print("ERR")
    #idColumn is the column name in the employee table
    for ep, idColumn in attributes.items():
        try:
            print(f'trying {ep}...',end="")
            r = requests.get(f'{host_url}/{ep}', auth=(username, password))
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
if __name__ == "__main__":
    main("2025-03-24", "2025-03-31")
  