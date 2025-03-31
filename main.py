
import tomllib, requests 
import pandas as pd
def main():
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    host_url = endpoint["Base"]["base_url"]
    username = endpoint["Base"]["username"]
    password = endpoint["Base"]["password"]
    attributes = endpoint["attribute_endpoints"]
    print(f'host_url: {host_url}\n, username:{username}, password:{password}')

    print(f'trying Employee...',end="")
    r = requests.get(f'{host_url}/Employee', auth=(username, password))
    r.raise_for_status()
    employee_df = pd.DataFrame.from_records(r.json()["value"], index="EmpId")
    #Time gets special treatment
    payload = {'$filter' : ['WorkDate ge 2025-03-22 and WorkDate le 2025-03-31']}
    try:
        print(f'trying Time...',end="")
        r = requests.get(f'{host_url}/Time', auth=(username, password), params= payload)
        print(r.url)
        time_df = pd.DataFrame.from_records(r.json()["value"], index="Id")
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
        content = content.rename(columns={"Name": f'{ep}_Name'
                                ,"Description":f'{ep}_Description'
                                }
                                ,errors='raise')
        content.to_csv(f'.\DataFiles\{ep}.csv')
        employee_df = employee_df.merge(content, left_on= [idColumn]
                                        , right_index=True
                                        )
        time_df = time_df.merge(content, left_on= [idColumn]
                                        , right_index=True
                                        )
        print(content)
    employee_df.to_csv(f'.\DataFiles\Employee.csv')
    time_df.to_csv(f'.\DataFiles\Time.csv')
if __name__ == "__main__":
    main()
  