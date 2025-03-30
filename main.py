
import tomllib, requests 
import pandas as pd
def main():
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    host_url = endpoint["Base"]["base_url"]
    username = endpoint["Base"]["username"]
    password = endpoint["Base"]["password"]
    endpoints = endpoint["Endpoints"]["endpoints"]
    print(f'host_url: {host_url}\n, username:{username}, password:{password}')
    org1 = f'OrgLevel1'
    for ep in endpoints:
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
        content.to_csv(f'.\DataFiles\{ep}.csv')
        print(content)
    payload = {'$filter' : 'WorkDate ge 2025-03-28'}
    #Time gets special treatment
    try:
        print(f'trying Time...',end="")
        r = requests.get(f'{host_url}/Time', auth=(username, password))
        print(r.url)
        ts = pd.DataFrame.from_records(r.json()["value"], index="Id", params= payload)
    except:
        print("ERR")
    ts.to_csv(f'Time.csv')
if __name__ == "__main__":
    main()
  