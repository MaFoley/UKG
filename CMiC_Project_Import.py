import json, tomllib, requests
import pandas as pd
import os
import sqlalchemy
from models import Project
from collections.abc import Generator

def load_cmic_projects():

    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    with open("secrets.toml","rb") as f:
        endpoint.update(tomllib.load(f))
    #establish CMiC API
    base_url = endpoint["CMiC_Base"]["host_url"]
    username = endpoint["CMiC_Base"]["username"]
    password = endpoint["CMiC_Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    s = requests.Session()
    s.auth = my_auth

    # === FILTER & SAVE ===
    filtered = [
        {
            "JobCode": job.get("JobCode"),
            "JobName": job.get("JobName"),
            "JobDefaultDeptCode": job.get("JobDefaultDeptCode")
        }
        for job in cmic_api_results(f"{base_url}/jc-rest-api/rest/1/jcjob", s)
    ]

    # Create DataFrame
    df = pd.DataFrame.from_records(filtered, index="JobCode")

    # Add new column that strips periods from JobCode
    df["JobCodeNoDots"] = df.index.str.replace(".", "", regex=False)

    # Get current working directory
    cwd = os.path.dirname(os.path.abspath(__file__))

    # Save path
    csv_path = "DataFiles/CMiC_Project_Summary.csv"

    #setup sqlite db file
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    df.to_csv(csv_path, index=True)
    df.to_sql("CMiC_Project",engine,if_exists='replace')
    print(f"Saved filtered project data to {csv_path}")
    s.close()

def cmic_api_results(endpoint_url: str, s: requests.Session, limit: int = 100)-> Generator[dict]:
    """
    Parameters:
    param: endpoint_url is the full path to api endpoint
    param: s is a valid Requests Session with basic auth for CMiC estbalished.
    
    This is a generator function for paginated cmic endpoints that follow the structure of 
    {items, count, hasMore, limit, offset}
    """
    # === CONFIG ===
    headers = {
        "Accept": "application/json"
    }

    # === PAGINATION LOOP ===
    offset = 0
    max_offset_limit = 50000  # safeguard
    while True:
        print(f"Requesting offset {offset}...")
        params = {"offset": offset
                  ,"limit": limit}
        response = s.get(endpoint_url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error {response.status_code}")
            print(response.text)
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("JSON decode fail")
            print(response.text)
            break

        items = data.get("items", data.get("value", []))
        has_more = data.get("hasMore", False)
        for item in items:
            yield item
        print(f"Retrieved {len(items)} records — hasMore: {has_more}")

        offset += len(items)
        if not has_more:
            print(f"No more pages. Loaded {offset} items")
            break

        if offset > max_offset_limit:
            print("Max offset reached.")
            break
    # return all_items
def create_session() -> tuple[str, requests.auth]:
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    with open("secrets.toml","rb") as f:
        endpoint.update(tomllib.load(f))

    #establish CMiC API
    host_url = endpoint["CMiC_Base"]["host_url"]
    username = endpoint["CMiC_Base"]["username"]
    password = endpoint["CMiC_Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    return host_url, my_auth

if __name__ == "__main__":
    # load_cmic_projects()
    host_url, my_auth = create_session()
    with requests.Session() as s:
        s.auth = my_auth
        endpoint = "hcm-rest-api/rest/1/pyemployee"
        existing_employees = [emp["EmpNo"] for emp in cmic_api_results(f"{host_url}/{endpoint}",s, limit=500)]
    print(existing_employees)