import json, tomllib, requests
import pandas as pd
import os
import sqlalchemy
from models import Project

with open("config.toml", "rb") as f:
    endpoint = tomllib.load(f)

#establish CMiC API
host_url = endpoint["CMiC_Base"]["base_url"]
username = endpoint["CMiC_Base"]["username"]
password = endpoint["CMiC_Base"]["password"]
# === AUTH ===

# === CONFIG ===
host_url = "https://nova-api-test.cmiccloud.com/cmictest"
base_url = f"{host_url}/jc-rest-api/rest/1/jcjob"
headers = {
    "Accept": "application/json"
}
my_auth = requests.auth.HTTPBasicAuth(username, password)

# === PAGINATION LOOP ===
offset = 0
all_jobs = []
max_offset_limit = 50000  # safeguard
s = requests.Session()
s.auth = my_auth
while True:
    print(f"Requesting offset {offset}...")
    params = {"offset": offset}
    response = s.get(base_url, headers=headers, params=params)

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

    print(f"Retrieved {len(items)} records — hasMore: {has_more}")
    all_jobs.extend(items)

    if not has_more:
        print(f"No more pages. Loaded {len(all_jobs)} jobs")
        break

    offset += len(items)

    if offset > max_offset_limit:
        print("Max offset reached.")
        break
s.close()
# === FILTER & SAVE ===
filtered = [
    {
        "JobCode": job.get("JobCode"),
        "JobName": job.get("JobName"),
        "JobDefaultDeptCode": job.get("JobDefaultDeptCode")
    }
    for job in all_jobs
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
engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=True)
df.to_csv(csv_path, index=True)
df.to_sql("CMiC_Project",engine,if_exists='replace')
print(f"Saved filtered project data to {csv_path}")