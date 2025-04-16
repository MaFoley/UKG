import requests
import json
import pandas as pd
from base64 import b64encode
import os

# === AUTH ===
username = "HJR||NSMITH"
password = ""
auth_string = f"{username}:{password}"
auth_encoded = b64encode(auth_string.encode()).decode()

# === CONFIG ===
host_url = "https://nova-api-test.cmiccloud.com/cmictest"
base_url = f"{host_url}/jc-rest-api/rest/1/jcjob"
headers = {
    "Authorization": f"Basic {auth_encoded}",
    "Accept": "application/json"
}

# === PAGINATION LOOP ===
offset = 0
all_jobs = []
max_offset_limit = 50000  # safeguard

while True:
    print(f"📡 Requesting offset {offset}...")
    params = {"offset": offset}
    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"❌ Error {response.status_code}")
        print(response.text)
        break

    try:
        data = response.json()
    except json.JSONDecodeError:
        print("❌ JSON decode fail")
        print(response.text)
        break

    items = data.get("items", data.get("value", []))
    has_more = data.get("hasMore", False)

    print(f"📦 Retrieved {len(items)} records — hasMore: {has_more}")
    all_jobs.extend(items)

    if not has_more:
        print("✅ No more pages.")
        break

    offset += len(items)

    if offset > max_offset_limit:
        print("🛑 Max offset reached.")
        break

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
df = pd.DataFrame(filtered)

# Add new column that strips periods from JobCode
df["JobCodeNoDots"] = df["JobCode"].str.replace(".", "", regex=False)

# Get current working directory
cwd = os.path.dirname(os.path.abspath(__file__))

# Save path
csv_path = os.path.join(cwd, "DataFiles/CMiC_Project_Summary.csv")

# Save to CSV
df.to_csv(csv_path, index=False)
print(f"✅ Saved filtered project data to {csv_path}")