import csv
from sqlalchemy import select
import pandas as pd
from sqlalchemy.orm import Session
from models import Employee, Location
import sqlalchemy
import requests

#auth
host_url = r'https://nova-api-test.cmiccloud.com/cmictest'
username = r'HJR||NSMITH'
password = r''
my_auth = requests.auth.HTTPBasicAuth(username, password)

results = []

#dept map
dept_map_df = pd.read_csv("DataFiles/DEPARTMENT MAP.csv", header=0, usecols=[1, 3])
dept_map_df.columns = ['UKGDName', 'CMiCD']
dept_map = {
    str(k).strip(): str(v).strip()
    for k, v in zip(dept_map_df['UKGDName'], dept_map_df['CMiCD'])
}

def get_employee_data(emp_id: str):  # Picks up all employees
    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)

    with Session(engine) as session:
         stmt = (
            select(Employee, Location.name.label("DeptName"))
            .join(Location, Employee.LocationId == Location.Id)
            .where(Employee.EmpId == emp_id)
        )

         result = session.execute(stmt).first()
         if not result:
             return None
         
         emp, dept_name = result

         mapped_dept = dept_map.get(dept_name, dept_name)
         
         return {
            "EmpNo": emp.EmpId.split("-")[0][-6:],
            "EmpPrimaryEmpNo": emp.EmpId.split("-")[0][-6:],
            "EmpUser": 'NSMITH',
            "EmpLastName": emp.LastName,
            "EmpFirstName": emp.FirstName,
            "EmpSinNo": '123456789', #plug
            "EmpStatus": 'A',
            "EmpDateOfBirth": '1999-01-01', #plug
            "EmpHireDate": '1999-01-02',#plug
            "EmpCompCode": 'HJR',
            "EmpDeptCode": mapped_dept,  # Mapped
            "EmpPrnCode": 'B',
            "EmpPygCode": 'PMOH' if mapped_dept== 'PMG' else 'CNOH',#I think this will work, does it even matter though
            "EmpTrdCode": 'HPM921', #Sent to the weirdest Trade Code I could find, Airplane Maint
            "EmpWrlCode": 'ATL',
            "EmpChargeOutRate": 3, #need provided info
            "EmpBillingRate": 3, #need provided info
            "EmpSecGrpEmpCode": 'MASTER',
            "EmpFilingStatus": '01',
            "EmpVUuid": '',
            "EmpCountryCode": 'US',
            "EmpStateCode": 'GA',
            "EmpZipCode": '30152',
            "EmpFlsaType": 'N',
            "EmpVertexGeocodeSource": 'M',
            "EmpType": 'S',
            "EmpFullPartTime": 'F',
            "EmpSubStatus": 'W',
            "EmpUnionized": 'N',
            "EmpAdjustedSeriviceDate": '1999-01-02',
            "EmpYearWorkingDays": 260.0,
            "EmpYearWorkingHours": 2080.0,
            "EmpUeValidFlag": 'Y',
            "EmpHomeDeptCode": mapped_dept,  # Mapped
            "EmpGlAccCode": '600.004',
            "EmpPayrollClearAccCode": '240.001',
            "EmpDrClearAccCode": '240.001',
            "EmpLevAcruGlAccCode": '240.001',
            "EmpLevClearAccCode": '240.001',
            "EmpAnnualSalary": 100, #I think this is acceptable as a plug unless it has a meaningful impact on charge/bill rate
            "EmpPreferPayRate": 'J',
            "EmpPreferChargeRate": 'J',
            "EmpPreferBillRate": 'J',
            "EmpDirectDepMethod": 'N'            
        }


def main():
    engine = sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)

    with Session(engine) as session:
        # Step 1: Get all employee IDs
        stmt = select(Employee.EmpId)
        all_ids = session.execute(stmt).scalars().all()

        # Step 2: Filter those ending in '-PQCD4'
        filtered_ids = [eid for eid in all_ids if eid.endswith('-PQCD4')]

    if not filtered_ids:
        print("❌ No matching employees found.")
        return

    # Step 3: Loop and post each
    print(f"📦 Found {len(filtered_ids)} matching employees. Starting POSTs...\n")

    with requests.Session() as s:
        s.auth = my_auth
        endpoint = "hcm-rest-api/rest/1/pyemployee"

        for emp_id in filtered_ids:
            payload = get_employee_data(emp_id)
            if not payload:
                print(f"⚠️ Skipping {emp_id} – no data returned.")
                continue

            try:
                r = s.post(f"{host_url}/{endpoint}", json=payload)
                results.append({
                    "EmpId": emp_id,
                    "Status": r.status_code,
                    "Response": r.text
                })
            except Exception as e:
                results.append({
                    "EmpId": emp_id,
                    "Status": "ERROR",
                    "Response": str(e)
                })
            
            # After loop:
            with open("employee_post_results.csv", "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["EmpId", "Status", "Response"])
                writer.writeheader()
                writer.writerows(results)

if __name__ == "__main__":
    main()