import csv, tomllib
from sqlalchemy import select
import pandas as pd
from sqlalchemy.orm import Session
from models import Employee, Location, CMiC_Employee
from CMiC_Project_Import import cmic_api_results
import sqlalchemy
import requests

#dept map
dept_map_df = pd.read_csv("DataFiles/DEPARTMENT MAP.csv", header=0, usecols=[1, 3])
dept_map_df.columns = ['UKGDName', 'CMiCD']
dept_map = {
    str(k).strip(): str(v).strip()
    for k, v in zip(dept_map_df['UKGDName'], dept_map_df['CMiCD'])
}
#deprecated?
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
         
         #TODO: refactor as cmic_Employee object inside models using __init__ to build up. will remove need for this function
         return {
            #"EmhActionCode": "NR"
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
            "EmpTrdCode": emp.job.name, 
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
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    with open("secrets.toml","rb") as f:
        endpoint.update(tomllib.load(f))

    #establish CMiC API
    host_url = endpoint["CMiC_Base"]["host_url"]
    username = endpoint["CMiC_Base"]["username"]
    password = endpoint["CMiC_Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    results = []


    #TODO: compare list of employees to those already in CMiC. For any already in CMiC do not post.
    #TODO: cmic get request of paginated employee ids
    with Session(engine) as session:
        # Step 1: Get all employee IDs
        emp_stmt = select(Employee).where(Employee.Active == 'A').where(Employee.PaygroupId == 18).where(Employee.OrgLevel3Id !=2)#.where(Employee.EmpId == "000223310-PQCD4")
        all_employees = session.execute(emp_stmt).scalars().all()

        # Step 2: Filter those ending in '-PQCD4'
        #filtered_employees = [emp for emp in all_employees if emp.companyCode() =='PQCD4']
        cmic_employees = [CMiC_Employee(emp) for emp in all_employees if emp.companyCode() == 'PQCD4']

    if not cmic_employees:
        print("❌ No matching employees found.")
        return

    # Step 3: Loop and post each
    print(f"📦 Found {len(cmic_employees)} matching employees. Starting POSTs...\n")

    with requests.Session() as s:
        s.auth = my_auth
        endpoint = "hcm-rest-api/rest/1/pyemployee"
        existing_employees = [emp["EmpNo"]
                               for emp in \
                                cmic_api_results(f"{host_url}/{endpoint}",s, limit=500)]

        for cmic_emp in cmic_employees:
            if cmic_emp.EmpNo in existing_employees:
                cmic_emp.EmhActionCode = "CH"
            else:
                cmic_emp.EmhActionCode = "NR"
            payload = cmic_emp.__dict__.copy()
            if not payload:
                print(f"⚠️ Skipping {cmic_emp} – no data returned.")
                continue

            try:
                r = s.post(f"{host_url}/{endpoint}", json=payload)
                results.append({
                    "EmpNo": cmic_emp.EmpNo,
                    "Status": r.status_code,
                    "Response": r.text.strip('\n')
                })
            except Exception as e:
                results.append({
                    "EmpNo": cmic_emp.EmpNo,
                    "Status": "ERROR",
                    "Response": str(e)
                })
            
        # After loop:
        with open("DataFiles/employee_post_results.csv", "w", newline="") as f:
            print(f"writing {len(results)} records to file")
            writer = csv.DictWriter(f, fieldnames=["EmpNo", "Status", "Response"])
            writer.writeheader()
            writer.writerows(results)

if __name__ == "__main__":
    main()