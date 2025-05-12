
import tomllib, requests
from datetime import datetime
import pandas as pd
import requests.auth
import sqlalchemy, models
from sqlalchemy import Select
from sqlalchemy.orm import Session
#Construction's ID in UKG tables
ORGLEVEL1ID = 263
OUTPUT_FILE_PATH = './DataFiles'
class MainEndpoint:
    def __init__(self, tableName, params, idCol):
        self.table_name = tableName
        self.params = params
        self.idCol = idCol
def create_session() -> requests.Session:
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    with open("secrets.toml","rb") as f:
        endpoint.update(tomllib.load(f))

    #establish UKG API
    username = endpoint["UTM_Base"]["username"]
    password = endpoint["UTM_Base"]["password"]
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    s = requests.Session()
    s.auth = my_auth
    return s

def load_ukg(startDate: str|datetime, endDate: str|datetime)->list[pd.DataFrame]:
    if isinstance(startDate, datetime):
        filterStartDate = get_date_string(startDate)
        filterEndDate = get_date_string(endDate)
    else:
        filterStartDate = startDate
        filterEndDate = endDate
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    host_url = endpoint["UTM_Base"]["base_url"]
    attributes = endpoint["attribute_endpoints"]

    #setup sqlite db file
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    models.Base.metadata.drop_all(engine)
    models.Base.metadata.create_all(engine)

    result_dataframes = []
    s = create_session()
    attributes_dataframes_dict = load_attributes(
            s,attributes=attributes,host_url=host_url,engine=engine
            )
    result_dataframes.extend(attributes_dataframes_dict.values())
    main_endpoints = [
        MainEndpoint("Employee", None, "EmpId"),
        MainEndpoint("Time",
                      f'$filter=WorkDate ge {filterStartDate} and WorkDate le {filterEndDate}&OrgLevel1Id eq {ORGLEVEL1ID}',
                      "Id"
        )
    ]
    for main_endpoint in main_endpoints:
        print(f'trying {main_endpoint.table_name}...',end="")
        try:
            r = s.get(f'{host_url}/{main_endpoint.table_name}',params=main_endpoint.params)
            r.raise_for_status()
        except Exception as e:
            print(e)
        main_df = pd.DataFrame.from_records(r.json()["value"], index=main_endpoint.idCol)
        main_df.to_sql(main_endpoint.table_name,engine,if_exists='append')

        #combine with attribute tables before sending to csv
        main_df = combine_df(main_df,attributes_dataframes_dict)
        main_df.to_csv(f'{OUTPUT_FILE_PATH}/{main_endpoint.table_name}.csv')
        main_df.Name = main_endpoint.table_name
        print(f"record count for {main_endpoint.table_name}: {len(main_df.index)}")
        result_dataframes.append(main_df)
    charge_rate_df = pd.read_csv("./DataFiles/Employee_Charge_Rates.csv",index_col="UKG Name")
    add_charge_rates(Session(engine), charge_rate_df, "Charge Rate")
    s.close()
    add_CMiC_Dept_Name("./Datafiles/DEPARTMENT MAP.csv")
    engine.dispose()
    return result_dataframes
def combine_df(main_df: pd.DataFrame, attr_dataframes_dict: dict[str, pd.DataFrame])-> pd.DataFrame:
    """
    for each idColumn, dataframe pair in the attr_dataframes_dict:
          this will left merge into the main dataframe.
          Using the idcolumn in main_df and index col of the attr_dict.
    """
    for idColumn, content_dataframe in attr_dataframes_dict.items():
        main_df = main_df.merge(content_dataframe,left_on=[idColumn],right_index=True)
    return main_df
def add_charge_rates(session: Session, charge_rates_df: pd.DataFrame, chargeRateColumn: str):
    charge_rates_dict = charge_rates_df.to_dict()
    stmt = Select(models.Employee)
    all_employees: list[models.Employee] = session.execute(stmt).scalars().all()
    for employee in all_employees:
        employee.ChargeRate = charge_rates_dict[chargeRateColumn].get(employee.EmpId,0)
    session.commit()
def get_date_string(d: datetime):
    format_string = '%Y-%m-%d'
    return d.strftime(format_string)
def load_attributes(s: requests.Session, attributes: dict, host_url: str, engine: sqlalchemy.Engine)-> dict[str, pd.DataFrame]:
    #idColumn is the column name in the employee table
    result_dict = dict()
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
        content = pd.DataFrame.from_records(r.json()["value"], index="Id")
        content.to_sql(f"{ep}",engine,if_exists='append', method='multi')
        content = content.rename(columns={"Name": f'{ep}_Name'
                                ,"Description":f'{ep}_Description'
                                }
                                ,errors='raise')
        content.to_csv(f'{OUTPUT_FILE_PATH}/{ep}.csv')
        content.Name = ep
        result_dict[idColumn] = content
        print(f"record counts for {ep}:{len(content.index)}")
    return result_dict
def add_CMiC_Dept_Name(filename: str):
    engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
    stmt = Select(models.Location)
    with Session(engine) as session:
        all_locations = session.execute(stmt).scalars().all()
        dept_map_df = pd.read_csv(filename, header=0, usecols=[1, 3])
        dept_map_df.columns = ['UKGDName', 'CMiCD']
        dept_map = {
            str(k).strip(): str(v).strip()
            for k, v in zip(dept_map_df['UKGDName'], dept_map_df['CMiCD'])
        }       

        for location in all_locations:
            location.CMiC_Department_ID = dept_map.get(location.name)
        remapped_locations = [location for location in all_locations if location.CMiC_Department_ID]
        # print(remapped_locations)
        session.commit()
if __name__ == "__main__":
    startdate = datetime(2025,4,25)
    enddate = datetime(2025,5,9)
    load_ukg(startdate, enddate)