import tomllib, pandas, requests
import numpy as np
from functools import wraps
import logging, sys
OUTPUT_FILE_PATH = './DataFiles'
logger = logging.getLogger('ukg')
logger.level = logging.INFO
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
sh, fh = logging.StreamHandler(sys.stdout),logging.FileHandler(f"{OUTPUT_FILE_PATH}/middleware.log")
sh.setFormatter(formatter)
sh.setLevel(logger.level)
fh.setFormatter(formatter)
sh.setLevel(logger.level)
logger.addHandler(sh)
logger.addHandler(fh)

class UKGAPIClient:
    def __init__(self, host_url, auth=None, client_api_key = None):
        self.host_url = host_url
        self.session = requests.Session()
        if auth:
            self.session.auth = auth
            self.session.headers.update(client_api_key)
    @classmethod
    def create_session(cls) -> tuple[str, requests.auth.HTTPBasicAuth, dict]:
        with open("config.toml", "rb") as f:
            endpoint = tomllib.load(f)
        with open("secrets.toml","rb") as f:
            endpoint.update(tomllib.load(f))
        host_url = endpoint["UKG_Base"]["host_url"]
        username = endpoint["UKG_Base"]["username"]
        password = endpoint["UKG_Base"]["password"]
        my_auth = requests.auth.HTTPBasicAuth(username, password)
        client_api_key_header ={"US-CUSTOMER-API-KEY": 
                                endpoint["UKG_Base"]["client_api_key"]
        }
        return host_url, my_auth, client_api_key_header
    #TODO: transform into pagination
    @wraps(requests.Session.get)
    def get(self, endpoint_url: str, **kwargs):
        return self.session.get(url=f"{self.host_url}/{endpoint_url}",**kwargs)
    @wraps(requests.Session.post)
    def post(self, endpoint_url: str, **kwargs):
        return self.session.post(url=f"{self.host_url}/{endpoint_url}",**kwargs)
    def get_paginated_results(self, endpoint_url: str, params: dict,per_Page = 100, start_Page = 1) -> list:
        all_results = []
        params.update({"page":start_Page,"per_Page":per_Page})
        while True:
            try:
                r = self.get(endpoint_url,params=params)
                r.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"request failed, server response: {r.text}")
            current_page = r.json()
            if not current_page:
                break
            all_results.extend(current_page)
            params["page"] += 1
        return all_results

    def __del__(self):
        if self.session:
            self.session.close()
def calculate_charge_rates(df: pandas.DataFrame, earning_code_map: dict) -> pandas.DataFrame:
    #TODO: medge in orglevel1 to allow construction vs program management clean split
    emp_df = pandas.read_csv("./DataFiles/Employee.csv",
                             dtype={'EmpId':'string',
                                    'OrgLevel1_Name':'string'})
    emp_df['shortEmpId'] = emp_df['EmpId'].str[3:9]
    logger.info(emp_df.dtypes)
    subset_cols = ['shortEmpId','OrgLevel1_Name']
    emp_df_subset = emp_df[subset_cols]
    df_filtered = filter_to_company(df, earning_code_map)
    df_filtered['employeeNumber'] = df_filtered['employeeNumber'].astype(str)
    df_filtered = pandas.merge(df_filtered,
                               emp_df_subset,
                               how="left",
                               left_on="employeeNumber",
                               right_on="shortEmpId")

    # df_filtered = df_filtered.loc[df_filtered["pay_classification"] == "Wages"]
    df_summary = df_filtered.groupby(['pay_classification','employeeId','employeeNumber','payDate','OrgLevel1_Name']).sum().reset_index()
    group_cols = ['employeeId','employeeNumber','payDate','OrgLevel1_Name']
    df_pivot = df_summary.pivot_table(index=group_cols,
                                      columns='pay_classification',
                                      values='currentAmount',
                                      aggfunc='sum')
    df_pivot['Burden'] = df_pivot['Burden'].fillna(0)
    df_hours_sum = df_summary.groupby(group_cols)['currentHours'].sum().reset_index()
    df_pivot = pandas.merge(df_pivot, df_hours_sum,on=group_cols)
    # df_summary['chargeRate'] = df_summary['currentAmount'] / df_summary['currentHours']
    # df_summary['chargeRate'] = df_summary['chargeRate'].round(2)
    df_pivot = departmental_charge_rate(df_pivot)

    #write to file for inspection
    save_dataframe_graceful(df_filtered,f"{OUTPUT_FILE_PATH}/pay_classifications.csv")
    save_dataframe_graceful(df_summary,f"{OUTPUT_FILE_PATH}/charge_rates.csv")
    save_dataframe_graceful(df_pivot,f"{OUTPUT_FILE_PATH}/pivot_charge_rates.csv")

    #throw loud exception if unmapped earningCodes
    unmapped_check = df_filtered['pay_classification'].isna() 
    if unmapped_check.any():
        unmapped_codes = df_filtered.loc[unmapped_check, 'earningCode'].unique()
        logger.error(f"Unmapped earningCode(s) encountered in the DataFrame: {list(unmapped_codes)}. ")
        raise ValueError(f"Unmapped earningCode(s) encountered in the DataFrame: {list(unmapped_codes)}. "
        "Please update the 'earning_code_map' dictionary.")
    return df_summary

def filter_to_company(df, earning_code_map):
    companyId = 'PQCD4'
    df_filtered = df[df['companyId']==companyId].copy()
    df_filtered['pay_classification'] = df_filtered['earningCode'].map(earning_code_map)
    df_filtered['location'] = df_filtered['location'].astype(str)
    return df_filtered
def departmental_charge_rate(df: pandas.DataFrame)-> pandas.DataFrame:
    """adds charge rate column with logic for construction vs program management
    """
    department_map = { "Construction":"90070","Program Management":"90012" }
    CONSTRUCTION_WAGE_MARKUP = 1.20
    conditions = [
        df['OrgLevel1_Name'] == department_map["Program Management"],
        df['OrgLevel1_Name'] == department_map["Construction"]
    ]
    choices = [
        df['Wages'] + df['Burden'],
        df['Wages'] * CONSTRUCTION_WAGE_MARKUP
    ]
    df['chargeAmount'] = np.select(conditions,choices,default=0)
    df['chargeRate'] = df['chargeAmount'] / df['currentHours']
    df['chargeRate'] = df['chargeRate'].round(2)
    return df
def get_earnings_history():
    endpoint_url = r'payroll/v1/earnings-history-base-elements'
    sesh = UKGAPIClient(*UKGAPIClient.create_session())
    r = sesh.get_paginated_results(endpoint_url=endpoint_url,params={"periodControl":"202510101"})
    out_file = f"{OUTPUT_FILE_PATH}/ukg_earnings_history.csv"
    earnings_df = pandas.DataFrame(r)
    logger.info(f"saving {len(r)} records to {out_file}")
    earnings_df.to_csv(out_file)
    return earnings_df
def save_dataframe_graceful(df: pandas.DataFrame, path: str):
    successful = False
    while not successful:
        try:
            df.to_csv(path, index=False)
            logger.info(f"writing to file. {path} {len(df)=}")

            successful = True
        except PermissionError as pe:
            logging.warning(f"Permission denied error: {pe}")
            user_input = input("File write failed. Enter 'Y' to retry: ")
            if user_input.upper() == 'Y':
                logging.info(f"Retrying file write to {path}...")
            else:
                logging.error("File write aborted by user.")
                raise
        
        except Exception as e:
            logging.error(f"An unexpected error occurred while saving files: {e}")
            raise
def main():
    #earnings_df = get_earnings_history()
    earnings_df = pandas.read_csv("./DataFiles/ukg_earnings_history.csv")
    with open("config.toml", "rb") as f:
        endpoint = tomllib.load(f)
    charge_rates = calculate_charge_rates(earnings_df, endpoint["earnings_mapping"])
    return charge_rates
if __name__ == "__main__":
    main()

