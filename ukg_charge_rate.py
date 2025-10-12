import tomllib, pandas, requests
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
endpoint_url = r'payroll/v2/general-ledger'
sesh = UKGAPIClient(*UKGAPIClient.create_session())
r = sesh.get_paginated_results(endpoint_url=endpoint_url,params={"mostRecent":None})
out_file = f"{OUTPUT_FILE_PATH}/ukg_gl.csv"
df = pandas.DataFrame(r)
logger.info(f"saving {len(r)} records to {out_file}")
df.to_csv(out_file)

