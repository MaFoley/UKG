import requests

def main():
    host_url = r'https://nova-api.cmiccloud.com/cmicprod'
    username = r'HJR||NSMITH'
    password = input('please enter API account password:')
    my_auth = requests.auth.HTTPBasicAuth(username, password)
    s = requests.Session()
    s.auth = my_auth
    endpoint = r'hcm-rest-api/rest/1/pypayrun'
    print(f'host_url: {host_url},\nusername:{username}, password:{password}')
    payload = {'finder': 'PrimaryKey;PrnCode=B'}
    r = s.get(f"{host_url}/{endpoint}",params=payload)
    print(r.json())
if __name__ == "__main__":
    main()
