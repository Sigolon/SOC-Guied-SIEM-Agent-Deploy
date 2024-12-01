import os, json, requests
from requests.auth import HTTPBasicAuth

def bulk_api_upload(target_url, bulk_data, ca_crt):
    headers = {
        "Content-Type": "application/json"
    }

    auth = HTTPBasicAuth('elastic', 'ik1FseVSFtb+Es0pW9Pu')

    response = requests.post(url=target_url, headers=headers, data=bulk_data, verify=ca_crt, auth=auth, timeout=10)
    if json.loads(response.text)["errors"] != False : 
        return "error"
    else : 
        return 200

def upload_error_data_store(store_error_ndjson_path, ndjson_data):
    if not os.path.exists(store_error_ndjson_path):
        with open(store_error_ndjson_path, "w") as f:
            pass

    if not isinstance(ndjson_data, str):
        raise ValueError("ndjson_data should be a string")

    with open(store_error_ndjson_path, "a") as error_file:
        if not ndjson_data.endswith("\n"):
            ndjson_data += "\n"
        error_file.write(ndjson_data)