from tools.tools import bulk_api_upload, upload_error_data_store
import os,  tqdm

ndjson_file = ""  # "absolute_path" modify this
ca_crt = "" # "absolute_path" modify this
target_url = "https://localhost:9200/_bulk?pretty"
index = "" # modify this
store_error_ndjson_path = os.path.dirname(ndjson_file) + "/error.ndjson"

BULK_SIZE = 500
with open(ndjson_file, "r") as f:
    ndjson_data_lines = f.readlines()

bulk_data = ""
counter = 0
for ndjson_data in tqdm(ndjson_data_lines, desc="Processing JSON lines") : 

    bulk_data += f'{{ "index" : {{ "_index" : "{index}" }} }}\n'
    bulk_data += ndjson_data
    counter += 1

    if counter % BULK_SIZE == 0:
        response_status = bulk_api_upload(target_url, bulk_data, ca_crt)
        if response_status != 200:
            upload_error_data_store(store_error_ndjson_path, bulk_data)
        bulk_data = "" 

if bulk_data:
    response_status = bulk_api_upload(target_url, bulk_data, ca_crt)
    if response_status != 200:
        upload_error_data_store(store_error_ndjson_path, bulk_data)