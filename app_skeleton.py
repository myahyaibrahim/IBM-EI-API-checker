import ibmpairs.client as client
import ibmpairs.query as query
import ibmpairs.catalog as catalog

# other imports
import pandas as pd
import configparser
import json
import time
import math
import os

############################
# Create connection to server
############################
config = configparser.RawConfigParser()
config.read('auth/secrets.ini')
EI_ORG_ID     = config.get('EI', 'api.org_id') 
EI_TENANT_ID  = config.get('EI', 'api.tenant_id') 
EI_API_KEY     = config.get('EI', 'api.api_key')

print(EI_ORG_ID)
print(EI_TENANT_ID)
print(EI_API_KEY)

EI_client_v3 = client.get_client(
    org_id    = EI_ORG_ID,
    tenant_id = EI_TENANT_ID,
    api_key   = EI_API_KEY,
    version   = 3
)


# Get all the datasets
dl_list = catalog.get_data_layers()

# Convert the data layers (ibmpair object) to JSON/Dict data type
dl_list_json = json.loads(dl_list.to_json()) 


# Get list of SPATIAL COVERAGE TYPE in data layers
# Iterate each of the data layers
dl_spatial_coverage_list = []
for dl in dl_list_json['data_layers']:
    for country in dl['spatial_coverage']['country']:
        if country not in dl_spatial_coverage_list:
            dl_spatial_coverage_list.append(country)

# print("List of spatial coverage in data layers")
# print(dl_spatial_coverage_list)


# Convert list of SPATIAL COVERAGE TYPE ===> dictionary python
dl_spatial_coverage_dict = {}
for spatial_coverage in dl_spatial_coverage_list:
    dl_spatial_coverage_dict[spatial_coverage] = []

# print("Data Layers Dict")
# print(dl_spatial_coverage_dict)


# Filter data layers to the SPATIAL COVERAGE TYPE dictionary
for dl in dl_list_json['data_layers']:
    for spatial_coverage_key in dl_spatial_coverage_dict.keys():
        if spatial_coverage_key in dl['spatial_coverage']['country']:
            temp = dl_spatial_coverage_dict.get(spatial_coverage_key)
            temp.append({
                "id": dl['id'],
                "name":dl['name'],
                # "name_alternate":dl['name_alternate'],
                # "description_short": dl['description_short'],
                "latitude_max": dl['latitude_max'],
                "latitude_min": dl['latitude_min'],
                "longitude_max": dl['longitude_max'],
                "longitude_min": dl['longitude_min'],
            })
            dl_spatial_coverage_dict.update({spatial_coverage_key: temp})

# print('Global')
# print(dl_spatial_coverage_dict['Global'])

# print('Global provisioning on-demand')
# print(dl_spatial_coverage_dict['Global provisioning on-demand'])


# Batch job
total_data_layer = len(dl_spatial_coverage_dict['Global'])
batch_size = 3
batch_number = math.ceil(total_data_layer/batch_size)
print("Total Global Data Layer Available: ", total_data_layer)

print("Choose batch")
for i in range (batch_number):
    print(i+1, end = ". ")
    start = ((i+1-1) * batch_size) + 1
    end = (i+1) * batch_size
    if end > total_data_layer:
        end = total_data_layer
    for j in range (start, end+1):
        print(j, end=" ")
    print("")


# Iterate to check if data layers has data (values) for Jakarta, Indonesia location
# JKT (Indonesia) = [latitute, longitude] = [ -6.2087634, 106.845599 ]
selected_batch_number = 1
start = ((selected_batch_number-1) * batch_size)
end = (selected_batch_number) * batch_size
if end > total_data_layer:
    end = total_data_layer

# Loop through the data layers
export_dict = {
    "batch number": selected_batch_number,
    "total observed data layers": end-start,
    "available data layers (value)": 0,
    "id data layers": [],
}

print(export_dict)

for idx in range (start, end):
    print("idx:", idx)
    query_json = { "name": "Test - Indonesia",
                    "layers": [ {  "id": dl_spatial_coverage_dict['Global'][idx]['id'], "type": "raster"  } ], 
                    "spatial":  {  "type": "point", "coordinates": [ -6.2087634, 106.845599 ]},
                    "temporal": {  "intervals": [ { "start": "2023-12-31 00:00:00", "end": "2024-01-01 01:00:00"  } ] }
                }

    print(dl_spatial_coverage_dict['Global'][idx]['name'])

    # Submit the query
    # print("SUBMIT QUERY")
    query_result = query.submit(query_json, client=EI_client_v3)
    # print(query_result)
    # print("")

    # print("To dict")
    query_result_dict = query_result.to_dict()
    # print(query_result_dict['submit_response'])
    # print("")
    # print(query_result_dict['submit_response']['data'])
    # if (query_result_dict['submit_response']['data'] == []):
    #     print("KOSOSNG")

    if (query_result_dict['submit_response']['data'] != []):
        export_dict["available data layers (value)"] = export_dict["available data layers (value)"] + 1
        export_dict["id data layers"].append(query_result_dict['layers'][0]['id'])
        print("Convert")
        # Convert the results to a dataframe
        point_df = query_result.point_data_as_dataframe()
        # Convert the timestamp to a human readable format
        point_df['datetime'] = pd.to_datetime(point_df['timestamp'] * 1e6, errors = 'coerce')
        print(point_df)

print("FINSIHED")
print("Result: ", export_dict)

# Dump to JSON File
# Specify the custom file path
filename = "batch_" + str(selected_batch_number) + ".json"
file_path = "./export/" + filename

# Ensure the directory exists
os.makedirs(os.path.dirname(file_path), exist_ok=True)


with open(file_path, "w") as json_file:
    json.dump(export_dict, json_file, indent=4)
