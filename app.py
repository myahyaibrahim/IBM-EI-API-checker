import ibmpairs.client as client
import ibmpairs.query as query
import ibmpairs.catalog as catalog

# other imports
import pandas as pd
import configparser
import json
import math
import streamlit as st

# Set the page to wide mode
st.set_page_config(layout="wide")


# Get list of SPATIAL COVERAGE TYPE in data layers
def get_spatial_coverage_type(dl_list_json):
    dl_spatial_coverage_list = []
    # Iterate each of the data layers
    for dl in dl_list_json['data_layers']:
        for country in dl['spatial_coverage']['country']:
            if country not in dl_spatial_coverage_list:
                dl_spatial_coverage_list.append(country)
    return dl_spatial_coverage_list


# Convert list of SPATIAL COVERAGE TYPE ===> dictionary python
def convert_ibmObject_to_dictionary(dl_spatial_coverage_list, dl_list_json):
    dl_spatial_coverage_dict = {}
    for spatial_coverage in dl_spatial_coverage_list:
        dl_spatial_coverage_dict[spatial_coverage] = []

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
    return dl_spatial_coverage_dict

# Main function
def main():
    # Title
    st.title("IBM Environmental Intelligence API - Coverage Checker")
    st.header("Simple application for checking geospatial coverage in IBM Environmental Intelligence API")

    ############################
    # Create connection to server
    ############################
    config = configparser.RawConfigParser()
    config.read('auth/secrets.ini')
    EI_ORG_ID     = config.get('EI', 'api.org_id') 
    EI_TENANT_ID  = config.get('EI', 'api.tenant_id') 
    EI_API_KEY     = config.get('EI', 'api.api_key')

    EI_client_v3 = client.get_client(
        org_id    = EI_ORG_ID,
        tenant_id = EI_TENANT_ID,
        api_key   = EI_API_KEY,
        version   = 3
    )

    # Get all the datasets
    dl_list = catalog.get_data_layers()
    st.dataframe(dl_list.display())

    # Convert the data layers (ibmpair object) to JSON/Dict data type
    dl_list_json = json.loads(dl_list.to_json()) 

    # Get list of SPATIAL COVERAGE TYPE in data layers
    dl_spatial_coverage_list = get_spatial_coverage_type(dl_list_json)

    # Convert list of SPATIAL COVERAGE TYPE ===> dictionary python
    dl_spatial_coverage_dict = convert_ibmObject_to_dictionary(dl_spatial_coverage_list, dl_list_json)

    # BATCH JOB SECTION
    st.markdown("## Total Global Data Layer")
    total_data_layer = len(dl_spatial_coverage_dict['Global'])
    print("Total Global Data Layer Available: ", total_data_layer)
    st.write("Total Global Data Layer Available:", total_data_layer)

    st.markdown("## Configure batching size")
    # Initialize session state for the batch size input
    if "batch_size" not in st.session_state:
        st.session_state["batch_size"] = 1

    batch_size = st.number_input(
        "Enter the batch size", 
        min_value=1,  # Optional: Set a minimum value
        step=1,       # Ensure increments are in integers
        # value=st.session_state["batch_size"],      # Default value as an integer
        format="%d",  # Force the input to display as an integer
        key="batch_size"
    )
    st.write("Your batch size is: ", batch_size)

    proceed = st.button("Submit")
    if proceed or "batch_size" in st.session_state:
        st.markdown("### Choose the batch number")
        batch_number = math.ceil(total_data_layer/batch_size)
        st.write("Number of batches:", batch_number)

        batch_number_component_list = []
        batch_number_list = []
        for i in range (batch_number):
            temp = ""
            # temp = "Batch " + str(i+1) + ": Data layers "
            # print(i+1, end = ". ")
            start = ((i+1-1) * batch_size) + 1
            end = (i+1) * batch_size
            if end > total_data_layer:
                end = total_data_layer
            for j in range (start, end+1):
                # print(j, end=" ")
                if j == end:
                    temp = temp + str(j)
                else:
                    temp = temp + str(j) + ", "
            batch_number_component_list.append(temp)
            batch_number_list.append(i+1)
        
        table_data_layers = {
            "Batch Number": batch_number_list,
            "Data Layers": batch_number_component_list,
        }

        # Display the table as an interactive dataframe
        data_layers_df = pd.DataFrame(table_data_layers)
        st.dataframe(data_layers_df)
        
        # Initialize session state for the selected_batch_number input
        if "selected_batch_number" not in st.session_state:
            st.session_state["selected_batch_number"] = 1
        selected_batch_number = st.selectbox("Batch Number", batch_number_list, key="selected_batch_number")
        st.write("Your selected batch number is: ", selected_batch_number)

        execute = st.button("Execute")
        if execute:
            # Iterate to check if data layers has data (values) for Jakarta, Indonesia location
            # JKT (Indonesia) = [latitute, longitude] = [ -6.2087634, 106.845599 ]
            st.markdown("## Checking the API Data")
            start = ((selected_batch_number-1) * batch_size) + 1
            end = (selected_batch_number) * batch_size
            if end > total_data_layer:
                end = total_data_layer
            
            # Loop through the data layers
            export_dict = {
                "batch number": selected_batch_number,
                "total observed data layers": end-start+1,
                "available data layers (value)": 0,
                "id data layers": [],
            }

            for idx in range (start, end+1):
                print("idx:", idx)
                query_json = { "name": "Test - Indonesia",
                                "layers": [ {  "id": dl_spatial_coverage_dict['Global'][idx]['id'], "type": "raster"  } ], 
                                "spatial":  {  "type": "point", "coordinates": [ -6.2087634, 106.845599 ]},
                                "temporal": {  "intervals": [ { "start": "2023-12-31 00:00:00", "end": "2024-01-01 01:00:00"  } ] }
                            }

                st.text(dl_spatial_coverage_dict['Global'][idx]['name'])
                print(dl_spatial_coverage_dict['Global'][idx]['name'])

                # Submit the query
                # print("SUBMIT QUERY")
                query_result = query.submit(query_json, client=EI_client_v3)
                # print(query_result)
                # print("")

                # Convert to dictionary
                # print("To dict")
                query_result_dict = query_result.to_dict()
                # print(query_result_dict['submit_response'])
                # print("")

                if (query_result_dict['submit_response']['data'] != []):
                    export_dict["available data layers (value)"] = export_dict["available data layers (value)"] + 1
                    export_dict["id data layers"].append(query_result_dict['layers'][0]['id'])
                    # Convert the results to a dataframe
                    point_df = query_result.point_data_as_dataframe()
                    # Convert the timestamp to a human readable format
                    point_df['datetime'] = pd.to_datetime(point_df['timestamp'] * 1e6, errors = 'coerce')
                    # print(point_df)
                    st.dataframe(point_df)
                else:
                    st.text("No data found!!! Empty data!!!")

            # Dump to JSON File
            filename = "batch_" + str(selected_batch_number) + ".json"
            json_data = json.dumps(export_dict, indent=4)

            st.download_button(
                label="Download as JSON File",
                data=json_data,
                file_name=filename,
                mime="application/json"
            )
        else:
            st.warning("Waiting for to be executed")

    else:
        st.warning("Waiting for user to input a batch size and confirm.")



if __name__ == "__main__":
    main()