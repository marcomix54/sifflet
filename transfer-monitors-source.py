import requests
import csv
import os
from ruamel.yaml import YAML
import uuid

#### Modify at the bottom of the script the def main(): section ####
####################################################################

def query_origin_monitors(bearer_token, tenant, origin_UUID, tag_monitor_to_transfer):
    print(f"1 - Initiated query to https://{tenant}.siffletdata.com")

    url = f"https://{tenant}.siffletdata.com/api/v1/monitors"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {bearer_token}"
    }

    if not tag_monitor_to_transfer:
        print(f"--> Filter won't be based on a tag.")
        data = {
        "textSearch": "",
        "criticality": [],
        "datasource": [origin_UUID],
        "sort": ["lastRunDate,ASC"],
        "itemsPerPage": 10000,
        "page": 0,
        "domain": "All"
    }
    else:
        print(f"--> Filter will be based on tag {tag_monitor_to_transfer}.")
        data = {
            "textSearch": "",
            "criticality": [],
            "datasource": [origin_UUID],
            "tag":[tag_monitor_to_transfer],
            "sort": ["lastRunDate,ASC"],
            "itemsPerPage": 10000,
            "page": 0,
            "domain": "All"
        }
    
    response = requests.post(url, json=data, headers=headers)
    print('2 - Answer received...')
    if response.status_code == 200:
        return response.json()["searchRules"]["data"]
    else:
        print(f"ERROR: Failed to retrieve monitors. Status code: {response.status_code}")
        return []

def save_origin_monitors_to_csv(monitors, path_origin_monitors_yaml, csv_origin_monitors):
    print(f"3 - Summary list of monitors saved in {path_origin_monitors_yaml}/{csv_origin_monitors}.csv")
    with open(f"{path_origin_monitors_yaml}/{csv_origin_monitors}.csv", 'w', newline='') as csvfile:
        fieldnames = ['id', 'name', 'createdBy']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for monitor in monitors:
            writer.writerow({
                'id': monitor.get('id', 'N/A'), 
                'name': monitor.get('name', 'N/A'), 
                'createdBy': monitor.get('createdBy', {}).get('login', 'N/A')
            })

def read_ids_from_csv(path_origin_monitors_yaml, csv_origin_monitors):
    ids = []
    with open(f"{path_origin_monitors_yaml}/{csv_origin_monitors}.csv", 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            ids.append(row['id'])
    return ids

def get_monitor_details(bearer_token, tenant, monitor_id):
    url = f"https://{tenant}.siffletdata.com/api/ui/v1/rules/{monitor_id}"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {bearer_token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(f"Success for monitor {monitor_id}")
        return response.json()
    else:
        print(f"ERROR: Failed to retrieve details for monitor ID {monitor_id}. Status code: {response.status_code}")
        return None

def convert_rule_to_code(bearer_token, tenant, rule_details):
    url = f"https://{tenant}.siffletdata.com/api/ui/v1/rules/_convert-to-code"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {bearer_token}"
    }
    response = requests.post(url, json=rule_details, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"ERROR: Failed to convert rule to code. Status code: {response.status_code}")
        return None

def save_yaml(path, file_name, data):
    yaml = YAML()
    yaml.default_flow_style = False
    with open(os.path.join(path, f"{file_name}.yaml"), 'w') as yamlfile:
        yaml.dump(data, yamlfile)

def clear_directory(path):
    for file_name in os.listdir(path):
        file_path = os.path.join(path, file_name)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"ERROR: Failed to delete {file_path}. Reason: {e}")

def modify_and_copy_yaml_files(path_origin, path_destination, prefix, destination_UUID):
    print(f"""

    5 -  Modification and copy of monitors to {path_destination}
    """)
    yaml = YAML()
    for file_name in os.listdir(path_origin):
        if file_name.endswith(".yaml"):
            with open(os.path.join(path_origin, file_name), 'r') as yamlfile:
                data = yaml.load(yamlfile)
            
            # Generate a new UUID for the top-level "id"
            data["id"] = str(uuid.uuid4())
            
            # Add prefix to the name
            data["name"] = f"{prefix} - {data['name']}"
            
            # Remove tags if they exist
            if "tags" in data:
                del data["tags"]

            # Modify datasets
            for dataset in data.get("datasets", []):
                if "id" in dataset:
                    del dataset["id"]
                if "datasource" in dataset:
                    if "id" in dataset["datasource"]:
                        dataset["datasource"]["id"] = destination_UUID
                    if "name" in dataset["datasource"]:
                        del dataset["datasource"]["name"]
            
            # Save modified data to destination path
            new_file_name = f"converted-{os.path.splitext(file_name)[0]}"
            save_yaml(path_destination, new_file_name, data)
            print(f"{file_name} completed")

def patch_monitor(bearer_token, tenant, monitor_id, tag_monitor_transferred):
    if not tag_monitor_transferred:
        print(f"Skipping patch for monitor ID {monitor_id} because tag_monitor_transferred is empty.")
        return
    
    url = f"https://{tenant}.siffletdata.com/api/ui/v1/rules/{monitor_id}"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {bearer_token}"
    }
    payload = {
        "tagIds": [tag_monitor_transferred],
    }
    response = requests.patch(url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"Successfully patched monitor ID {monitor_id}")
    else:
        print(f"ERROR: Failed to patch monitor ID {monitor_id}. Status code: {response.status_code}")

def main():
    ###### Please update this section ######
    # Update bearer_token
    bearer_token = "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiI5YWQ5NThmMS05MTNlLTQ2ZWItOGU3YS1jZWZkNTdmNDZhMTkiLCJpc3MiOiJBY2Nlc3NUb2tlblNlcnZpY2UiLCJleHAiOjE3Nzk0MDAxNDIsImF1dGhvcml0aWVzIjoiQWRtaW4ifQ.7VbzgzcYk5ZMi82Ws6mMy2Nug14s3UcWOGyQlWHL1U7chDIhchE-Gs-qYnMHLtpAeyzFtNEXYtu1Uw3rfc0v9A"
    # Update tenant name e.g. demo for https://demo.siffletdata.com
    tenant = "onboarding"

    # Update path_origin_monitors_yaml
    path_origin_monitors_yaml = "/Users/marcmontanari/Documents/Codes/Transfer-Monitors-Source/yaml_origin"
    # csv name for origin monitors (no need to change)
    csv_origin_monitors = "origin_monitors"
    # Origin source UUID
    origin_UUID = "5d862786-4998-4091-baf9-c44a4383e48d"

    # Update path_destination_monitors_yaml
    path_destination_monitors_yaml = "/Users/marcmontanari/Documents/Codes/Transfer-Monitors-Source/yaml_destination"
    # Destination source UUID
    destination_UUID = "79a5e543-40b3-4651-b4e3-6eff5c38a447"
    
    # Prefix to add to all duplicated monitors to new source
    prefix_new_monitors = "Source XYZ"

    # Tag UUIDs
    # (Optional) - UUID of tag to use in query to find which monitors should be transferred
    tag_monitor_to_transfer = "07247885-7728-4bbd-976d-c6510f95369c"
    # (Optional) - UUID of tag to use to flag when a monitor has been transferred
    tag_monitor_transferred = "4badc485-4436-48c2-88d5-5ac869af8611"



    ###### No action needed beyond this point ######
    # Clear both directories
    print("0 - Cleaning directories.")
    clear_directory(path_origin_monitors_yaml)
    clear_directory(path_destination_monitors_yaml)

    monitors = query_origin_monitors(bearer_token, tenant, origin_UUID, tag_monitor_to_transfer)
    
    if monitors:
        save_origin_monitors_to_csv(monitors, path_origin_monitors_yaml, csv_origin_monitors)
    else:
        print("No monitors found.")
        return
    
    ids = read_ids_from_csv(path_origin_monitors_yaml, csv_origin_monitors)
    
    print(f"""

    4 - Initiating request of details for monitors...
    """)
    for monitor_id in ids:
        monitor_details = get_monitor_details(bearer_token, tenant, monitor_id)
        if monitor_details:
            code_response = convert_rule_to_code(bearer_token, tenant, monitor_details)
            if code_response:
                save_yaml(path_origin_monitors_yaml, monitor_id, code_response)
                print(f"Conversion saved to converted-{monitor_id}.yaml")
                patch_monitor(bearer_token, tenant, monitor_id, tag_monitor_transferred)
            else:
                print(f"ERROR: Failed to convert rule for monitor ID {monitor_id}")
        else:
            print(f"WARNING: No details found for monitor ID {monitor_id}")
    
    # Modify and copy YAML files to destination
    modify_and_copy_yaml_files(path_origin_monitors_yaml, path_destination_monitors_yaml, prefix_new_monitors, destination_UUID)
    
    print(f"""
    
    END: This script converted {len(ids)} monitors.
    Please create and apply workspace with yaml_destination monitors using Sifflet CLI.
    """)

if __name__ == "__main__":
    main()
