import requests
from requests.auth import HTTPBasicAuth
import subprocess
import argparse
import sys
import concurrent.futures
import asyncio
from tabulate import tabulate
from requests.adapters import HTTPAdapter

parser = argparse.ArgumentParser(description="Get the status of collections.")
parser.add_argument("-t", "--target", help="Specify Target Fusion Environment IP/Hostname", required=True)
parser.add_argument("-u", "--username", help="Specify Fusion username", required=True)
parser.add_argument("-f", "--file", help="Specify file name to write to", required=False)
parser.add_argument("-c", "--collection", help="Specify a single collection to output", required=False)
parser.add_argument("-p", "--password-item", help="Specify 1Password item title to retrieve Fusion password", required=True)
args = parser.parse_args()

def firstPassword(item_property):
    try:
        items = subprocess.check_output(["op", "item", "get", item_property], stderr=subprocess.STDOUT).decode("utf-8")
        lines = items.split("\n")
        for line in lines:
            if line.strip().startswith("password:"):
                password = line.split(":")[1].strip()
                return password
        print("Error: Password not found in 1Password CLI output.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error fetching password from 1Password: {e.output}")
        sys.exit(1)

def get_collections(url, username, pwd):
    headers = {"Content-Type":"application/json"}
    basic = HTTPBasicAuth(username, pwd)
    sesh = requests.Session()
    sesh.mount('https://', HTTPAdapter(pool_connections=3))
    response = sesh.get(url + "/api/collections", auth=basic, headers=headers)
    return response.json()

def get_collection_status(url, username, pwd):
    headers = {"Content-Type":"application/json"}
    basic = HTTPBasicAuth(username, pwd)
    response = requests.get(url, auth=basic, headers=headers)
    return response.json()

def get_collection_urls(url, ids_list):
    return [url + "/api/collections/" + id + "/status/" for id in ids_list]

async def main():
    url = args.target
    if not url.startswith("http"):
        url = "https://" + url
    username = args.username
    pwd = firstPassword(args.password_item)

    collections_list = get_collections(url, username, pwd)
    ids_list = [collection["id"] for collection in collections_list]
    urls_list = get_collection_urls(url, ids_list)

    clusterstatus_dict = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(get_collection_status, url, username, pwd) for url in urls_list]
        concurrent.futures.wait(futures)
        for status in futures:
            if "configName" not in status.result().keys():
                continue
            this_status = status.result()
            this_id = this_status["configName"]
            clusterstatus_dict[this_id] = this_status

    if args.collection:
        collection_name = args.collection
        if collection_name in clusterstatus_dict:
            clusterstatus_dict = {collection_name: clusterstatus_dict[collection_name]}
        else:
            print(f"Error: Collection '{collection_name}' not found.")
            sys.exit(1)

    formatted_output = []
    for collection_name, details in clusterstatus_dict.items():
        for shard, shard_data in details['shards'].items():
            for replica, replica_data in shard_data['replicas'].items():
                formatted_output.append([
                    collection_name.replace(":8983/solr", ""),
                    details['replicationFactor'],
                    details['maxShardsPerNode'],
                    shard,
                    replica_data['state'],
                    replica_data['core'],
                    replica_data['base_url']
                ])

    table_headers = ["Collection", "Replication", "Max Shards", "Shard", "State", "Core", "Base URL"]
    print(tabulate(formatted_output, headers=table_headers, tablefmt="pretty"))

if __name__ == '__main__':
    import warnings
    warnings.simplefilter('ignore')
    asyncio.run(main())
