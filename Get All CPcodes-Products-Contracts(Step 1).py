import requests
import os
import csv
import json
from akamai.edgegrid import EdgeGridAuth, EdgeRc
from urllib.parse import urljoin


def initialize_akamai_session(edgerc_path):
    """Initializes an Akamai EdgeGrid session."""
    try:
        edgerc = EdgeRc(edgerc_path)
        hostname = edgerc.get('default', 'host')
        client_token = edgerc.get('default', 'client_token')
        client_secret = edgerc.get('default', 'client_secret')
        access_token = edgerc.get('default', 'access_token')
        session = requests.Session()
        session.auth = EdgeGridAuth(client_token, client_secret, access_token)
        session.headers.update({"accept": "application/json"})
        return session, hostname
    except Exception as e:
        print(f"Error initializing Akamai session: {e}")
        return None, None


def get_all_cpcodes(session, hostname, account_switch_key=None):
    """Fetches all CP codes from the Akamai CPRG API."""
    try:
        path = "/cprg/v1/cpcodes"
        url = urljoin(f'https://{hostname}', path)
        params = {"accountSwitchKey": account_switch_key} if account_switch_key else {}
        response = session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching CP codes: {e}")
        return None


def write_cpcodes_to_csv(cpcode_data, account_switch_key, filename_prefix="All"):
    """Writes raw CP code data to a CSV file."""
    if not cpcode_data or 'cpcodes' not in cpcode_data or not cpcode_data['cpcodes']:
        print("No CP code data to write to CSV.")
        return None

    try:
        filename = f"{filename_prefix}_{account_switch_key}_CPcodes.csv"
        fieldnames = cpcode_data['cpcodes'][0].keys()
        with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for cpcode in cpcode_data['cpcodes']:
                writer.writerow(cpcode)
        print(f"✅ Raw CP codes written to: {filename}")
        return filename
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}")
        return None


def clean_contracts(contracts_str):
    """Cleans contracts field into 'Expired:C-14DACIX,Ongoing:C-14D88N9' format."""
    try:
        contracts = json.loads(contracts_str.replace("'", '"'))
        filtered = [f"{c['status'].capitalize()}:{c['contractId']}" for c in contracts if
                    'contractId' in c and 'status' in c]
        return ",".join(filtered) if filtered else ""
    except (json.JSONDecodeError, TypeError):
        return contracts_str  # Return original if parsing fails


def clean_timezone(timezone_str):
    """Cleans overrideTimezone to show only the timezone value, e.g., 'GMT 0'."""
    try:
        timezone_dict = json.loads(timezone_str.replace("'", '"'))
        return timezone_dict.get('timezoneValue', timezone_str).replace(" (Greenwich Mean Time)", "")
    except (json.JSONDecodeError, TypeError):
        return timezone_str  # Return original if parsing fails


def clean_products(products_str):
    """Cleans products to show all product names, e.g., 'AdaptiveMediaDelivery, Progressive_Media'."""
    try:
        products = json.loads(products_str.replace("'", '"'))
        if isinstance(products, list) and products:
            # Extract base product names from productId, removing the namespace prefix before '::'
            product_names = [
                p.get('productId', '').split('::')[-1] if '::' in p.get('productId', '') else p.get('productId', '')
                for p in products if p.get('productId')]
            return ", ".join(product_names) if product_names else ""
        return ""
    except (json.JSONDecodeError, TypeError):
        return products_str  # Return original if parsing fails


def clean_access_group(access_group_str):
    """Cleans accessGroup to show only the contractId, e.g., 'C-14D88N9', without surrounding JSON."""
    try:
        # Remove the JSON structure and extract just the contractId value
        access_group = json.loads(access_group_str.replace("'", '"'))
        return access_group.get('contractId', access_group_str)
    except (json.JSONDecodeError, TypeError):
        # If parsing fails, try to extract the contractId manually
        if "contractId" in access_group_str:
            start = access_group_str.find("'contractId': '") + len("'contractId': '")
            end = access_group_str.find("'", start)
            return access_group_str[start:end]
        return access_group_str  # Return original if all else fails


def clean_csv(input_filename, account_switch_key):
    """Reads the raw CSV and writes a cleaned version."""
    if not os.path.exists(input_filename):
        print(f"Input file {input_filename} does not exist.")
        return

    output_filename = f"Cleaned_All_{account_switch_key}_CPcodes.csv"
    with open(input_filename, mode='r', encoding='utf-8') as infile, \
            open(output_filename, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            # Clean specific columns
            row['overrideTimezone'] = clean_timezone(row['overrideTimezone'])
            row['contracts'] = clean_contracts(row['contracts'])
            row['products'] = clean_products(row['products'])
            row['accessGroup'] = clean_access_group(row['accessGroup'])
            writer.writerow(row)

    print(f"✅ Cleaned CP codes written to: {output_filename}")


def fetch_and_process_cpcodes(edgerc_path, account_switch_key=None):
    """Fetches CP codes, writes raw CSV, and cleans it."""
    session, hostname = initialize_akamai_session(edgerc_path)
    if not session:
        return

    # Fetch CP codes
    cpcode_data = get_all_cpcodes(session, hostname, account_switch_key)

    if cpcode_data:
        print("CP codes retrieved successfully:")
        print(cpcode_data)  # Print full response for verification

        # Write raw CSV
        raw_csv = write_cpcodes_to_csv(cpcode_data, account_switch_key)
        if raw_csv:
            # Clean the CSV
            clean_csv(raw_csv, account_switch_key)
    else:
        print("No CP code data returned.")


if __name__ == "__main__":
    edgerc_path = "/Users/afrolov/.edgerc"  # Replace with your .edgerc path
    account_switch_key = "F-AC-997250"  # Replace with your account switch key

    fetch_and_process_cpcodes(edgerc_path, account_switch_key)
