import requests
import os
import csv
import time
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

def get_alert_details(session, hostname, account_switch_key, definition_id):
    """Retrieves alert details for a given definition ID."""
    try:
        path = f"/alerts/v2/alert-summaries/{definition_id}/details"
        params = {"accountSwitchKey": account_switch_key}
        url = urljoin(f'https://{hostname}', path)
        response = session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error getting alert details for {definition_id}: {e}")
        return None

def write_filtered_alerts_to_csv(filtered_alerts, filename):
    """Writes filtered alert details to a CSV file."""
    if not filtered_alerts:
        print("No filtered alerts to write to CSV.")
        return

    try:
        fieldnames = filtered_alerts[0].keys()
        file_exists = os.path.isfile(filename)

        with open(filename, mode='a' if file_exists else 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for alert in filtered_alerts:
                writer.writerow(alert)
        print(f"✅ Filtered alert details written to: {filename}")
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}")

def read_and_process_alerts(edgerc_path, account_switch_key, input_filename="alerts.csv", output_filename=None):
    """Reads alerts.csv, fetches details for rows with non '-' in 'lastTriggered', and writes to a new CSV."""
    if output_filename is None:
        output_filename = f"{account_switch_key}_alerts_details.csv"  # Compute default value here

    filtered_alerts = []
    try:
        with open(input_filename, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)

            for row in reader:
                if row['lastTriggered'].strip() != '-':
                    definition_id = row['definitionId'].strip()
                    print(f"--> Processing alert ID: {definition_id} with lastTriggered: {row['lastTriggered']}")

                    # Reinitialize session for each request
                    session, hostname = initialize_akamai_session(edgerc_path)
                    if not session:
                        continue

                    # Fetch alert details from API
                    alert_details = get_alert_details(session, hostname, account_switch_key, definition_id)

                    if alert_details:
                        print(f"Raw response for {definition_id}: {alert_details}")
                        # Use the 'definition' field instead of 'alerts'
                        if 'definition' in alert_details:
                            alert_row = row.copy()
                            # Flatten the 'definition' dictionary into the row
                            for key, value in alert_details['definition'].items():
                                # Convert lists or complex objects to strings to avoid CSV issues
                                if isinstance(value, (list, dict)):
                                    alert_row[f"definition_{key}"] = str(value)
                                else:
                                    alert_row[f"definition_{key}"] = value
                            filtered_alerts.append(alert_row)
                            print(f"--> Alert definition added for ID: {definition_id}")
                        else:
                            print(f"--> No definition found in response for ID: {definition_id}")
                    else:
                        print(f"--> Failed to fetch alert details for ID: {definition_id}")

                    time.sleep(1)  # Avoid rate limiting

        # Write all filtered alerts to the output CSV
        if filtered_alerts:
            write_filtered_alerts_to_csv(filtered_alerts, output_filename)
        else:
            print("No alerts processed.")

    except FileNotFoundError:
        print(f"❌ Error: File '{input_filename}' not found.")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")

if __name__ == "__main__":
    edgerc_path = "/Users/afrolov/.edgerc"  # Replace with your .edgerc path
    # account_switch_key      # Define account_switch_key here

    read_and_process_alerts(edgerc_path, account_switch_key = "F-AC-997250")
