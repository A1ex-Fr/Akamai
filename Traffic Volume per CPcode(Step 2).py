import requests
import csv
import json
import statistics
from akamai.edgegrid import EdgeGridAuth, EdgeRc
from urllib.parse import urljoin, urlencode
from datetime import datetime, timedelta, UTC

def initialize_akamai_session(edgerc_path):
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

def get_traffic_report(session, hostname, cpcode, account_switch_key=None, include_filters=True):
    try:
        path = "/reporting-api/v1/reports/hits-by-cpcode/versions/1/report-data"
        url = urljoin(f'https://{hostname}', path)

        today = datetime.now(UTC)
        start_date = (today - timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = (today - timedelta(days=4)).replace(hour=23, minute=0, second=0, microsecond=0)

        start = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        params = {
            "start": start,
            "end": end,
            "objectIds": cpcode,
            "metrics": "edgeHits,hitsOffload"
        }

        if include_filters:
            params["filters"] = "delivery_type=secure,delivery_type=non_secure,ip_version=ipv4,ip_version=ipv6"

        if account_switch_key:
            params["accountSwitchKey"] = account_switch_key

        full_url = f"{url}?{urlencode(params, doseq=True)}"
        print(f"\nRequesting URL: {full_url}")

        response = session.get(full_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Error fetching traffic report: {e}")
        return None

def calculate_traffic_averages(report_data, cpcode):
    edge_hits = []
    hits_offload = []

    if not isinstance(report_data.get('data'), list):
        print(f"Invalid report format for CP code {cpcode}.")
        return None, None

    for entry in report_data['data']:
        try:
            edge_hits.append(float(entry['edgeHits']))
            hits_offload.append(float(entry['hitsOffload']))
        except (ValueError, KeyError):
            continue

    if edge_hits and hits_offload:
        avg_edge = statistics.mean(edge_hits)
        avg_offload = statistics.mean(hits_offload)
        return avg_edge, avg_offload
    else:
        return None, None

def read_cp_codes_from_csv(csv_path):
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            cp_codes = [row['cpcodeId'].strip() for row in reader if row.get('cpcodeId')]
            return cp_codes
    except Exception as e:
        print(f"Error reading CPcodes.csv: {e}")
        return []

def write_results_to_csv(results, output_path):
    try:
        with open(output_path, 'w', newline='') as csvfile:
            fieldnames = ['CPcode', 'Average_edgeHits', 'Average_hitsOffload', 'ReportDataFound']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        print(f"\nResults written to {output_path}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

def fetch_and_analyze_traffic_report(edgerc_path, csv_path, account_switch_key=None):
    session, hostname = initialize_akamai_session(edgerc_path)
    if not session:
        return

    cpcode_list = read_cp_codes_from_csv(csv_path)
    results = []

    for cpcode in cpcode_list:
        print(f"\nFetching report for CP code: {cpcode}")
        report_data = get_traffic_report(session, hostname, cpcode, account_switch_key, include_filters=True)

        if not report_data or not report_data.get('data'):
            print(f"No valid data found for CP code {cpcode} with filters. Retrying without filters...")
            report_data = get_traffic_report(session, hostname, cpcode, account_switch_key, include_filters=False)

        if report_data and report_data.get('data'):
            avg_edge, avg_offload = calculate_traffic_averages(report_data, cpcode)
            if avg_edge is not None and avg_offload is not None:
                print(f"✅ Averages for CP code {cpcode}: edgeHits={avg_edge:.2f}, hitsOffload={avg_offload:.2f}")
                results.append({
                    'CPcode': cpcode,
                    'Average_edgeHits': f"{avg_edge:.2f}",
                    'Average_hitsOffload': f"{avg_offload:.2f}",
                    'ReportDataFound': 'Yes'
                })
            else:
                print(f"No valid metrics in report data for CP code {cpcode}.")
                results.append({
                    'CPcode': cpcode,
                    'Average_edgeHits': 'N/A',
                    'Average_hitsOffload': 'N/A',
                    'ReportDataFound': 'No'
                })
        else:
            print(f"❌ Report data not found for {cpcode}.")
            results.append({
                'CPcode': cpcode,
                'Average_edgeHits': 'N/A',
                'Average_hitsOffload': 'N/A',
                'ReportDataFound': 'No'
            })

    if results:
        write_results_to_csv(results, 'Alerts/Traffic_by_CPcode.csv')

if __name__ == "__main__":
    edgerc_path = "/Users/afrolov/.edgerc"        # Your .edgerc path
    csv_path = "Alerts/CPcodes.csv"  # Path to your CPcodes.csv file
    account_switch_key = "F-AC-997250"            # Optional account switch key

    fetch_and_analyze_traffic_report(edgerc_path, csv_path, account_switch_key)
