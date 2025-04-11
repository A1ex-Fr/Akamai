import requests
import pandas as pd
from akamai.edgegrid import EdgeGridAuth, EdgeRc
from urllib.parse import urljoin
import logging
from typing import Dict, Optional, Any, List
import json

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("akamai_api")

def initialize_akamai_session(edgerc_path: str, section: str = 'default'):
    """
    Set up the authenticated session using EdgeGrid authentication.
    """
    try:
        edgerc = EdgeRc(edgerc_path)
        hostname = edgerc.get(section, 'host')
        client_token = edgerc.get(section, 'client_token')
        client_secret = edgerc.get(section, 'client_secret')
        access_token = edgerc.get(section, 'access_token')
        session = requests.Session()
        session.auth = EdgeGridAuth(client_token, client_secret, access_token)
        session.headers.update({"accept": "application/json"})
        logger.info(f"Successfully initialized Akamai client with host: {hostname}")
        return session, hostname
    except Exception as e:
        logger.error(f"Failed to initialize Akamai client: {str(e)}")
        raise

def read_only_request(session: requests.Session, hostname: str, path: str,
                        params: Optional[Dict[str, Any]] = None) -> requests.Response:
    """
    Make a read-only GET request to the Akamai API with proper authentication.
    """
    if not session:
        raise ValueError("Session not initialized. Check your credentials and .edgerc file.")
    url = urljoin(f'https://{hostname}', path)
    try:
        logger.info(f"Making GET request to {url}")
        response = session.get(url, params=params)
        response.raise_for_status()  # Ensure we raise an error for bad status codes
        logger.debug(f"Response received: {response.text}")
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        logger.debug(f"Response content: {response.text}")  # Debug the response content
        raise

def get_alerts(session: requests.Session, hostname: str, account_switch_key: str) -> Dict[str, Any]:
    """Retrieve alerts from the Alerts API."""
    path = "/alerts/v2/alert-summaries"
    params = {"accountSwitchKey": account_switch_key}
    return read_only_request(session, hostname, path, params).json()

def test_read_api(session: requests.Session, hostname: str, switch_key: str):
    """
    Run test API reads: fetch alert summaries and alert definitions,
    write to CSV, and print to console.
    """
    # Fetch alert summaries
    logger.info("Fetching alert summaries from /alerts/v2/alert-summaries...")
    try:
        summaries_url = f"https://{hostname}/alerts/v2/alert-summaries"
        params = {"accountSwitchKey": switch_key}
        response = session.get(summaries_url, params=params)

        # Log status code and response
        logger.debug(f"Status code: {response.status_code}")
        logger.debug(f"Response text: {response.text}")

        response.raise_for_status()
        summaries_json = response.json()

        # If data exists, process and print them
        alert_data = summaries_json.get("data", [])
        if alert_data:
            print(f"Found {len(alert_data)} alerts.")
            alerts_df = pd.DataFrame(alert_data)
            alerts_df.fillna("-").to_csv(f"{switch_key}_alerts.csv", index=False)
            print("Alerts successfully written to alerts.csv")
        else:
            print("No alerts data found.")
            with open("../../../alerts.csv", "w", encoding='utf-8') as f:
                f.write("No alerts data available.\n")
    except Exception as e:
        logger.error(f"Failed to fetch alert summaries: {str(e)}")
        with open("../../../alerts.csv", "w", encoding='utf-8') as f:
            f.write(f"Error fetching alert summaries: {str(e)}\n")

if __name__ == "__main__":
    edgerc_path = "/Users/afrolov/.edgerc"  # Update if needed
    session, hostname = initialize_akamai_session(edgerc_path)
    print("\nEXECUTING READ-ONLY OPERATIONS - NO CHANGES WILL BE MADE TO AKAMAI CONFIGURATION\n")
    test_read_api(session, hostname, switch_key="F-AC-997250")
