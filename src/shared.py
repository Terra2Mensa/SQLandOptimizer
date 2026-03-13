"""
Shared utility functions for Terra Mensa valuation engine.
"""

import base64
import json
import urllib.request

from config import DATAMART_BASE_URL, MARS_BASE_URL


def parse_number(s) -> float:
    if s is None or str(s) in ('', 'None', '.00'):
        return 0.0
    return float(str(s).replace(',', ''))


def fetch_datamart(report_id: int, last_reports: int = 1, all_sections: bool = True) -> list:
    params = f"lastReports={last_reports}"
    if all_sections:
        params += "&allSections=True"
    url = f"{DATAMART_BASE_URL}/{report_id}?{params}"
    req = urllib.request.Request(url)
    req.add_header('Accept', 'application/json')
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read().decode())


def fetch_mars(report_id: int, api_key: str, last_reports: int = 1) -> dict:
    url = f"{MARS_BASE_URL}/{report_id}?lastReports={last_reports}"
    req = urllib.request.Request(url)
    req.add_header('Accept', 'application/json')
    encoded = base64.b64encode(f":{api_key}".encode()).decode()
    req.add_header('Authorization', f'Basic {encoded}')
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read().decode())
