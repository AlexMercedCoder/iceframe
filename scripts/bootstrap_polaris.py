"""
Create an IceFrame test catalog in a fresh Apache Polaris server.

Used by the catalog-compatibility tests (``tests/test_catalog_compat.py``) and
the ``catalogs`` CI job. Polaris must run with FILE storage allowed; see
``ci/catalogs-compose.yml``.

    python scripts/bootstrap_polaris.py http://localhost:18182 file:///tmp/wh/polaris
"""

import json
import sys
import time
import urllib.parse
import urllib.request

CATALOG = "iceframe"


def _request(method, url, token=None, body=None, form=None):
    headers = {}
    data = None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = resp.read()
        return json.loads(payload) if payload else None


def bootstrap(base: str, location: str, client: str = "root", secret: str = "s3cr3t") -> None:
    deadline = time.time() + 120
    while True:
        try:
            token = _request(
                "POST",
                f"{base}/api/catalog/v1/oauth/tokens",
                form={
                    "grant_type": "client_credentials",
                    "client_id": client,
                    "client_secret": secret,
                    "scope": "PRINCIPAL_ROLE:ALL",
                },
            )["access_token"]
            break
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(2)

    mgmt = f"{base}/api/management/v1"
    _request(
        "POST",
        f"{mgmt}/catalogs",
        token,
        {
            "catalog": {
                "name": CATALOG,
                "type": "INTERNAL",
                "properties": {"default-base-location": location},
                "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [location]},
            }
        },
    )
    _request(
        "PUT",
        f"{mgmt}/catalogs/{CATALOG}/catalog-roles/catalog_admin/grants",
        token,
        {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
    )
    _request(
        "PUT",
        f"{mgmt}/principal-roles/service_admin/catalog-roles/{CATALOG}",
        token,
        {"catalogRole": {"name": "catalog_admin"}},
    )
    print(f"Polaris catalog '{CATALOG}' ready at {location}")


if __name__ == "__main__":
    bootstrap(sys.argv[1], sys.argv[2])
