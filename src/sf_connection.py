"""
Shared Salesforce Connection Module - Python

Centralizes all Salesforce authentication and API call logic.

Dependencies:
    pip install simple-salesforce python-dotenv requests
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from simple_salesforce import Salesforce
    SF_AVAILABLE = True
except ImportError:
    SF_AVAILABLE = False

try:
    import requests as http_requests
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False


class SFConnection:
    TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"

    def __init__(self):
        self.instance_url = os.getenv("SF_INSTANCE_URL", "")
        self.client_id = os.getenv("SF_CLIENT_ID", "")
        self.client_secret = os.getenv("SF_CLIENT_SECRET", "")
        self.refresh_token = os.getenv("SF_REFRESH_TOKEN", "")
        self.access_token = os.getenv("SF_ACCESS_TOKEN", "")
        self.api_version = os.getenv("SF_API_VERSION", "v60.0")
        self.connection = None
        self.auth_method = "none"

    def init(self):
        if not SF_AVAILABLE:
            if self.client_id or self.access_token:
                print("[SF Connection] simple-salesforce not installed. pip install simple-salesforce")
            print("[SF Connection] Running in DRY RUN mode (no SF library).")
            self.connection = None
            self.auth_method = "none"
            return None

        if self.client_id and self.refresh_token and HTTP_AVAILABLE:
            try:
                token_data = self._do_token_refresh()
                if token_data:
                    self.connection = Salesforce(
                        instance_url=token_data["instance_url"],
                        session_id=token_data["access_token"],
                    )
                    self.connection.query("SELECT COUNT() FROM Contact")
                    self.instance_url = token_data["instance_url"]
                    self.auth_method = "oauth2"
                    print("[SF Connection] Connected (OAuth2 refresh token)")
                    print(f"[SF Connection] Instance: {self.instance_url}")
                    return self.connection
            except Exception as err:
                print(f"[SF Connection] OAuth2 error: {err}")

        if self.access_token and self.instance_url:
            try:
                self.connection = Salesforce(
                    instance_url=self.instance_url,
                    session_id=self.access_token,
                )
                self.connection.query("SELECT COUNT() FROM Contact")
                self.auth_method = "direct"
                print("[SF Connection] Connected (direct access token)")
                print(f"[SF Connection] Instance: {self.instance_url}")
                return self.connection
            except Exception as err:
                print(f"[SF Connection] Direct token failed: {err}")

        print("[SF Connection] No valid credentials. DRY RUN mode.")
        self.connection = None
        self.auth_method = "none"
        return None

    def _do_token_refresh(self):
        if not HTTP_AVAILABLE:
            return None
        resp = http_requests.post(self.TOKEN_URL, data={
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        })
        if resp.status_code == 200:
            return resp.json()
        print(f"[SF Connection] Token refresh failed ({resp.status_code}): {resp.text}")
        return None

    def refresh(self):
        if self.auth_method != "oauth2" or not self.connection:
            return False
        try:
            token_data = self._do_token_refresh()
            if token_data:
                self.connection = Salesforce(
                    instance_url=token_data["instance_url"],
                    session_id=token_data["access_token"],
                )
                self.instance_url = token_data["instance_url"]
                print("[SF Connection] Token refreshed.")
                return True
            return False
        except Exception as err:
            print(f"[SF Connection] Refresh failed: {err}")
            return False

    def api_call(self, fn):
        if not self.connection:
            return None
        try:
            return fn(self.connection)
        except Exception as err:
            if "INVALID_SESSION_ID" in str(err):
                print("[SF Connection] Session expired, refreshing...")
                if self.refresh():
                    return fn(self.connection)
            raise

    def health_check(self):
        if not self.connection:
            return False
        try:
            self.api_call(lambda conn: conn.query("SELECT COUNT() FROM Account"))
            return True
        except Exception as err:
            print(f"[SF Connection] Health check failed: {err}")
            return False

    @property
    def is_connected(self):
        return self.connection is not None

    def get_instance_url(self):
        return self.instance_url
