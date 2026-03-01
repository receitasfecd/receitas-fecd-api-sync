import os
import msal
import requests
import logging
from urllib.parse import quote

class OneDriveClient:
    def __init__(self):
        self.client_id = os.getenv("ONEDRIVE_CLIENT_ID")
        self.tenant_id = os.getenv("ONEDRIVE_TENANT_ID")
        self.client_secret = os.getenv("ONEDRIVE_CLIENT_SECRET")
        self.user_id = os.getenv("ONEDRIVE_USER_ID")
        self.remote_root = os.getenv("ONEDRIVE_REMOTE_ROOT", "NotasFiscais")
        self.access_token = None

    def _get_token(self):
        if not all([self.client_id, self.tenant_id, self.client_secret]):
            return None
        
        try:
            authority = f"https://login.microsoftonline.com/{self.tenant_id}"
            app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=authority,
                client_credential=self.client_secret
            )
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            return result.get("access_token")
        except:
            return None

    def upload_file(self, content, filename, subfolder=""):
        token = self._get_token()
        if not token: return False

        # Ensure folder exists
        self.ensure_folder(subfolder)

        # Path: root/subfolder/filename
        remote_path = f"{self.remote_root}/{subfolder}/{filename}" if subfolder else f"{self.remote_root}/{filename}"
        safe_path = quote(remote_path)
        
        url = f"https://graph.microsoft.com/v1.0/users/{self.user_id}/drive/root:/{safe_path}:/content"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }

        try:
            resp = requests.put(url, headers=headers, data=content, timeout=60)
            return resp.status_code in [200, 201]
        except:
            return False

    def ensure_folder(self, subfolder):
        if not subfolder: return True
        token = self._get_token()
        if not token: return False

        parts = [p for p in subfolder.split('/') if p]
        current = self.remote_root

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        for part in parts:
            safe_current = quote(current)
            url = f"https://graph.microsoft.com/v1.0/users/{self.user_id}/drive/root:/{safe_current}:/children"
            try:
                # Check if exists
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    children = resp.json().get('value', [])
                    exists = any(c['name'] == part for c in children)
                    if not exists:
                        # Create
                        requests.post(url, headers=headers, json={"name": part, "folder": {}})
                current = f"{current}/{part}"
            except:
                return False
        return True

    def get_file_link(self, filename, subfolder=""):
        token = self._get_token()
        if not token: return None
        remote_path = f"{self.remote_root}/{subfolder}/{filename}" if subfolder else f"{self.remote_root}/{filename}"
        safe_path = quote(remote_path)
        url = f"https://graph.microsoft.com/v1.0/users/{self.user_id}/drive/root:/{safe_path}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.json().get("webUrl")
        except:
            pass
        return None

onedrive = OneDriveClient()
