"""Google Drive client for Planeorama — upload/download files by name."""

import io
import json
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

_SCOPES = ["https://www.googleapis.com/auth/drive"]


class DriveClient:
    def __init__(self):
        info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = service_account.Credentials.from_service_account_info(info, scopes=_SCOPES)
        self._svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        self._folder = os.environ["GOOGLE_DRIVE_FOLDER_ID"]
        self._id_cache: dict = {}  # name → file_id, to avoid repeated list calls

    # ── Internal ───────────────────────────────────────────────────────────────

    def _find(self, name: str) -> str | None:
        if name in self._id_cache:
            return self._id_cache[name]
        q = f"name='{name}' and '{self._folder}' in parents and trashed=false"
        res = self._svc.files().list(q=q, fields="files(id)").execute()
        files = res.get("files", [])
        fid = files[0]["id"] if files else None
        if fid:
            self._id_cache[name] = fid
        return fid

    # ── Public ─────────────────────────────────────────────────────────────────

    def download(self, name: str) -> bytes | None:
        """Returns file contents as bytes, or None if the file doesn't exist."""
        fid = self._find(name)
        if not fid:
            return None
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, self._svc.files().get_media(fileId=fid))
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    def upload(self, name: str, content: bytes, mime: str = "text/csv"):
        """Create or replace a file in the Planeorama Drive folder."""
        fid = self._find(name)
        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime, resumable=True)
        if fid:
            self._svc.files().update(fileId=fid, media_body=media).execute()
        else:
            meta = {"name": name, "parents": [self._folder]}
            result = self._svc.files().create(body=meta, media_body=media).execute()
            self._id_cache[name] = result["id"]

    def download_json(self, name: str) -> dict | list | None:
        data = self.download(name)
        return json.loads(data.decode("utf-8")) if data else None

    def upload_json(self, name: str, obj):
        self.upload(name, json.dumps(obj, indent=2).encode("utf-8"), mime="application/json")
