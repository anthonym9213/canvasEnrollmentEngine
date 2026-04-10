"""
canvas_uploader.py
==================
Uploads a built SIS ZIP to Canvas and (optionally) polls the import until
it reaches a terminal state.

Kept as a separate module from engine.py on purpose:
    * engine.py is deterministic file assembly — safe to re-run, no network.
    * canvas_uploader.py is the side-effecting step — can be disabled,
      dry-run'd, or run from a different host with the ZIP as input.

Canvas SIS Import API:
    POST {base}/api/v1/accounts/:id/sis_imports
      ?import_type=instructure_csv
      &extension=zip
    Body: raw ZIP bytes
    Header: Authorization: Bearer <token>

    GET  {base}/api/v1/accounts/:id/sis_imports/:id
    -> workflow_state in:
         created, importing, cleanup_batch, imported,
         imported_with_messages, aborted, failed, failed_with_messages
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

log = logging.getLogger(__name__)

TERMINAL_STATES = {
    "imported",
    "imported_with_messages",
    "failed",
    "failed_with_messages",
    "aborted",
}
SUCCESS_STATES = {"imported", "imported_with_messages"}


@dataclass
class UploadResult:
    sis_import_id: Optional[int]
    workflow_state: str
    progress: int
    raw: dict

    @property
    def ok(self) -> bool:
        return self.workflow_state in SUCCESS_STATES


class CanvasUploader:
    def __init__(self, config: dict):
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required for canvas_uploader. "
                "Install with: pip install requests"
            )
        env_name = config["TARGET_ENV"]
        env_cfg = config["environments"][env_name]
        self.base_url = env_cfg["canvas_base_url"].rstrip("/")
        self.endpoint = env_cfg["sis_import_endpoint"]
        token_var = env_cfg["api_token_env_var"]
        token = os.environ.get(token_var, "")
        if not token:
            raise RuntimeError(
                f"Canvas API token not set. Export ${token_var} before running."
            )
        self.headers = {"Authorization": f"Bearer {token}"}
        self.env = env_name

    # ------------------------------------------------------------------ api
    def upload(self, zip_path: str, dry_run: bool = False) -> UploadResult:
        url = f"{self.base_url}{self.endpoint}"
        params = {"import_type": "instructure_csv", "extension": "zip"}

        if dry_run:
            log.info(
                "[dry-run] would POST %s bytes to %s",
                os.path.getsize(zip_path), url,
            )
            return UploadResult(None, "dry_run", 0, {})

        log.info("Uploading %s to %s (env=%s)", zip_path, url, self.env)
        with open(zip_path, "rb") as fh:
            resp = requests.post(
                url,
                params=params,
                headers={**self.headers, "Content-Type": "application/zip"},
                data=fh.read(),
                timeout=120,
            )
        resp.raise_for_status()
        data = resp.json()
        result = UploadResult(
            sis_import_id=data.get("id"),
            workflow_state=data.get("workflow_state", "unknown"),
            progress=int(data.get("progress", 0) or 0),
            raw=data,
        )
        log.info(
            "Upload accepted: sis_import_id=%s workflow_state=%s",
            result.sis_import_id, result.workflow_state,
        )
        return result

    def poll(
        self,
        sis_import_id: int,
        interval_seconds: int = 15,
        timeout_seconds: int = 1800,
    ) -> UploadResult:
        """Block until the import reaches a terminal state or times out."""
        url = f"{self.base_url}{self.endpoint}/{sis_import_id}"
        deadline = time.monotonic() + timeout_seconds
        last: UploadResult | None = None

        while time.monotonic() < deadline:
            resp = requests.get(url, headers=self.headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            last = UploadResult(
                sis_import_id=data.get("id"),
                workflow_state=data.get("workflow_state", "unknown"),
                progress=int(data.get("progress", 0) or 0),
                raw=data,
            )
            log.info(
                "  poll: state=%s progress=%d%%",
                last.workflow_state, last.progress,
            )
            if last.workflow_state in TERMINAL_STATES:
                return last
            time.sleep(interval_seconds)

        raise TimeoutError(
            f"SIS import {sis_import_id} did not finish within "
            f"{timeout_seconds}s (last state: "
            f"{last.workflow_state if last else 'unknown'})"
        )
