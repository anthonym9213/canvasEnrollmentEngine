"""
canvas_api.py
=============
Thin wrapper around Canvas API lookup operations. Separate from
canvas_uploader.py because uploading a SIS ZIP is an async write and
this is sync reads.

Currently supports:
    * course_exists(sis_course_id) -> bool

The engine uses this before running a cohort loader to make sure the
target course is actually there. If it isn't, the loader is skipped and
an admin notification is sent — the engine will not create courses
automatically because course setup (syllabus, modules, instructor
assignment) is a human decision.

Design notes
------------
* Canvas lets you address a course by SIS ID using the `sis_course_id:`
  prefix on the /api/v1/courses/:id endpoint. A 404 means it doesn't
  exist; any other error gets raised.
* No caching — course-exists checks happen once per cohort loader per
  run, which is cheap.
* `requests` is imported lazily so the rest of the engine works without
  it installed, same pattern as canvas_uploader.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import quote

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

log = logging.getLogger(__name__)


class CanvasAPIError(RuntimeError):
    pass


class CanvasAPI:
    def __init__(self, config: dict):
        if requests is None:
            raise CanvasAPIError(
                "The 'requests' package is required. Install with: pip install requests"
            )
        env_name = config["TARGET_ENV"]
        env_cfg = config["environments"][env_name]
        self.base_url = env_cfg["canvas_base_url"].rstrip("/")
        token_var = env_cfg["api_token_env_var"]
        token = os.environ.get(token_var, "")
        if not token:
            raise CanvasAPIError(
                f"Canvas API token not set. Export ${token_var} before running."
            )
        self.headers = {"Authorization": f"Bearer {token}"}
        self.env = env_name

    def course_exists(self, sis_course_id: str) -> bool:
        """
        Return True if a course with this SIS ID exists in Canvas.

        Canvas address-by-SIS syntax: /api/v1/courses/sis_course_id:<id>
        The ID must be URL-encoded because it often contains characters
        like underscores or colons that are otherwise fine but the
        encoding is cheap insurance.
        """
        encoded = quote(sis_course_id, safe="")
        url = f"{self.base_url}/api/v1/courses/sis_course_id:{encoded}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=30)
        except requests.RequestException as e:
            raise CanvasAPIError(f"Network error checking course {sis_course_id}: {e}") from e

        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            return False
        raise CanvasAPIError(
            f"Unexpected {resp.status_code} from Canvas checking {sis_course_id}: "
            f"{resp.text[:200]}"
        )
