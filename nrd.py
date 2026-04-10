"""
loaders/nrd.py
==============
Non-Registrar-Driven enrollments. Same shape as bulk_sa but tagged
separately so admins can tell them apart in reports.
"""

from __future__ import annotations

from .base import EnrollmentFeedLoader

_ROLE_MAP = {
    "S": "student",
    "I": "teacher",
    "T": "teacher",
    "A": "ta",
    "O": "observer",
}


class Loader(EnrollmentFeedLoader):
    def parse_row(self, row: dict) -> dict | None:
        eid = (row.get("eid") or "").strip()
        course = (row.get("course_sis_id") or "").strip()
        if not eid or not course:
            return None
        raw_role = (row.get("role") or "S").strip().upper()
        return {
            "user_id": eid,
            "course_id": course,
            "section_id": (row.get("section_sis_id") or "").strip(),
            "role": _ROLE_MAP.get(raw_role, "student"),
            "status": "active",
        }
