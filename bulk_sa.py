"""
loaders/bulk_sa.py
==================
Bulk Student/Admin enrollment feed — the big one. The SIS dumps a single
CSV with user + course + section + role per row, overwritten in place each
update.

Expected columns (case-insensitive via DictReader):
    eid, course_sis_id, section_sis_id, role

Role codes: S = student, I/T = teacher, A = ta, O = observer
"""

from __future__ import annotations

from .base import EnrollmentFeedLoader

ROLE_MAP = {
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
            "role": ROLE_MAP.get(raw_role, "student"),
            "status": "active",
        }
