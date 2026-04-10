"""
loaders/manual.py
=================
Faculty-driven enrollment overrides. Reads `faculty_additions.csv` and
treats every row in it as an active enrollment, bypassing the SIS feed
entirely.

File format:
    user_id,course_id,section_id,role,reason,added_by,added_on

Workflow:
    - A professor asks the LMS team to add a student who isn't in the
      registrar's feed (audit student, TA not on the roster, guest, etc).
    - An admin adds a row to faculty_additions.csv.
    - This loader ingests it on the next run, so the student appears as
      active in the SIS ZIP and stays that way as long as the row exists.
    - When the row is removed from the file, the enrollment goes through
      the normal grace period on subsequent runs.

This loader is just an EnrollmentFeedLoader with a simpler row format.
"""

from __future__ import annotations

from .base import EnrollmentFeedLoader


class Loader(EnrollmentFeedLoader):
    def parse_row(self, row: dict) -> dict | None:
        uid = (row.get("user_id") or "").strip()
        cid = (row.get("course_id") or "").strip()
        if not uid or not cid:
            return None
        return {
            "user_id": uid,
            "course_id": cid,
            "section_id": (row.get("section_id") or "").strip(),
            "role": (row.get("role") or "student").strip().lower(),
            "status": "active",
        }
