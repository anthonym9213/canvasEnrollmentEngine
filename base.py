"""
loaders/base.py
===============
Three loader categories, each with a distinct real-world contract.

EnrollmentFeedLoader
    Reads a file where each row already specifies user + course + role.
    Normal grace-period semantics apply: stop appearing in the feed and
    after N days you're marked for deletion. Used by bulk_sa, nrd.

RosterLoader
    Reads a file that's just a list of eIDs, and has a configured target
    course and role. Synthesizes enrollments in that target course for
    every eID. Normal grace-period semantics apply. Used by trn, fyr.

CohortLoader
    Same input shape as RosterLoader (list of eIDs + target course) but
    with capacity-balanced random section assignment and strictly
    join-only semantics (cohort_locked=True). Used by online_training,
    wble.

Every loader subclass shares the same file-resolution plumbing:
    source_dir + file_pattern -> newest matching file
    max_age_hours             -> staleness guard, logs a warning / alerts
"""

from __future__ import annotations

import glob
import hashlib
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Iterator

log = logging.getLogger(__name__)


# =========================================================================
# File resolution shared by all loaders
# =========================================================================
class FileResolver:
    """
    Given source_dir + file_pattern, pick the newest matching file.
    Emits warnings for missing or stale files without raising, so a single
    dead feed never crashes the whole run.
    """

    @staticmethod
    def resolve(
        source_dir: str,
        file_pattern: str,
        loader_name: str,
        max_age_hours: float | None = None,
    ) -> str | None:
        if not os.path.isdir(source_dir):
            log.warning(
                "[%s] source_dir does not exist: %s — skipping loader.",
                loader_name, source_dir,
            )
            return None

        candidates = glob.glob(os.path.join(source_dir, file_pattern))
        if not candidates:
            log.warning(
                "[%s] no files matching %s in %s — skipping loader.",
                loader_name, file_pattern, source_dir,
            )
            return None

        newest = max(candidates, key=lambda p: os.path.getmtime(p))

        if max_age_hours is not None:
            age_hours = (time.time() - os.path.getmtime(newest)) / 3600
            if age_hours > max_age_hours:
                log.warning(
                    "[%s] newest file %s is %.1f hours old "
                    "(max_age_hours=%.1f) — using it anyway but flagging.",
                    loader_name, os.path.basename(newest), age_hours, max_age_hours,
                )

        log.info("[%s] using file: %s", loader_name, newest)
        return newest


# =========================================================================
# Abstract base
# =========================================================================
class BaseLoader(ABC):
    """Every loader inherits from this. Subclasses pick one of the three
    category bases below, not this one directly."""

    #: category name stored in enrollments.source column
    category: str = "base"

    def __init__(self, config: dict, entry: dict):
        self.config = config
        self.entry = entry
        self.name: str = entry["name"]
        self.source_dir: str = entry.get("source_dir", "")
        self.file_pattern: str = entry.get("file_pattern", "")
        self.max_age_hours: float | None = entry.get("max_age_hours")

    def resolve_input_file(self) -> str | None:
        return FileResolver.resolve(
            self.source_dir, self.file_pattern,
            loader_name=self.name,
            max_age_hours=self.max_age_hours,
        )

    @abstractmethod
    def load(self, state_manager) -> Iterable[dict]:
        """
        Yield normalized enrollment dicts:
            {user_id, course_id, section_id, role, status='active'}

        state_manager is passed in because cohort loaders need to query
        current section sizes to balance assignments. Other loader types
        typically ignore it.
        """
        raise NotImplementedError

    @property
    def cohort_locked(self) -> bool:
        """Override to True in CohortLoader."""
        return False


# =========================================================================
# Category 1: enrollment feed (user + course + role per row)
# =========================================================================
class EnrollmentFeedLoader(BaseLoader):
    """
    Feed with pre-resolved enrollment rows. Subclasses override `parse_row`
    to handle their specific column names and role encoding.
    """

    category = "enrollment_feed"

    def load(self, state_manager) -> Iterable[dict]:
        import csv
        path = self.resolve_input_file()
        if path is None:
            return
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                parsed = self.parse_row(row)
                if parsed is not None:
                    yield parsed

    @abstractmethod
    def parse_row(self, row: dict) -> dict | None:
        """Return a normalized enrollment dict or None to skip."""
        raise NotImplementedError


# =========================================================================
# Category 2: roster (list of eIDs -> configured target course)
# =========================================================================
class RosterLoader(BaseLoader):
    """
    Reads a file that is just a list of eIDs (one column). The target
    course and role come from the loader's config entry. Useful for
    TRN and FYR where the file is just "who's eligible" and the course
    is implied by context.

    Config entry fields:
        target_course    : str, required. SIS course_id.
        role             : str, default 'student'.
        eid_column       : str, default 'eid'. Column name in the file.
    """

    category = "roster"

    def load(self, state_manager) -> Iterable[dict]:
        import csv
        target_course = self.entry.get("target_course")
        if not target_course:
            log.error("[%s] no target_course configured — skipping.", self.name)
            return

        role = self.entry.get("role", "student").strip().lower()
        eid_column = self.entry.get("eid_column", "eid")

        path = self.resolve_input_file()
        if path is None:
            return

        count = 0
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if eid_column not in (reader.fieldnames or []):
                log.error(
                    "[%s] eid_column '%s' not in file columns %s",
                    self.name, eid_column, reader.fieldnames,
                )
                return
            for row in reader:
                eid = (row.get(eid_column) or "").strip()
                if not eid:
                    continue
                yield {
                    "user_id": eid,
                    "course_id": target_course,
                    "section_id": "",  # roster loaders don't assign sections
                    "role": role,
                    "status": "active",
                }
                count += 1
        log.info("[%s] emitted %d roster enrollments into %s", self.name, count, target_course)


# =========================================================================
# Category 3: cohort (join-only, capacity-balanced section assignment)
# =========================================================================
class CohortLoader(BaseLoader):
    """
    Sticks users to a cohort course for the long haul.

    Behavior:
        - For each eID in the file, if they're already enrolled in the
          target course (from any prior run), do nothing.
        - Otherwise, pick the smallest existing section in the course.
          If all existing sections are at or above `section_cap`, or if
          no sections exist yet, create a new section.
        - Ties among equal-sized sections are broken by hash(eid) so the
          assignment is deterministic and reproducible.
        - Record the new enrollment with cohort_locked=True so the engine
          never expires it through grace-period logic.

    Config entry fields:
        target_course           : str, required. SIS course_id.
        section_sis_id_template : str, e.g. "OST-{year}-SEC{n:02d}"
        section_name_template   : str, e.g. "Online Student Training {year} Section {n}"
        year                    : int, passed into templates (semester rollover key)
        initial_sections        : int, how many sections to pre-create on first touch
        section_cap             : int, max enrollments per section before creating new
        role                    : str, default 'student'
        eid_column              : str, default 'eid'

    Section assignment algorithm
    ----------------------------
    1. Load current section sizes for this course from state_manager.
    2. If no sections exist yet, synthesize the first `initial_sections`
       placeholder entries (size 0) so the balancer has targets.
    3. For each new eID (in a stable sorted order for reproducibility):
         a. Find sections with room (size < section_cap).
         b. Among those, find the minimum size.
         c. Tie-break with hash(eid) % (number of tied sections).
         d. If no sections have room, create a new section and place there.
       Update the running size map as we go so balancing stays correct
       within a single run.
    """

    category = "cohort"

    @property
    def cohort_locked(self) -> bool:
        return True

    def load(self, state_manager) -> Iterable[dict]:
        import csv

        target_course = self.entry.get("target_course")
        if not target_course:
            log.error("[%s] no target_course — skipping.", self.name)
            return

        role = self.entry.get("role", "student").strip().lower()
        eid_column = self.entry.get("eid_column", "eid")
        section_cap = int(self.entry.get("section_cap", 350))
        initial_sections = int(self.entry.get("initial_sections", 1))
        sid_template = self.entry["section_sis_id_template"]
        name_template = self.entry.get(
            "section_name_template", "{course} Section {n}"
        )
        year = self.entry.get("year", "")

        path = self.resolve_input_file()
        if path is None:
            return

        # read eids up front so we can sort for stable assignment
        eids: list[str] = []
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if eid_column not in (reader.fieldnames or []):
                log.error(
                    "[%s] eid_column '%s' not in file columns %s",
                    self.name, eid_column, reader.fieldnames,
                )
                return
            for row in reader:
                eid = (row.get(eid_column) or "").strip()
                if eid:
                    eids.append(eid)
        eids.sort()  # stable input order

        # Current state of sections in this course (from prior runs)
        current_sizes = state_manager.get_section_sizes(target_course)

        # First touch of a cohort course: seed the initial sections
        if not current_sizes:
            log.info(
                "[%s] first touch of course %s — creating %d initial sections.",
                self.name, target_course, initial_sections,
            )
            for i in range(1, initial_sections + 1):
                sid = sid_template.format(year=year, n=i)
                sname = name_template.format(course=target_course, year=year, n=i)
                state_manager.record_section_creation(
                    course_id=target_course,
                    section_id=sid,
                    name=sname,
                    created_by=self.name,
                )
                current_sizes[sid] = 0

        # Track what the "next n" is for any new sections we create mid-run
        existing_numbers = self._extract_section_numbers(current_sizes.keys(), sid_template, year)
        next_n = (max(existing_numbers) + 1) if existing_numbers else 1

        placed = 0
        reused = 0
        for eid in eids:
            # already placed? leave them alone
            existing_section = state_manager.user_has_enrollment_in_course(eid, target_course)
            if existing_section is not None:
                reused += 1
                continue

            # pick a section with room
            sections_with_room = {
                sid: n for sid, n in current_sizes.items() if n < section_cap
            }
            if sections_with_room:
                min_size = min(sections_with_room.values())
                candidates = sorted(
                    sid for sid, n in sections_with_room.items() if n == min_size
                )
                # deterministic tie-break
                idx = int(hashlib.sha1(eid.encode("utf-8")).hexdigest(), 16) % len(candidates)
                chosen = candidates[idx]
            else:
                # every section is at cap — create a new one
                chosen = sid_template.format(year=year, n=next_n)
                chosen_name = name_template.format(course=target_course, year=year, n=next_n)
                state_manager.record_section_creation(
                    course_id=target_course,
                    section_id=chosen,
                    name=chosen_name,
                    created_by=self.name,
                )
                log.info(
                    "[%s] all sections at cap (%d) — created overflow section %s",
                    self.name, section_cap, chosen,
                )
                current_sizes[chosen] = 0
                next_n += 1

            current_sizes[chosen] += 1
            placed += 1
            yield {
                "user_id": eid,
                "course_id": target_course,
                "section_id": chosen,
                "role": role,
                "status": "active",
            }

        log.info(
            "[%s] cohort run: placed=%d already_enrolled=%d total_eids=%d",
            self.name, placed, reused, len(eids),
        )

    @staticmethod
    def _extract_section_numbers(section_ids: Iterable[str], template: str, year) -> list[int]:
        """
        Parse back the numeric {n} value from previously-created section IDs.
        Only used to decide the next integer when we need to spill over.

        We can't just substitute a sentinel string into the template because
        templates like "OST-{year}-SEC{n:02d}" include format specs that
        reject non-numeric values. Instead:
            1. Strip the {n[:spec]} placeholder from the template to get a
               prefix and suffix pattern.
            2. Render the template once for year only (leaving {n} intact)
               so fixed parts (year) get their real values.
            3. Split on the {n} marker to get true prefix/suffix.
            4. Match each section_id and extract the numeric middle.
        """
        import re

        # Replace {n} or {n:spec} with a literal marker we can split on.
        marker = "\x00N\x00"
        try:
            # First substitute {n...} with the marker using a regex, then
            # format the rest of the template (which now has no {n}).
            stripped = re.sub(r"\{n(?::[^}]*)?\}", marker, template)
            rendered = stripped.format(year=year)
        except Exception:
            return []

        if marker not in rendered:
            return []
        prefix, _, suffix = rendered.partition(marker)

        numbers: list[int] = []
        for sid in section_ids:
            if not (sid.startswith(prefix) and sid.endswith(suffix)):
                continue
            middle = sid[len(prefix): len(sid) - len(suffix)] if suffix else sid[len(prefix):]
            try:
                numbers.append(int(middle))
            except ValueError:
                pass
        return numbers
