"""
state_manager.py
================
SQLite-backed state for the Enrollment Engine.

Schema (v2)
-----------
enrollments(
    user_id        TEXT,
    course_id      TEXT,
    section_id     TEXT,
    role           TEXT,
    status         TEXT,   -- 'active' | 'pending_deletion'
    first_seen     TEXT,
    last_seen      TEXT,
    source         TEXT,   -- high-level category (bulk_sa, fyr, cohort, manual...)
    loader_name    TEXT,   -- specific loader that created it (online_training_2025...)
    cohort_locked  INTEGER,-- 0 or 1; locked rows skip cohort-feed grace expiry
    PRIMARY KEY (user_id, course_id, section_id, role)
)

sections(
    course_id      TEXT,
    section_id     TEXT,
    name           TEXT,
    created_by     TEXT,    -- loader that created it
    created_on     TEXT,
    PRIMARY KEY (course_id, section_id)
)

suppressed_deletions(
    run_ts         TEXT,
    user_id        TEXT,
    course_id      TEXT,
    section_id     TEXT,
    role           TEXT,
    reason         TEXT
)

schema_version(version INTEGER PRIMARY KEY)

Migrations
----------
v1 -> v2 adds loader_name, cohort_locked columns and the sections table.
First run against a v1 DB will auto-migrate in place. Back up your DB
before the first run of the new code.
"""

from __future__ import annotations

import csv
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Iterator

log = logging.getLogger(__name__)

STATUS_ACTIVE = "active"
STATUS_PENDING = "pending_deletion"

SCHEMA_VERSION = 2


@dataclass(frozen=True)
class EnrollmentKey:
    user_id: str
    course_id: str
    section_id: str
    role: str

    def as_tuple(self) -> tuple[str, str, str, str]:
        return (self.user_id, self.course_id, self.section_id, self.role)


@dataclass
class EnrollmentRecord:
    user_id: str
    course_id: str
    section_id: str
    role: str
    status: str
    first_seen: str
    last_seen: str
    source: str
    loader_name: str
    cohort_locked: int

    @property
    def key(self) -> EnrollmentKey:
        return EnrollmentKey(self.user_id, self.course_id, self.section_id, self.role)


@dataclass
class SectionRecord:
    course_id: str
    section_id: str
    name: str
    created_by: str
    created_on: str


class StateManager:
    def __init__(self, db_path: str, grace_period_days: int):
        self.db_path = db_path
        self.grace_period_days = grace_period_days
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    # ------------------------------------------------------------ migrations
    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)"
        )
        row = cur.execute("SELECT version FROM schema_version").fetchone()
        current = row["version"] if row else 0

        if current == 0:
            self._migrate_fresh()
        elif current == 1:
            self._migrate_v1_to_v2()

        self.conn.commit()

    def _migrate_fresh(self) -> None:
        log.info("Initializing fresh schema at v%d", SCHEMA_VERSION)
        self.conn.executescript(
            """
            CREATE TABLE enrollments (
                user_id       TEXT NOT NULL,
                course_id     TEXT NOT NULL,
                section_id    TEXT NOT NULL DEFAULT '',
                role          TEXT NOT NULL,
                status        TEXT NOT NULL,
                first_seen    TEXT NOT NULL,
                last_seen     TEXT NOT NULL,
                source        TEXT NOT NULL,
                loader_name   TEXT NOT NULL DEFAULT '',
                cohort_locked INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, course_id, section_id, role)
            );

            CREATE INDEX idx_enrollments_status     ON enrollments(status);
            CREATE INDEX idx_enrollments_last_seen  ON enrollments(last_seen);
            CREATE INDEX idx_enrollments_course     ON enrollments(course_id);
            CREATE INDEX idx_enrollments_user       ON enrollments(user_id);
            CREATE INDEX idx_enrollments_locked     ON enrollments(cohort_locked);

            CREATE TABLE sections (
                course_id   TEXT NOT NULL,
                section_id  TEXT NOT NULL,
                name        TEXT NOT NULL,
                created_by  TEXT NOT NULL,
                created_on  TEXT NOT NULL,
                PRIMARY KEY (course_id, section_id)
            );

            CREATE TABLE suppressed_deletions (
                run_ts     TEXT NOT NULL,
                user_id    TEXT NOT NULL,
                course_id  TEXT NOT NULL,
                section_id TEXT NOT NULL DEFAULT '',
                role       TEXT NOT NULL,
                reason     TEXT NOT NULL
            );
            """
        )
        self.conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    def _migrate_v1_to_v2(self) -> None:
        log.info("Migrating schema v1 -> v2")
        self.conn.executescript(
            """
            ALTER TABLE enrollments ADD COLUMN loader_name TEXT NOT NULL DEFAULT '';
            ALTER TABLE enrollments ADD COLUMN cohort_locked INTEGER NOT NULL DEFAULT 0;

            CREATE INDEX IF NOT EXISTS idx_enrollments_course ON enrollments(course_id);
            CREATE INDEX IF NOT EXISTS idx_enrollments_user   ON enrollments(user_id);
            CREATE INDEX IF NOT EXISTS idx_enrollments_locked ON enrollments(cohort_locked);

            CREATE TABLE IF NOT EXISTS sections (
                course_id   TEXT NOT NULL,
                section_id  TEXT NOT NULL,
                name        TEXT NOT NULL,
                created_by  TEXT NOT NULL,
                created_on  TEXT NOT NULL,
                PRIMARY KEY (course_id, section_id)
            );
            """
        )
        self.conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))

    # -------------------------------------------------------------- ingestion
    def upsert_seen(
        self,
        rows: Iterable[dict],
        source: str,
        loader_name: str,
        cohort_locked: bool = False,
        run_date: date | None = None,
    ) -> int:
        """
        Mark each enrollment row as seen on `run_date`.

        For non-cohort loaders: inserts new rows with status=active, reactivates
        previously-pending rows, and refreshes last_seen on existing rows.

        For cohort loaders (cohort_locked=True): strictly join-only. If the
        enrollment already exists it's left alone (including its last_seen,
        which we do not refresh — cohort-locked rows ignore grace period
        anyway, and not refreshing avoids masking whether the student is
        still showing up in their cohort feed).
        """
        run_date = run_date or date.today()
        run_iso = run_date.isoformat()
        touched = 0
        locked_int = 1 if cohort_locked else 0

        for row in rows:
            key = EnrollmentKey(
                user_id=str(row["user_id"]).strip(),
                course_id=str(row["course_id"]).strip(),
                section_id=str(row.get("section_id", "") or "").strip(),
                role=str(row["role"]).strip().lower(),
            )
            existing = self._get(key)

            if existing is None:
                self.conn.execute(
                    """INSERT INTO enrollments
                       (user_id, course_id, section_id, role, status,
                        first_seen, last_seen, source, loader_name, cohort_locked)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (*key.as_tuple(), STATUS_ACTIVE, run_iso, run_iso,
                     source, loader_name, locked_int),
                )
                touched += 1
                continue

            if cohort_locked:
                # Join-only: do nothing if they're already placed.
                continue

            if existing.status == STATUS_PENDING:
                log.info(
                    "Reactivating previously-pending enrollment %s/%s/%s",
                    key.user_id, key.course_id, key.role,
                )
            self.conn.execute(
                """UPDATE enrollments
                      SET last_seen = ?, status = ?, source = ?, loader_name = ?
                    WHERE user_id = ? AND course_id = ?
                      AND section_id = ? AND role = ?""",
                (run_iso, STATUS_ACTIVE, source, loader_name, *key.as_tuple()),
            )
            touched += 1

        self.conn.commit()
        return touched

    def _get(self, key: EnrollmentKey) -> EnrollmentRecord | None:
        row = self.conn.execute(
            """SELECT * FROM enrollments
                WHERE user_id = ? AND course_id = ?
                  AND section_id = ? AND role = ?""",
            key.as_tuple(),
        ).fetchone()
        return EnrollmentRecord(**dict(row)) if row else None

    def user_has_enrollment_in_course(self, user_id: str, course_id: str) -> str | None:
        """
        Return the section_id if the user is already placed in this course
        (active or pending), else None. Used by cohort loaders to decide
        whether to assign a new section or leave the student where they are.
        """
        row = self.conn.execute(
            """SELECT section_id FROM enrollments
                WHERE user_id = ? AND course_id = ?
                ORDER BY cohort_locked DESC, (status = 'active') DESC
                LIMIT 1""",
            (user_id, course_id),
        ).fetchone()
        return row["section_id"] if row else None

    # --------------------------------------------------------- grace / expiry
    def apply_grace_period(self, run_date: date | None = None) -> int:
        """
        Flip active rows to pending_deletion when their last_seen is older
        than `grace_period_days`. Cohort-locked rows are explicitly excluded.
        """
        run_date = run_date or date.today()
        cutoff = (run_date - timedelta(days=self.grace_period_days)).isoformat()
        cur = self.conn.execute(
            """UPDATE enrollments
                  SET status = ?
                WHERE status = ? AND last_seen < ? AND cohort_locked = 0""",
            (STATUS_PENDING, STATUS_ACTIVE, cutoff),
        )
        self.conn.commit()
        if cur.rowcount:
            log.info(
                "Grace period expired for %d non-cohort enrollments (last_seen < %s).",
                cur.rowcount, cutoff,
            )
        return cur.rowcount

    def get_pending_deletions(self) -> list[EnrollmentRecord]:
        rows = self.conn.execute(
            "SELECT * FROM enrollments WHERE status = ? ORDER BY user_id, course_id",
            (STATUS_PENDING,),
        ).fetchall()
        return [EnrollmentRecord(**dict(r)) for r in rows]

    def purge_deleted(self, keys: Iterable[EnrollmentKey]) -> int:
        count = 0
        for k in keys:
            self.conn.execute(
                """DELETE FROM enrollments
                    WHERE user_id = ? AND course_id = ?
                      AND section_id = ? AND role = ?""",
                k.as_tuple(),
            )
            count += 1
        self.conn.commit()
        return count

    # ------------------------------------------------------------ admin holds
    @staticmethod
    def load_admin_holds(hold_csv_path: str) -> dict[tuple[str, str], str]:
        """
        Read the three-state admin holds file.

        Expected columns:
            user_id,course_id,status,reason,added_by,added_on

        `status` values:
            (blank) or 'pending_review'    -> suppress deletion
            'approved_for_deletion'        -> allow deletion through on this run

        Returns a dict mapping (user_id, course_id) to the normalized status.
        Rows with an unrecognized status are treated as 'pending_review' and
        a warning is logged — we default to the safer option.
        """
        out: dict[tuple[str, str], str] = {}
        if not os.path.exists(hold_csv_path):
            log.warning("Admin holds file not found at %s — no holds applied.", hold_csv_path)
            return out

        with open(hold_csv_path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                uid = (row.get("user_id") or "").strip()
                cid = (row.get("course_id") or "").strip()
                if not uid or not cid:
                    continue
                raw_status = (row.get("status") or "").strip().lower()
                if raw_status in ("", "pending_review", "pending"):
                    status = "pending_review"
                elif raw_status in ("approved_for_deletion", "approved"):
                    status = "approved_for_deletion"
                else:
                    log.warning(
                        "Admin hold row for %s/%s has unrecognized status '%s' — "
                        "treating as pending_review (safer default).",
                        uid, cid, raw_status,
                    )
                    status = "pending_review"
                out[(uid, cid)] = status

        log.info("Loaded %d admin hold entries from %s", len(out), hold_csv_path)
        return out

    def record_suppression(
        self,
        rec: EnrollmentRecord,
        reason: str,
        run_ts: datetime | None = None,
    ) -> None:
        ts = (run_ts or datetime.now()).isoformat(timespec="seconds")
        self.conn.execute(
            """INSERT INTO suppressed_deletions
               (run_ts, user_id, course_id, section_id, role, reason)
               VALUES (?,?,?,?,?,?)""",
            (ts, rec.user_id, rec.course_id, rec.section_id, rec.role, reason),
        )
        self.conn.commit()

    # ---------------------------------------------------------------- sections
    def get_section_sizes(self, course_id: str) -> dict[str, int]:
        """
        Return {section_id: active_enrollment_count} for every section that
        has at least one active enrollment in this course. Used by cohort
        loaders to pick the smallest section during capacity-balanced
        assignment.
        """
        sizes: dict[str, int] = {}
        for row in self.conn.execute(
            """SELECT section_id, COUNT(*) AS n
                 FROM enrollments
                WHERE course_id = ? AND status = 'active'
                GROUP BY section_id""",
            (course_id,),
        ):
            sizes[row["section_id"]] = row["n"]
        return sizes

    def record_section_creation(
        self,
        course_id: str,
        section_id: str,
        name: str,
        created_by: str,
    ) -> None:
        """
        Record that we have created a section so the engine can emit it in
        sections.csv. Idempotent.
        """
        self.conn.execute(
            """INSERT OR IGNORE INTO sections
               (course_id, section_id, name, created_by, created_on)
               VALUES (?,?,?,?,?)""",
            (course_id, section_id, name, created_by, date.today().isoformat()),
        )
        self.conn.commit()

    def iter_sections(self) -> Iterator[SectionRecord]:
        for row in self.conn.execute("SELECT * FROM sections ORDER BY course_id, section_id"):
            yield SectionRecord(**dict(row))

    def section_exists(self, course_id: str, section_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sections WHERE course_id = ? AND section_id = ?",
            (course_id, section_id),
        ).fetchone()
        return row is not None

    # ------------------------------------------------------------------ misc
    def iter_active(self) -> Iterator[EnrollmentRecord]:
        for row in self.conn.execute(
            "SELECT * FROM enrollments WHERE status = ?", (STATUS_ACTIVE,)
        ):
            yield EnrollmentRecord(**dict(row))

    def close(self) -> None:
        self.conn.close()
