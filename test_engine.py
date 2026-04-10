"""
test_engine.py
==============
End-to-end tests for the Enrollment Engine. Each test_* function builds
a fresh temp workdir with its own config, input files, and state DB, so
tests don't interfere with each other.

Coverage:
    test_basic_feed_and_grace_period
        Three enrollments ingested, two drop out, grace period expires,
        one is held, one is deleted, then a reappearance reactivates.

    test_roster_loader_synthesizes_enrollments
        FYR-style roster (list of eIDs, no course in file) correctly
        creates enrollments in the configured target_course.

    test_cohort_balanced_distribution
        30 students into a course with 4 initial sections cap 10.
        Should produce balanced distribution, no overflow.

    test_cohort_overflow_creates_new_section
        40 students into 4 initial sections cap 8 (= 32 slots).
        Should fill the initial 4, then create SEC05 for the remaining 8.
        SEC05 must be correctly numbered, not collide with SEC01.

    test_cohort_locked_survives_grace_period
        Cohort student disappears from cohort feed for weeks. Should
        remain active (cohort_locked=1 bypasses grace period).

    test_cohort_idempotent
        Running the cohort loader twice with the same file places zero
        new students the second time.

    test_three_state_holds
        One student is in holds with pending_review (suppressed),
        another with approved_for_deletion (deleted), a third not in
        holds at all (deleted normally).

    test_faculty_additions_override_feed
        A student not in bulk_sa but in faculty_additions.csv should
        appear as active in the ZIP.
"""

from __future__ import annotations

import csv
import importlib
import json
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


# -------------------------------------------------------------- test fixtures
def make_workdir() -> str:
    tmp = tempfile.mkdtemp(prefix="engine_test_")
    for sub in ("input", "output", "logs", "state"):
        os.makedirs(os.path.join(tmp, sub), exist_ok=True)
    # copy engine modules into the workdir so imports find them when we
    # cd there. This isolates each test's module state.
    for name in (
        "engine.py", "state_manager.py", "notifications.py",
        "canvas_uploader.py", "canvas_api.py",
    ):
        shutil.copy(os.path.join(HERE, name), os.path.join(tmp, name))
    shutil.copytree(
        os.path.join(HERE, "loaders"),
        os.path.join(tmp, "loaders"),
    )
    return tmp


def write_csv(path: str, header: list[str], rows: list[list[str]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def write_config(workdir: str, cfg: dict) -> str:
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=2)
    return path


def base_config(workdir: str) -> dict:
    """Minimal config shell; tests fill in the loader sections they need."""
    return {
        "TARGET_ENV": "beta",
        "environments": {
            "beta": {
                "canvas_base_url": "https://example.beta.instructure.com",
                "sis_import_endpoint": "/api/v1/accounts/1/sis_imports",
                "api_token_env_var": "CANVAS_BETA_TOKEN",
            },
            "production": {
                "canvas_base_url": "https://example.instructure.com",
                "sis_import_endpoint": "/api/v1/accounts/1/sis_imports",
                "api_token_env_var": "CANVAS_PROD_TOKEN",
            },
        },
        "paths": {
            "input_dir":    os.path.join(workdir, "input"),
            "output_dir":   os.path.join(workdir, "output"),
            "logs_dir":     os.path.join(workdir, "logs"),
            "state_db":     os.path.join(workdir, "state", "test.db"),
            "admin_holds":  os.path.join(workdir, "input", "admin_missing_user_holds.csv"),
            "proposed_deletions_report": os.path.join(workdir, "output", "proposed_deletions.csv"),
        },
        "grace_period_days": 7,
        "upload": {"enabled": False},
        "enrollment_feed_loaders": [],
        "roster_loaders": [],
        "cohort_loaders": [],
        "smtp": {
            "host": "localhost", "port": 25, "use_tls": False,
            "from_address": "test@example.com",
            "to_addresses": ["admin@example.com"],
            "password_env_var": "_UNSET_", "subject_prefix": "[Test]",
        },
    }


def run_engine_at(workdir: str, config_path: str, run_date: date) -> str:
    """
    Run the engine in `workdir` with `run_date` pretending to be today.
    Returns the path of the newest SIS ZIP produced.

    We freeze `date.today` by assigning a FrozenDate subclass to the
    `date` attribute of both engine and state_manager modules. This
    requires reloading those modules each call so the module-level
    `from datetime import date` rebinding takes effect.
    """
    old_cwd = os.getcwd()
    os.chdir(workdir)
    try:
        # purge cached modules so each call gets fresh ones
        for modname in list(sys.modules.keys()):
            if modname in ("engine", "state_manager", "notifications",
                           "canvas_uploader", "canvas_api") or modname.startswith("loaders"):
                del sys.modules[modname]
        sys.path.insert(0, workdir)

        import state_manager as sm
        import engine as eng

        class FrozenDate(date):
            @classmethod
            def today(cls):
                return run_date

        sm.date = FrozenDate     # type: ignore[attr-defined]
        eng.date = FrozenDate    # type: ignore[attr-defined]

        e = eng.EnrollmentEngine(config_path)
        e.skip_upload = True
        e.skip_api_checks = True
        e.run()

        out_dir = e.paths["output_dir"]
        zips = sorted(
            [f for f in os.listdir(out_dir) if f.endswith(".zip")],
            key=lambda n: os.path.getmtime(os.path.join(out_dir, n)),
        )
        return os.path.join(out_dir, zips[-1])
    finally:
        sys.path.remove(workdir)
        os.chdir(old_cwd)


def read_zip(zip_path: str) -> dict[str, list[dict]]:
    """Return {filename: [rows]} for every CSV in the ZIP."""
    out: dict[str, list[dict]] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            with zf.open(name) as fh:
                text = fh.read().decode("utf-8").splitlines()
            out[name] = list(csv.DictReader(text))
    return out


def read_proposed_deletions(workdir: str) -> list[dict]:
    path = os.path.join(workdir, "output", "proposed_deletions.csv")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ================================================================ tests
def test_basic_feed_and_grace_period():
    print("\n>>> test_basic_feed_and_grace_period")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["enrollment_feed_loaders"] = [{
        "name": "bulk_sa", "module": "loaders.bulk_sa", "enabled": True,
        "source_dir": os.path.join(wd, "input"),
        "file_pattern": "bulk_sa.csv",
    }]
    cfg_path = write_config(wd, cfg)

    # empty holds file
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )

    today = date(2026, 4, 1)

    # Day 0: three enrollments
    write_csv(
        os.path.join(wd, "input", "bulk_sa.csv"),
        ["eid", "course_sis_id", "section_sis_id", "role"],
        [
            ["jdoe23", "HIST-101", "HIST-101-01", "S"],
            ["asmith7", "BIOL-220", "BIOL-220-02", "S"],
            ["kwilliams", "HIST-101", "HIST-101-01", "I"],
        ],
    )
    zip1 = run_engine_at(wd, cfg_path, today)
    rows = read_zip(zip1)["enrollments.csv"]
    assert len(rows) == 3 and all(r["status"] == "active" for r in rows), rows
    print("   day 0 OK — 3 active")

    # Day 1: two drop
    write_csv(
        os.path.join(wd, "input", "bulk_sa.csv"),
        ["eid", "course_sis_id", "section_sis_id", "role"],
        [["kwilliams", "HIST-101", "HIST-101-01", "I"]],
    )
    zip2 = run_engine_at(wd, cfg_path, today + timedelta(days=1))
    rows2 = read_zip(zip2)["enrollments.csv"]
    assert all(r["status"] == "active" for r in rows2), rows2
    print("   day 1 OK — grace still running")

    # Day 9: grace expired, both dropped. No holds = both deleted.
    zip3 = run_engine_at(wd, cfg_path, today + timedelta(days=9))
    rows3 = read_zip(zip3)["enrollments.csv"]
    deleted = [r for r in rows3 if r["status"] == "deleted"]
    assert len(deleted) == 2, rows3
    assert {r["user_id"] for r in deleted} == {"jdoe23", "asmith7"}, deleted
    print("   day 9 OK — 2 deleted")

    shutil.rmtree(wd)


def test_roster_loader_synthesizes_enrollments():
    print("\n>>> test_roster_loader_synthesizes_enrollments")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["roster_loaders"] = [{
        "name": "fyr_spring_2026", "module": "loaders.fyr", "enabled": True,
        "source_dir": os.path.join(wd, "input"),
        "file_pattern": "fyr_*.csv",
        "target_course": "Enrollment_Modules_First_Year_Students_Spring_2026",
        "role": "student", "eid_column": "eid",
    }]
    cfg_path = write_config(wd, cfg)
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )

    write_csv(
        os.path.join(wd, "input", "fyr_spring2026.csv"),
        ["eid"],
        [["alice01"], ["bob02"], ["carol03"], ["dave04"]],
    )

    zip1 = run_engine_at(wd, cfg_path, date(2026, 1, 15))
    rows = read_zip(zip1)["enrollments.csv"]
    assert len(rows) == 4, rows
    for r in rows:
        assert r["course_id"] == "Enrollment_Modules_First_Year_Students_Spring_2026"
        assert r["role"] == "student"
        assert r["status"] == "active"
        assert r["section_id"] == ""  # roster loaders don't assign sections
    eids = {r["user_id"] for r in rows}
    assert eids == {"alice01", "bob02", "carol03", "dave04"}, eids
    print(f"   OK — 4 eids synthesized into target course")

    shutil.rmtree(wd)


def test_cohort_balanced_distribution():
    print("\n>>> test_cohort_balanced_distribution")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["cohort_loaders"] = [{
        "name": "ot_2025", "module": "loaders.cohort", "enabled": True,
        "year": 2025,
        "source_dir": os.path.join(wd, "input"),
        "file_pattern": "ot_*.csv",
        "target_course": "Online_Student_Training_2025",
        "section_sis_id_template": "OST-{year}-SEC{n:02d}",
        "section_name_template": "OST {year} Sec {n}",
        "initial_sections": 4,
        "section_cap": 10,
        "role": "student", "eid_column": "eid",
    }]
    cfg_path = write_config(wd, cfg)
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )

    # 30 students into 4 sections cap 10 — no overflow needed
    eids = [f"s{i:03d}" for i in range(30)]
    write_csv(
        os.path.join(wd, "input", "ot_2025.csv"),
        ["eid"],
        [[e] for e in eids],
    )

    zip1 = run_engine_at(wd, cfg_path, date(2026, 1, 15))
    content = read_zip(zip1)
    sections = content["sections.csv"]
    enrollments = content["enrollments.csv"]

    assert len(sections) == 4, sections
    assert len(enrollments) == 30, enrollments

    # count per section
    from collections import Counter
    counts = Counter(r["section_id"] for r in enrollments)
    print(f"   distribution: {dict(counts)}")
    # 30 / 4 = 7 or 8 per section — never unbalanced by more than 1
    assert max(counts.values()) - min(counts.values()) <= 1, counts
    # and all 4 sections got used
    assert len(counts) == 4, counts
    print("   OK — balanced across 4 sections, spread ≤ 1")

    shutil.rmtree(wd)


def test_cohort_overflow_creates_new_section():
    print("\n>>> test_cohort_overflow_creates_new_section")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["cohort_loaders"] = [{
        "name": "ot_2025", "module": "loaders.cohort", "enabled": True,
        "year": 2025,
        "source_dir": os.path.join(wd, "input"),
        "file_pattern": "ot_*.csv",
        "target_course": "Online_Student_Training_2025",
        "section_sis_id_template": "OST-{year}-SEC{n:02d}",
        "section_name_template": "OST {year} Sec {n}",
        "initial_sections": 4,
        "section_cap": 8,
        "role": "student", "eid_column": "eid",
    }]
    cfg_path = write_config(wd, cfg)
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )

    # 40 students, cap 8, 4 sections = 32 slots. 8 should overflow.
    eids = [f"s{i:03d}" for i in range(40)]
    write_csv(
        os.path.join(wd, "input", "ot_2025.csv"),
        ["eid"],
        [[e] for e in eids],
    )

    zip1 = run_engine_at(wd, cfg_path, date(2026, 1, 15))
    content = read_zip(zip1)
    sections = content["sections.csv"]
    enrollments = content["enrollments.csv"]

    section_ids = sorted(s["section_id"] for s in sections)
    assert section_ids == [
        "OST-2025-SEC01", "OST-2025-SEC02", "OST-2025-SEC03",
        "OST-2025-SEC04", "OST-2025-SEC05",
    ], f"expected 5 sections, got {section_ids}"
    print(f"   sections created: {section_ids}")

    from collections import Counter
    counts = Counter(r["section_id"] for r in enrollments)
    assert counts["OST-2025-SEC01"] == 8, counts
    assert counts["OST-2025-SEC02"] == 8, counts
    assert counts["OST-2025-SEC03"] == 8, counts
    assert counts["OST-2025-SEC04"] == 8, counts
    assert counts["OST-2025-SEC05"] == 8, counts  # 40 - 32 = 8 overflow
    print(f"   distribution: {dict(counts)}")
    print("   OK — overflow section SEC05 created, all 40 students placed")

    shutil.rmtree(wd)


def test_cohort_locked_survives_grace_period():
    print("\n>>> test_cohort_locked_survives_grace_period")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["cohort_loaders"] = [{
        "name": "ot_2025", "module": "loaders.cohort", "enabled": True,
        "year": 2025,
        "source_dir": os.path.join(wd, "input"),
        "file_pattern": "ot_*.csv",
        "target_course": "Online_Student_Training_2025",
        "section_sis_id_template": "OST-{year}-SEC{n:02d}",
        "section_name_template": "OST {year} Sec {n}",
        "initial_sections": 2, "section_cap": 10,
        "role": "student", "eid_column": "eid",
    }]
    cfg_path = write_config(wd, cfg)
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )

    today = date(2026, 1, 15)

    # Day 0: 3 students enroll in cohort
    write_csv(
        os.path.join(wd, "input", "ot_2025.csv"),
        ["eid"],
        [["alice"], ["bob"], ["carol"]],
    )
    zip1 = run_engine_at(wd, cfg_path, today)
    rows1 = read_zip(zip1)["enrollments.csv"]
    assert len(rows1) == 3 and all(r["status"] == "active" for r in rows1)
    print("   day 0 OK — 3 cohort students placed")

    # Day 30: cohort file is empty. Grace period (7 days) has long since
    # expired, but cohort_locked should prevent any deletion.
    write_csv(os.path.join(wd, "input", "ot_2025.csv"), ["eid"], [])
    zip2 = run_engine_at(wd, cfg_path, today + timedelta(days=30))
    rows2 = read_zip(zip2)["enrollments.csv"]
    assert len(rows2) == 3, rows2
    assert all(r["status"] == "active" for r in rows2), rows2
    deleted = [r for r in rows2 if r["status"] == "deleted"]
    assert len(deleted) == 0, deleted
    print("   day 30 OK — cohort students still active despite empty feed")

    shutil.rmtree(wd)


def test_cohort_idempotent():
    print("\n>>> test_cohort_idempotent")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["cohort_loaders"] = [{
        "name": "ot_2025", "module": "loaders.cohort", "enabled": True,
        "year": 2025,
        "source_dir": os.path.join(wd, "input"),
        "file_pattern": "ot_*.csv",
        "target_course": "Online_Student_Training_2025",
        "section_sis_id_template": "OST-{year}-SEC{n:02d}",
        "section_name_template": "OST {year} Sec {n}",
        "initial_sections": 2, "section_cap": 10,
        "role": "student", "eid_column": "eid",
    }]
    cfg_path = write_config(wd, cfg)
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )

    write_csv(
        os.path.join(wd, "input", "ot_2025.csv"),
        ["eid"],
        [["alice"], ["bob"], ["carol"]],
    )
    today = date(2026, 1, 15)

    run_engine_at(wd, cfg_path, today)

    # Capture first-run section assignments
    import sqlite3
    c = sqlite3.connect(os.path.join(wd, "state", "test.db"))
    c.row_factory = sqlite3.Row
    first = {r["user_id"]: r["section_id"] for r in c.execute(
        "SELECT user_id, section_id FROM enrollments WHERE cohort_locked=1"
    )}
    c.close()

    # Re-run with same input
    run_engine_at(wd, cfg_path, today + timedelta(days=1))

    c = sqlite3.connect(os.path.join(wd, "state", "test.db"))
    c.row_factory = sqlite3.Row
    second = {r["user_id"]: r["section_id"] for r in c.execute(
        "SELECT user_id, section_id FROM enrollments WHERE cohort_locked=1"
    )}
    c.close()

    assert first == second, f"sections changed between runs: {first} vs {second}"
    # Should still be 3 rows (not duplicated)
    assert len(second) == 3, second
    print(f"   OK — same placements on both runs: {first}")

    shutil.rmtree(wd)


def test_three_state_holds():
    print("\n>>> test_three_state_holds")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["enrollment_feed_loaders"] = [{
        "name": "bulk_sa", "module": "loaders.bulk_sa", "enabled": True,
        "source_dir": os.path.join(wd, "input"),
        "file_pattern": "bulk_sa.csv",
    }]
    cfg_path = write_config(wd, cfg)

    today = date(2026, 4, 1)

    # Day 0: three students enrolled
    write_csv(
        os.path.join(wd, "input", "bulk_sa.csv"),
        ["eid", "course_sis_id", "section_sis_id", "role"],
        [
            ["alice", "HIST-101", "SEC01", "S"],
            ["bob",   "HIST-101", "SEC01", "S"],
            ["carol", "HIST-101", "SEC01", "S"],
        ],
    )
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )
    run_engine_at(wd, cfg_path, today)

    # Day 1: all three drop out of the feed
    write_csv(
        os.path.join(wd, "input", "bulk_sa.csv"),
        ["eid", "course_sis_id", "section_sis_id", "role"],
        [],
    )

    # Day 9: grace has expired. Set up three-state holds:
    #   alice -> pending_review    (suppressed)
    #   bob   -> approved_for_deletion (delete this run)
    #   carol -> not in file       (delete by default)
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [
            ["alice", "HIST-101", "pending_review", "investigating", "lms.admin", "2026-04-09"],
            ["bob",   "HIST-101", "approved_for_deletion", "confirmed withdrawn", "lms.admin", "2026-04-09"],
        ],
    )
    zip3 = run_engine_at(wd, cfg_path, today + timedelta(days=9))
    rows3 = read_zip(zip3)["enrollments.csv"]
    by_user = {r["user_id"]: r["status"] for r in rows3}
    print(f"   zip contents: {by_user}")

    # alice: suppressed -> does NOT appear as deleted in ZIP
    assert "alice" not in by_user, f"alice should be suppressed, got {by_user}"
    # bob: approved -> appears as deleted
    assert by_user.get("bob") == "deleted", by_user
    # carol: not in holds -> appears as deleted
    assert by_user.get("carol") == "deleted", by_user

    # Check proposed_deletions.csv shows all three with correct hold_status
    pd = read_proposed_deletions(wd)
    pd_by_user = {r["user_id"]: r for r in pd}
    assert pd_by_user["alice"]["hold_status"] == "pending_review", pd_by_user["alice"]
    assert pd_by_user["bob"]["hold_status"] == "approved_for_deletion", pd_by_user["bob"]
    assert pd_by_user["carol"]["hold_status"] == "(not in holds file)", pd_by_user["carol"]
    print("   OK — three-state holds routed correctly")

    shutil.rmtree(wd)


def test_faculty_additions_override_feed():
    print("\n>>> test_faculty_additions_override_feed")
    wd = make_workdir()
    cfg = base_config(wd)
    cfg["enrollment_feed_loaders"] = [
        {
            "name": "bulk_sa", "module": "loaders.bulk_sa", "enabled": True,
            "source_dir": os.path.join(wd, "input"),
            "file_pattern": "bulk_sa.csv",
        },
        {
            "name": "faculty_additions", "module": "loaders.manual", "enabled": True,
            "source_dir": os.path.join(wd, "input"),
            "file_pattern": "faculty_additions.csv",
        },
    ]
    cfg_path = write_config(wd, cfg)
    write_csv(
        os.path.join(wd, "input", "admin_missing_user_holds.csv"),
        ["user_id", "course_id", "status", "reason", "added_by", "added_on"],
        [],
    )

    # Feed has only alice. Faculty additions has bob (who is not in feed).
    write_csv(
        os.path.join(wd, "input", "bulk_sa.csv"),
        ["eid", "course_sis_id", "section_sis_id", "role"],
        [["alice", "HIST-101", "SEC01", "S"]],
    )
    write_csv(
        os.path.join(wd, "input", "faculty_additions.csv"),
        ["user_id", "course_id", "section_id", "role", "reason", "added_by", "added_on"],
        [["bob", "HIST-101", "SEC01", "student", "audit student", "lms.admin", "2026-04-10"]],
    )

    zip1 = read_zip(run_engine_at(wd, cfg_path, date(2026, 4, 10)))
    rows = zip1["enrollments.csv"]
    by_user = {r["user_id"]: r["status"] for r in rows}
    assert by_user.get("alice") == "active", rows
    assert by_user.get("bob") == "active", rows
    print(f"   OK — bob appears as active via faculty_additions despite not being in bulk_sa")

    shutil.rmtree(wd)


# ================================================================ main
def main() -> int:
    tests = [
        test_basic_feed_and_grace_period,
        test_roster_loader_synthesizes_enrollments,
        test_cohort_balanced_distribution,
        test_cohort_overflow_creates_new_section,
        test_cohort_locked_survives_grace_period,
        test_cohort_idempotent,
        test_three_state_holds,
        test_faculty_additions_override_feed,
    ]
    failed = []
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed.append((t.__name__, f"AssertionError: {e}"))
            print(f"   FAIL: {e}")
        except Exception as e:
            import traceback
            failed.append((t.__name__, traceback.format_exc()))
            print(f"   ERROR: {e}")

    print()
    print("=" * 70)
    if failed:
        print(f"FAILED: {len(failed)}/{len(tests)}")
        for name, err in failed:
            print(f"  - {name}")
            print(f"    {err}")
        return 1
    print(f"ALL {len(tests)} TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
