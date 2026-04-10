"""
engine.py
=========
Main orchestrator for the Canvas Enrollment Engine.

Flow
----
1. Load config.json, set up logging, construct StateManager + Notifier.
2. Run enrollment_feed_loaders (bulk_sa, nrd, manual).
       These are the feeds where each row already knows its course.
       They upsert active rows and refresh last_seen.
3. Run roster_loaders (fyr, trn).
       These read a list of eIDs and synthesize enrollments into a
       configured target course. Same grace-period behavior as feeds.
4. Run cohort_loaders (online_training, wble).
       Before each loader runs, the target course is existence-checked
       via the Canvas API. If it doesn't exist, the loader is skipped
       and an admin alert is sent.
       Cohort loaders do capacity-balanced section assignment and mark
       their enrollments cohort_locked=True.
5. Apply grace period (cohort-locked rows are excluded).
6. Load admin_missing_user_holds.csv and split pending deletions into:
       - approved_for_deletion -> emit in SIS ZIP
       - pending_review / blank -> suppress, log, leave in DB
7. Write proposed_deletions.csv for admin review.
8. Write enrollments.csv + sections.csv, zip them into sis_import_<env>_<ts>.zip.
9. Purge emitted deletions from state.
10. Optionally upload to Canvas.

Anything that raises during a stage is caught, logged with the stage
name, emailed to admins via notifications.py, and re-raised so the
process exits non-zero.
"""

from __future__ import annotations

import argparse
import csv
import importlib
import json
import logging
import os
import sys
import zipfile
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from typing import Iterable

from notifications import Notifier
from state_manager import (
    EnrollmentKey,
    EnrollmentRecord,
    StateManager,
)

SIS_ENROLLMENT_HEADER = ["course_id", "user_id", "role", "section_id", "status"]
SIS_SECTIONS_HEADER = ["section_id", "course_id", "name", "status"]


# --------------------------------------------------------------- bootstrap
def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def setup_logging(logs_dir: str, env: str) -> logging.Logger:
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"engine_{env}.log")

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s :: %(message)s")
    fh = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    root.addHandler(fh)
    root.addHandler(sh)
    return logging.getLogger("engine")


# ------------------------------------------------------------------- engine
class EnrollmentEngine:
    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.env = self.config["TARGET_ENV"]
        self.paths = self.config["paths"]
        self.log = setup_logging(self.paths["logs_dir"], self.env)
        self.notifier = Notifier(self.config["smtp"], env=self.env)
        self.state = StateManager(
            db_path=self.paths["state_db"],
            grace_period_days=self.config["grace_period_days"],
        )
        self.stage = "init"
        # upload flags — set by CLI
        self.force_upload = False
        self.skip_upload = False
        self.dry_run_upload = False
        # API gate: if True, skip Canvas API calls (useful for offline tests)
        self.skip_api_checks = False

    # --------------------------------------------------------------- stages
    def run(self) -> None:
        self.log.info("=" * 70)
        self.log.info("Enrollment Engine starting — TARGET_ENV=%s", self.env)
        self.log.info("=" * 70)

        try:
            self._stage_run_feed_loaders()
            self._stage_run_roster_loaders()
            self._stage_run_cohort_loaders()
            self._stage_apply_grace()
            to_delete, suppressed, all_pending = self._stage_resolve_deletions()
            self._stage_write_proposed_report(all_pending, suppressed)
            sis_zip = self._stage_build_sis_zip(to_delete)
            self._stage_purge_emitted(to_delete)
            self._stage_upload(sis_zip)

            self.log.info("Run complete. SIS ZIP: %s", sis_zip)
            self.log.info(
                "Summary: to_delete=%d  suppressed=%d  total_pending=%d",
                len(to_delete), len(suppressed), len(all_pending),
            )
        except Exception:
            self.log.exception("Engine failed at stage: %s", self.stage)
            self.notifier.send_failure(stage=self.stage, exc_info=sys.exc_info())
            raise
        finally:
            self.state.close()

    # ---------- 1. enrollment feed loaders -----------------------------
    def _stage_run_feed_loaders(self) -> None:
        today = date.today()
        for entry in self.config.get("enrollment_feed_loaders", []):
            if not entry.get("enabled", False):
                self.log.info("Feed loader %s disabled — skipping.", entry["name"])
                continue
            self.stage = f"feed_loader:{entry['name']}"
            self.log.info("Running feed loader: %s", entry["name"])
            module = importlib.import_module(entry["module"])
            loader = module.Loader(self.config, entry)
            rows = list(loader.load(self.state))
            touched = self.state.upsert_seen(
                rows,
                source=loader.category,
                loader_name=entry["name"],
                cohort_locked=False,
                run_date=today,
            )
            self.log.info("  %s: %d rows ingested", entry["name"], touched)

    # ---------- 2. roster loaders (fyr/trn) ----------------------------
    def _stage_run_roster_loaders(self) -> None:
        today = date.today()
        for entry in self.config.get("roster_loaders", []):
            if not entry.get("enabled", False):
                self.log.info("Roster loader %s disabled — skipping.", entry["name"])
                continue
            self.stage = f"roster_loader:{entry['name']}"
            self.log.info(
                "Running roster loader: %s -> %s",
                entry["name"], entry.get("target_course", "?"),
            )
            module = importlib.import_module(entry["module"])
            loader = module.Loader(self.config, entry)
            rows = list(loader.load(self.state))
            touched = self.state.upsert_seen(
                rows,
                source=loader.category,
                loader_name=entry["name"],
                cohort_locked=False,
                run_date=today,
            )
            self.log.info("  %s: %d rows ingested", entry["name"], touched)

    # ---------- 3. cohort loaders --------------------------------------
    def _stage_run_cohort_loaders(self) -> None:
        today = date.today()
        cohort_entries = self.config.get("cohort_loaders", [])
        if not cohort_entries:
            return

        api = None
        if not self.skip_api_checks:
            try:
                from canvas_api import CanvasAPI
                api = CanvasAPI(self.config)
            except Exception as e:
                self.log.warning(
                    "Canvas API unavailable (%s) — cohort course existence "
                    "checks will be skipped. Cohort loaders will still run.",
                    e,
                )

        for entry in cohort_entries:
            if not entry.get("enabled", False):
                self.log.info("Cohort loader %s disabled — skipping.", entry["name"])
                continue

            self.stage = f"cohort_loader:{entry['name']}"
            target_course = entry.get("target_course", "")
            self.log.info(
                "Running cohort loader: %s -> %s",
                entry["name"], target_course,
            )

            if api is not None:
                try:
                    if not api.course_exists(target_course):
                        msg = (
                            f"Cohort loader '{entry['name']}' target course "
                            f"'{target_course}' does not exist in Canvas. "
                            f"Loader skipped for this run. Create the course "
                            f"manually and the engine will pick it up next run."
                        )
                        self.log.warning(msg)
                        self.notifier.send_info(
                            subject=f"Cohort course missing: {target_course}",
                            body=msg,
                        )
                        continue
                except Exception as e:
                    # API check itself failed — log and proceed. We don't
                    # want a transient Canvas outage to skip legitimate work.
                    self.log.warning(
                        "Course-existence check failed for %s (%s) — "
                        "running loader anyway.", target_course, e,
                    )

            module = importlib.import_module(entry["module"])
            loader = module.Loader(self.config, entry)
            rows = list(loader.load(self.state))
            touched = self.state.upsert_seen(
                rows,
                source=loader.category,
                loader_name=entry["name"],
                cohort_locked=True,
                run_date=today,
            )
            self.log.info("  %s: %d rows placed (cohort_locked)", entry["name"], touched)

    # ---------- 4. grace period ----------------------------------------
    def _stage_apply_grace(self) -> None:
        self.stage = "apply_grace_period"
        flipped = self.state.apply_grace_period()
        self.log.info("Grace period applied: %d enrollments now pending_deletion.", flipped)

    # ---------- 5. resolve holds ---------------------------------------
    def _stage_resolve_deletions(
        self,
    ) -> tuple[list[EnrollmentRecord], list[EnrollmentRecord], list[EnrollmentRecord]]:
        """
        Split pending_deletion rows into to_delete vs suppressed based on
        admin_missing_user_holds.csv status column.

        Three-state logic:
            not in holds file             -> delete (default)
            status = pending_review       -> suppress
            status = approved_for_deletion-> delete (admin signed off)
        """
        self.stage = "resolve_deletions"
        pending = self.state.get_pending_deletions()
        holds = self.state.load_admin_holds(self.paths["admin_holds"])

        to_delete: list[EnrollmentRecord] = []
        suppressed: list[EnrollmentRecord] = []

        for rec in pending:
            hold_status = holds.get((rec.user_id, rec.course_id))
            if hold_status is None:
                # not reviewed — default behavior is to delete after grace
                to_delete.append(rec)
            elif hold_status == "approved_for_deletion":
                self.log.info(
                    "APPROVED: %s from %s (role=%s) — admin approved deletion.",
                    rec.user_id, rec.course_id, rec.role,
                )
                to_delete.append(rec)
            else:  # pending_review
                self.log.info(
                    "HOLD: suppressing deletion of %s from %s (role=%s) — pending admin review.",
                    rec.user_id, rec.course_id, rec.role,
                )
                self.state.record_suppression(rec, reason="pending admin review")
                suppressed.append(rec)

        return to_delete, suppressed, pending

    # ---------- 6. proposed_deletions.csv ------------------------------
    def _stage_write_proposed_report(
        self,
        all_pending: Iterable[EnrollmentRecord],
        suppressed: Iterable[EnrollmentRecord],
    ) -> None:
        self.stage = "write_proposed_deletions"
        out_path = self.paths["proposed_deletions_report"]
        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        held_keys = {(r.user_id, r.course_id) for r in suppressed}
        holds = self.state.load_admin_holds(self.paths["admin_holds"])

        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow([
                "user_id", "course_id", "section_id", "role",
                "first_seen", "last_seen", "days_missing",
                "source", "loader_name", "status",
                "hold_status", "suggested_action",
            ])
            today = date.today()
            for r in all_pending:
                try:
                    days_missing = (today - date.fromisoformat(r.last_seen)).days
                except ValueError:
                    days_missing = ""
                key = (r.user_id, r.course_id)
                hold_status = holds.get(key, "")
                if key in held_keys:
                    suggested = "Admin: set status=approved_for_deletion to remove, or investigate"
                else:
                    suggested = "Will be deleted this run (or already was)"
                w.writerow([
                    r.user_id, r.course_id, r.section_id, r.role,
                    r.first_seen, r.last_seen, days_missing,
                    r.source, r.loader_name, r.status,
                    hold_status or "(not in holds file)",
                    suggested,
                ])
        self.log.info("Proposed deletions report written: %s", out_path)

    # ---------- 7. build SIS ZIP (enrollments.csv + sections.csv) ------
    def _stage_build_sis_zip(self, to_delete: list[EnrollmentRecord]) -> str:
        self.stage = "build_sis_zip"
        out_dir = self.paths["output_dir"]
        os.makedirs(out_dir, exist_ok=True)

        enrollments_csv = os.path.join(out_dir, "enrollments.csv")
        sections_csv = os.path.join(out_dir, "sections.csv")

        # sections.csv — every section the engine has created
        with open(sections_csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(SIS_SECTIONS_HEADER)
            section_count = 0
            for sec in self.state.iter_sections():
                w.writerow([sec.section_id, sec.course_id, sec.name, "active"])
                section_count += 1

        # enrollments.csv — all active, plus the to_delete set as 'deleted'
        with open(enrollments_csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(SIS_ENROLLMENT_HEADER)
            active_count = 0
            for rec in self.state.iter_active():
                w.writerow([rec.course_id, rec.user_id, rec.role, rec.section_id, "active"])
                active_count += 1
            for rec in to_delete:
                w.writerow([rec.course_id, rec.user_id, rec.role, rec.section_id, "deleted"])

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = os.path.join(out_dir, f"sis_import_{self.env}_{ts}.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(sections_csv, arcname="sections.csv")
            zf.write(enrollments_csv, arcname="enrollments.csv")

        self.log.info(
            "SIS ZIP: %d sections, %d active enrollments, %d deletions -> %s",
            section_count, active_count, len(to_delete), zip_path,
        )
        return zip_path

    # ---------- 8. purge emitted deletions -----------------------------
    def _stage_purge_emitted(self, to_delete: list[EnrollmentRecord]) -> None:
        self.stage = "purge_emitted"
        keys = [
            EnrollmentKey(r.user_id, r.course_id, r.section_id, r.role)
            for r in to_delete
        ]
        n = self.state.purge_deleted(keys)
        self.log.info("Purged %d emitted deletions from state DB.", n)

    # ---------- 9. upload ----------------------------------------------
    def _stage_upload(self, zip_path: str) -> None:
        upload_cfg = self.config.get("upload", {})
        if not upload_cfg.get("enabled", False) and not self.force_upload:
            self.log.info("Upload disabled — ZIP left at %s for manual review.", zip_path)
            return
        if self.skip_upload:
            self.log.info("Upload explicitly skipped via --no-upload.")
            return

        self.stage = "upload_to_canvas"
        from canvas_uploader import CanvasUploader

        uploader = CanvasUploader(self.config)
        result = uploader.upload(zip_path, dry_run=self.dry_run_upload)
        if self.dry_run_upload or result.sis_import_id is None:
            return

        if upload_cfg.get("poll_until_done", True):
            final = uploader.poll(
                result.sis_import_id,
                interval_seconds=upload_cfg.get("poll_interval_seconds", 15),
                timeout_seconds=upload_cfg.get("poll_timeout_seconds", 1800),
            )
            if not final.ok:
                raise RuntimeError(
                    f"Canvas SIS import finished in non-success state: "
                    f"{final.workflow_state} (id={final.sis_import_id})"
                )
            self.log.info(
                "Canvas SIS import %s finished: %s",
                final.sis_import_id, final.workflow_state,
            )


# --------------------------------------------------------------------- cli
def main() -> int:
    parser = argparse.ArgumentParser(description="Canvas SIS Enrollment Engine")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--upload", action="store_true", help="Force upload")
    parser.add_argument("--no-upload", action="store_true", help="Skip upload")
    parser.add_argument("--dry-run-upload", action="store_true")
    parser.add_argument(
        "--skip-api-checks", action="store_true",
        help="Skip Canvas API course-existence checks (offline mode)",
    )
    args = parser.parse_args()

    engine = EnrollmentEngine(args.config)
    engine.force_upload = args.upload
    engine.skip_upload = args.no_upload
    engine.dry_run_upload = args.dry_run_upload
    engine.skip_api_checks = args.skip_api_checks
    try:
        engine.run()
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
