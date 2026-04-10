# Canvas Enrollment Engine

Modular SIS import builder for Canvas LMS. Ingests multiple enrollment
feeds of different shapes, applies a grace period before deletions,
gives admins an approval workflow for missing users, and handles
long-running cohorts with capacity-balanced section assignment.

## Loader categories

Every loader falls into one of three categories, each with distinct
input and removal semantics.

**Enrollment feed loaders** (`bulk_sa`, `nrd`, `faculty_additions`)
read files where each row already has user + course + role. They use
normal grace-period semantics: stop appearing in the feed for more than
`grace_period_days` days and the enrollment is marked for deletion.

**Roster loaders** (`fyr`, `trn`) read files that are just a list of
eIDs. Each loader has a configured `target_course` and `role` in its
config block, and synthesizes enrollments pointing at that course. When
the semester rolls over, the admin creates the new course in Canvas and
updates the config to point at it. Same grace-period semantics as feed
loaders.

**Cohort loaders** (`online_training_*`, `wble_*`) also read a list of
eIDs with a configured target course, but with two critical differences.
First, they do **capacity-balanced section assignment**: new students
are placed in the smallest existing section (tie-broken deterministically
by `hash(eid)`), with automatic creation of overflow sections once every
existing section reaches `section_cap`. Second, they are **join-only**:
once a student is placed in a cohort course, they stay there. They are
never removed by the grace period, even if they stop appearing in their
cohort feed. Removal must be done manually by an admin.

## Folder structure

```
enrollment_engine/
├── config.json              # main config
├── config.smoke.json        # local smoke-test config
├── engine.py                # main orchestrator
├── state_manager.py         # SQLite + schema migrations
├── notifications.py         # SMTP failure alerts
├── canvas_api.py            # read-only Canvas API wrapper
├── canvas_uploader.py       # SIS ZIP upload + polling
├── test_engine.py           # full test suite
├── loaders/
│   ├── __init__.py
│   ├── base.py              # EnrollmentFeedLoader / RosterLoader / CohortLoader
│   ├── bulk_sa.py
│   ├── nrd.py
│   ├── manual.py            # reads faculty_additions.csv
│   ├── fyr.py
│   ├── trn.py
│   └── cohort.py            # used by all cohort entries
├── input/                   # (local dev) or /data/sis_feeds/ (production)
└── output/
    ├── proposed_deletions.csv
    └── sis_import_<env>_<ts>.zip
```

Production directory layout recommended:

```
/data/sis_feeds/
├── bulk_enroll/bulk_sa.csv              # overwritten in place
├── fyr/fyr_spring2026.csv
├── trn/trn_spring2026.csv
├── nrd/nrd.csv
├── online_training/
│   ├── ot_2025_fall.csv
│   └── ot_2026_fall.csv
├── wble/
│   └── wble_2025.csv
└── force_enrollment_change/
    ├── faculty_additions.csv            # admin-edited, in git
    └── admin_missing_user_holds.csv     # admin-edited, in git
```

## Admin workflows

### Faculty asks me to add a student who isn't in the feed

Add a row to `faculty_additions.csv`:

```csv
user_id,course_id,section_id,role,reason,added_by,added_on
jsmith99,HIST-101-F25,HIST-101-F25-SEC01,student,Audit student,lms.admin,2026-04-10
```

The student appears as active in the next SIS ZIP and stays active as
long as the row is in the file. To remove the override, delete the row;
the enrollment will then go through the normal grace period.

### A student disappeared from the feed and I want to investigate before they're dropped

Check `proposed_deletions.csv` in the output directory. For any user you
want to hold, add a row to `admin_missing_user_holds.csv` with
`status=pending_review`:

```csv
user_id,course_id,status,reason,added_by,added_on
jdoe23,HIST-101-F25,pending_review,checking with registrar,lms.admin,2026-04-10
```

The engine will skip the deletion and log the suppression. The student
will appear in every subsequent `proposed_deletions.csv` with
`hold_status=pending_review` so you don't lose track of them.

### I finished investigating and the student really should be removed

Edit the same row and change `status` to `approved_for_deletion`:

```csv
user_id,course_id,status,reason,added_by,added_on
jdoe23,HIST-101-F25,approved_for_deletion,confirmed withdrawn,lms.admin,2026-04-11
```

On the next run the deletion flows through. You can then delete the row
from the file entirely.

### New semester — rolling over FYR or TRN

The admin creates the new course in Canvas (e.g.
`Enrollment_Modules_First_Year_Students_Fall_2026`). Then in
`config.json`, update the existing roster loader's `target_course` and
`file_pattern`, or add a new loader entry and disable the old one.

### New semester — rolling over a cohort

Copy the existing cohort loader entry and bump the year:

```json
{
  "name": "online_training_2026",
  "module": "loaders.cohort",
  "enabled": true,
  "year": 2026,
  "source_dir": "/data/sis_feeds/online_training",
  "file_pattern": "ot_2026*.csv",
  "target_course": "Online_Student_Training_2026",
  "section_sis_id_template": "OST-{year}-SEC{n:02d}",
  "initial_sections": 14,
  "section_cap": 350,
  "role": "student"
}
```

Then create the `Online_Student_Training_2026` course manually in
Canvas. On the first run after that, the engine creates the 14 initial
sections and starts placing students. Older cohorts (2025, 2024...)
keep running as long as their entries are `enabled: true` — and can be
set to `enabled: false` once the cohort has graduated, without losing
their data.

## Run

```bash
export CANVAS_BETA_TOKEN=...
export SMTP_PASSWORD=...
python engine.py --config config.json
```

Flags:

- `--upload` — force upload even if `config.upload.enabled=false`
- `--no-upload` — skip upload
- `--dry-run-upload` — go through motions, don't POST
- `--skip-api-checks` — skip Canvas API course-existence checks

Run the test suite:

```bash
python test_engine.py
```

## Schema migration

The state DB is versioned. Upgrading from v1 (previous release) to v2
(this release) happens automatically on first run: two columns are added
to `enrollments` (`loader_name`, `cohort_locked`) and a new `sections`
table is created. Back up your state DB before the first run anyway.

## Known limitations

**Cohort students have no removal path beyond manual intervention.**
This is intentional for now — we don't currently have an authoritative
"student has left the institution" feed, and auto-removing cohort
students based on the bulk_sa feed would incorrectly drop students who
are just between semesters. If you need to remove a cohort student, do
it manually in Canvas.

**Course creation is not automated.** Cohort loaders check that the
target course exists via the Canvas API before running, and skip + alert
if it doesn't. Admins must create courses manually so syllabus/modules/
instructor assignment stay under human control.

**Section capacity is enforced at placement time, not globally.** If
you lower `section_cap` in config for a course that already has
overfull sections, the engine won't rebalance existing students — it
will just stop adding new ones to the overfull sections. This is the
right behavior for stability but worth knowing.

**Between-semester gaps are not handled specially.** If a student
disappears from the bulk_sa feed for the summer, their non-cohort
enrollments will expire through the grace period. Cohort enrollments
are unaffected.
