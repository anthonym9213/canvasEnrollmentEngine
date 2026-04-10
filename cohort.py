"""
loaders/cohort.py
=================
Generic cohort loader used by Online Student Training, Work-Based Learning
Experience, and any future long-running cohort course. Instantiated once
per (program, year) combination via config entries.

All behavior lives in CohortLoader — join-only enrollment with capacity-
balanced section assignment. Each config entry that uses this module
should supply: target_course, section_sis_id_template, section_name_template,
year, initial_sections, section_cap, role, source_dir, file_pattern.

See loaders/base.py CohortLoader docstring for the assignment algorithm.
"""

from .base import CohortLoader


class Loader(CohortLoader):
    pass
