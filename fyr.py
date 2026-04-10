"""
loaders/fyr.py
==============
First-Year Registration roster. The input file is just a list of eIDs;
the target course is configured in the loader's config entry
(`target_course`) and gets re-pointed each semester when the admin
creates the new course in Canvas.

All behavior comes from RosterLoader — this module exists so the config's
`module: loaders.fyr` reference has something to import.
"""

from .base import RosterLoader


class Loader(RosterLoader):
    pass
