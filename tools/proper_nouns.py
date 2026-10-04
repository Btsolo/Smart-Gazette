"""
PLACEHOLDER - pass-through stand-in for a module that was never committed.

probate_template.py and land_template.py call PN.repair_name / PN.repair_names
(dedicated proper-noun repair, commit cf3c72d). The original file was lost, so
these functions return their input unchanged. That keeps the tools runnable
for measurement: categories, keyword precision, notice detection and structure
do not depend on name repair. Template name-quality figures measured with this
stub are PROVISIONAL (they may be lower than with the real repair step).

Replace with a real implementation before trusting any name-quality number.
"""


def repair_name(name):
    return name


def repair_names(names):
    return list(names or [])
