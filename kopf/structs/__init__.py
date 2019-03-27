"""
All the functions to manipulate the resource fields, state changes, etc.

Grouped by the type of the fields and the purpose of the manipulation.

Used in the handling routines to check if there were significant changes at all
(i.e. not our own internal and system changes, like the uids, links, etc),
and to get the exact per-field diffs for the specific handler functions.

All the functions are purely data-manipulative and computational.
No external calls or any i/o activities are done here.
"""
