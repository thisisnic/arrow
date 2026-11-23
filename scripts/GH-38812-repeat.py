# Follow-up to https://github.com/apache/arrow/issues/38812.
# NOT the reporter's code. An earlier check on Linux found the CSV file was
# occasionally still open for a few milliseconds after read_csv had raised,
# only with use_threads=True. On Windows an open handle blocks rename and
# delete, so this repeats a failing read_csv many times and, straight after
# each exception, tries to rename the file and rename it back.
import os
import platform
import time

import pyarrow as pa
from pyarrow import csv

print("pyarrow", pa.__version__, "|", platform.platform())

path = os.path.abspath("long_field.csv")
with open(path, "w") as f:
    f.write("a,b\n")
    f.write("1," + "x" * 5_000_000 + "\n")
    f.write("2,short\n")
moved = path + ".moved"

def try_rename_there_and_back():
    """True if the file could be renamed away. Always leaves it at `path`."""
    try:
        os.rename(path, moved)
    except OSError:
        return False
    # Put it back; retry briefly in case something (e.g. a scanner) holds it.
    for _ in range(50):
        try:
            os.rename(moved, path)
            break
        except OSError:
            time.sleep(0.1)
    return True


REPEATS = 300
MAX_RECHECKS = 10  # only wait and re-check the first few refusals
parts = []
for use_threads in (True, False):
    for block_size in (1 << 16, 1 << 20):
        refused = 0
        rechecked = 0
        still_refused_later = 0
        raised = 0
        for _ in range(REPEATS):
            try:
                csv.read_csv(path, read_options=csv.ReadOptions(block_size=block_size, use_threads=use_threads))
            except pa.lib.ArrowInvalid:
                raised += 1
                if not try_rename_there_and_back():
                    refused += 1
                    if rechecked < MAX_RECHECKS:
                        rechecked += 1
                        time.sleep(0.5)
                        if not try_rename_there_and_back():
                            still_refused_later += 1
        line = (f"use_threads={use_threads} block_size={block_size}: {raised} of {REPEATS} raised, "
                f"rename refused right after {refused}, still refused 0.5 s later {still_refused_later} of {rechecked} re-checked")
        print(line)
        parts.append(f"threads={use_threads}/block={block_size}: refused {refused} of {raised}, "
                     f"still refused 0.5 s later {still_refused_later} of {rechecked} re-checked")

try:
    os.remove(path)
    removed = "removed"
except OSError as exc:
    print("remove refused:", type(exc).__name__, exc)
    removed = "COULD NOT BE REMOVED"

print("RESULT: " + "; ".join(parts) + f"; file {removed} at the end")
