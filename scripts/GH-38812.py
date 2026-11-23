# https://github.com/apache/arrow/issues/38812
# The reporter posted only the retry loop, not a full script. The loop below
# is theirs (parse_options and convert_options left at their defaults). The
# CSV is CONSTRUCTED from their description: one field several million
# characters long, read with a small block_size so read_csv raises.
# Added: after each exception, rename the file and rename it back, which
# Windows refuses while another handle is open on it.
# At the end, try to remove the file, which is what the reporter could not do.
import os
import platform

import pyarrow as pa
from pyarrow import csv

print("pyarrow", pa.__version__, "|", platform.platform())

csv_file_path = os.path.abspath("long_field.csv")
with open(csv_file_path, "w") as f:
    f.write("a,b\n")
    f.write("1," + "x" * 5_000_000 + "\n")
    f.write("2,short\n")


def handle_free():
    """True if the file can be renamed, i.e. no other handle is holding it."""
    moved = csv_file_path + ".moved"
    try:
        os.rename(csv_file_path, moved)
        os.rename(moved, csv_file_path)
        return True
    except OSError as exc:
        print("   rename refused:", type(exc).__name__, exc)
        return False


read_options = csv.ReadOptions(block_size=1 << 16)
failures = 0
locked_after_exception = 0
while True:
    try:
        table = csv.read_csv(csv_file_path, read_options=read_options)
        break
    except pa.lib.ArrowInvalid:
        failures += 1
        print(f'Doubling CSV block_size from {read_options.block_size}')
        if not handle_free():
            locked_after_exception += 1
        read_options.block_size *= 2

print("rows read:", table.num_rows, "| exceptions before success:", failures)
del table

try:
    os.remove(csv_file_path)
    removed = "removed"
except OSError as exc:
    print("remove refused:", type(exc).__name__, exc)
    removed = "COULD NOT BE REMOVED"

print(f"RESULT: {failures} exceptions; file locked after {locked_after_exception} of them; at the end the file was {removed}")
