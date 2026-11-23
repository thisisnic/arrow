# https://github.com/apache/arrow/issues/34081
# Reporter's call, unchanged. Added: version prints, try/except, RESULT line,
# and the "+0100" variant a maintainer suggested on the thread as the form
# that should work on macOS.
import platform
import traceback

import pyarrow as pa
from pyarrow.compute import strptime

print("pyarrow", pa.__version__, "|", platform.platform())

try:
    print(strptime(["2012-01-01 01:02:03+01:00"], format="%Y-%m-%d %H:%M:%S%z", unit="s"))
    colon = "parsed"
except Exception:
    traceback.print_exc()
    colon = "FAILED (the reported error)"

try:
    print(strptime(["2012-01-01 01:02:03+0100"], format="%Y-%m-%d %H:%M:%S%z", unit="s"))
    no_colon = "parsed"
except Exception:
    traceback.print_exc()
    no_colon = "FAILED"

print(f"RESULT: '+01:00' {colon}; '+0100' {no_colon}")
