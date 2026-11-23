# https://github.com/apache/arrow/issues/40337
# The two assertions from pyarrow's test_dateutil_tzinfo_to_string, as quoted
# in the issue, with the asserts replaced by prints. Added: version prints,
# repr of the dateutil tz object (asked for on the thread), RESULT line.
# Reported failure: 'Europe/Monaco' instead of 'Europe/Paris' on Windows with
# python-dateutil 2.9.0.
import platform
import traceback

import dateutil
import dateutil.tz
import pyarrow as pa

print("pyarrow", pa.__version__, "| python-dateutil", dateutil.__version__, "|", platform.platform())

try:
    tz = dateutil.tz.UTC
    print("UTC ->", repr(pa.lib.tzinfo_to_string(tz)))
    tz = dateutil.tz.gettz('Europe/Paris')
    print("dateutil tz object:", repr(tz))
    got = pa.lib.tzinfo_to_string(tz)
    print("Europe/Paris ->", repr(got))
    print(f"RESULT: tzinfo_to_string(gettz('Europe/Paris')) = {got!r} (expected 'Europe/Paris'); tz repr {tz!r}")
except Exception:
    traceback.print_exc()
    print("RESULT: RAISED, see log")
