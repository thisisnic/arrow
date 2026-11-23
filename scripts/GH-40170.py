# https://github.com/apache/arrow/issues/40170
# Reporter's two calls, unchanged. Added: version prints, try/except so the
# second call runs even if the first fails, RESULT line.
import platform
import traceback

import pandas as pd
import pyarrow as pa
import pyarrow.compute

print("pyarrow", pa.__version__, "| pandas", pd.__version__, "|", platform.platform())

try:
    print(repr(pa.compute.strftime(pd.Timestamp.utcnow(), "%Y-%m-%d %z")))
except Exception:
    traceback.print_exc()

try:
    print(repr(pa.compute.strptime('2024-02-21 +0000', "%Y-%m-%d %z", "ns")))
    print("RESULT: strptime parsed '2024-02-21 +0000'")
except Exception:
    traceback.print_exc()
    print("RESULT: strptime FAILED on '2024-02-21 +0000' (the reported error)")
