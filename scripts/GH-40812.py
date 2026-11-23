# https://github.com/apache/arrow/issues/40812
# The issue is about BUILDING pyarrow from source on Windows on ARM64, which
# this does not test. It only checks whether a released wheel installs and
# works on the runner, which is what the earlier note's "close" rested on.
import platform
import struct
import sysconfig

print("python", platform.python_version(), "|", platform.platform(), "|", platform.machine(),
      "|", sysconfig.get_platform(), "|", struct.calcsize("P") * 8, "bit")

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
pq.write_table(table, "t.parquet")
back = pq.read_table("t.parquet")
total = pc.sum(back["a"]).as_py()

print("pyarrow file:", pa.__file__)
print(f"RESULT: pyarrow {pa.__version__} imported on {platform.machine()} ({sysconfig.get_platform()}); parquet round trip equal={back.equals(table)}, sum={total}")
