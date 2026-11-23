# https://github.com/apache/arrow/issues/38854
# Reporter's steps against the GCS storage testbench instead of real GCS.
# Their code: open the file, f.size(), f.seek(0, 2). Changed: the filesystem
# is built for the emulator, the bucket and the 48-byte test.py are created
# here instead of with gsutil. Added: try/except, two extra seeks for
# comparison, RESULT line.
import os
import platform
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

import pyarrow as pa

print("pyarrow", pa.__version__, "|", platform.platform())

bucket = "bucket-38854"
emu.gcs_create_bucket(bucket)
fs = emu.gcsfs_arrow()

content = b"  #test.py\n  x = 1 +2\n  def func(x):\n      return x + 1\n"
with fs.open_output_stream(f"{bucket}/test.py") as out:
    out.write(content)

f = fs.open_input_file(f"{bucket}/test.py")

size = f.size()
print("f.size():", size)


def try_seek(label, *args):
    try:
        pos = f.seek(*args)
        print(f"{label}: ok, position {pos}")
        return "ok"
    except Exception as exc:
        traceback.print_exc()
        return f"FAILED ({type(exc).__name__})"


before_end = try_seek("f.seek(size - 1)", size - 1)
to_end = try_seek("f.seek(0, 2)", 0, 2)  # WHENCE EOF = 2, the reported call
absolute_end = try_seek("f.seek(size)", size)

print(f"RESULT: seek(0, 2) {to_end}; seek(size) {absolute_end}; seek(size - 1) {before_end}")
