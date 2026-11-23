# https://github.com/apache/arrow/issues/31339
# Reporter's read through gcsfs, against the GCS storage testbench instead of
# real GCS. The thread traced the failure to a zero-byte object whose name is
# the directory ("features/MyParquet.parquet/"), which gcsfs reported as a
# file on one call and a directory on the next. This creates that layout,
# prints fs.info() twice as asked on the thread, then runs the reporter's
# read_table both ways. Changed: gcsfs is pointed at the emulator, bucket
# name. Added: setup, try/except, RESULT line.
import os
import platform
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

os.environ["STORAGE_EMULATOR_HOST"] = f"http://{emu.GCS_HOST}"

import gcsfs
import pyarrow as pa
import pyarrow.parquet as pq

print("pyarrow", pa.__version__, "| gcsfs", gcsfs.__version__, "|", platform.platform())

bucket = "bucket-31339"
emu.gcs_create_bucket(bucket)

# The zero-byte directory marker, then two part files under it.
emu.gcs_upload(bucket, "features/MyParquet.parquet/")
for i in range(2):
    table = pa.table({"x": [i, i + 1]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    emu.gcs_upload(bucket, f"features/MyParquet.parquet/year=2022/part-001{i}.snappy.parquet",
                   sink.getvalue().to_pybytes())

fs = gcsfs.GCSFileSystem(token="anon")

bare_path = f"{bucket}/features/MyParquet.parquet/"
for attempt in (1, 2):
    try:
        print(f"fs.info() call {attempt}:", fs.info(bare_path))
    except Exception:
        traceback.print_exc()


def try_read(label, file_path):
    try:
        table = pq.read_table(file_path, filesystem=fs)
        print(f"{label}: ok, {table.num_rows} rows")
        return f"ok ({table.num_rows} rows)"
    except Exception as exc:
        traceback.print_exc()
        return f"FAILED ({type(exc).__name__}: {str(exc)[:100]})"


with_scheme = try_read("with gs://", f"gs://{bucket}/features/MyParquet.parquet/")
without_scheme = try_read("without gs://", bare_path)

print(f"RESULT: read_table with gs:// {with_scheme}; without gs:// {without_scheme}; expected 4 rows")
