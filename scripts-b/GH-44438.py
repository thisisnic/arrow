# https://github.com/apache/arrow/issues/44438
# Reporter's script against the GCS storage testbench instead of real GCS.
# Changed: GcsFileSystem is built for the emulator (no credentials file), the
# bucket name, and the bucket is created here. Added: try/except, RESULT line.
import os
import platform
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

from fsspec.implementations.arrow import ArrowFSWrapper
import fsspec
import pandas
import pyarrow
import pyarrow.dataset as dt;

print("pyarrow", pyarrow.__version__, "| fsspec", fsspec.__version__, "| pandas", pandas.__version__,
      "|", platform.platform())

bucket = "bucket-44438"
emu.gcs_create_bucket(bucket)

fileset_storage_location = f"gs://{bucket}/catalog/schema/fileset3"
selffs = ArrowFSWrapper(emu.gcsfs_arrow())
data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})
parquet_file = fileset_storage_location + "/test.parquet"

try:
    data.to_parquet(parquet_file, filesystem=selffs)
    write = "ok"
except Exception as exc:
    traceback.print_exc()
    write = f"FAILED ({type(exc).__name__})"

try:
    arrow_dataset = dt.dataset(parquet_file, filesystem=selffs)
    rows = arrow_dataset.to_table().num_rows
    read = f"ok, {rows} rows"
except Exception as exc:
    traceback.print_exc()
    read = f"FAILED ({type(exc).__name__}: {str(exc)[:120]})"

print(f"RESULT: to_parquet {write}; dataset() {read}")
