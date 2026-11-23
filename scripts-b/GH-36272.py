# https://github.com/apache/arrow/issues/36272
# The reporter scanned 300 files of a dataset on real S3 through
# polars.scan_pyarrow_dataset and got "AWS Error NETWORK_CONNECTION during
# HeadObject operation: curlCode 43". Their three lines are kept. Changed:
# the dataset is 300 small Parquet files created here on a local MinIO, and
# the filesystem is built for the emulator. Added: a control using
# `.scanner`, which they say works; try/except; RESULT line.
# Caveat: the error came from the network layer talking to real S3, so an
# emulator on localhost may not show it.
import os
import platform
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.dataset import dataset

print("pyarrow", pa.__version__, "| polars", pl.__version__, "|", platform.platform())

bucket = "bucket-36272"
fs = emu.s3fs()
fs.create_dir(bucket)
for i in range(300):
    pq.write_table(pa.table({"area": [float(i), float(i) + 0.5], "id": [i, i]}),
                   f"{bucket}/data/part-{i:04d}.parquet", filesystem=fs)

ds = dataset(f"{bucket}/data", format="parquet", filesystem=fs)
files = ds.files
print("files in dataset:", len(files))

try:
    ds = dataset(files[:300], format="parquet", filesystem=fs)
    control = ds.scanner(columns=["area"]).head(5)
    scanner = f"ok ({control.num_rows} rows)"
except Exception as exc:
    traceback.print_exc()
    scanner = f"FAILED ({type(exc).__name__})"

try:
    ds = dataset(files[:300], format="parquet", filesystem=fs)
    out = pl.scan_pyarrow_dataset(ds).select(pl.col("area").head()).collect()
    print(out)
    polars = f"ok ({out.height} rows)"
except Exception as exc:
    traceback.print_exc()
    polars = f"FAILED ({type(exc).__name__}: {str(exc)[:140]})"

print(f"RESULT: .scanner {scanner}; polars.scan_pyarrow_dataset {polars}")
