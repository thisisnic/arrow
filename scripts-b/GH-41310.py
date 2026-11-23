# https://github.com/apache/arrow/issues/41310
# CONSTRUCTED: the issue has no script. The reporter says reads through
# S3FileSystem fail with "Unexpected end of stream: Page was smaller than
# expected" or "Couldn't deserialize thrift" when another process overwrites
# the object during the read, because the size comes from a HEAD and the data
# from later ranged GETs with nothing tying them to the same object version.
# This makes that sequence deterministic against a local MinIO: open the file
# and read its footer, overwrite the object with a different Parquet file,
# then read the data. A control does the same without the overwrite.
import os
import platform
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

print("pyarrow", pa.__version__, "|", platform.platform())

bucket = "bucket-41310"
s3 = emu.s3fs()
s3.create_dir(bucket)

rng = np.random.default_rng(0)
first = pa.table({"x": rng.standard_normal(400_000), "y": rng.integers(0, 1000, 400_000)})
second = pa.table({"x": rng.standard_normal(250_000), "y": rng.integers(0, 1000, 250_000)})


def attempt(label, overwrite):
    path = f"{bucket}/{label}.parquet"
    pq.write_table(first, path, filesystem=s3, row_group_size=50_000)
    try:
        f = s3.open_input_file(path)
        pf = pq.ParquetFile(f)
        print(f"{label}: opened, footer says {pf.metadata.num_rows} rows in {pf.metadata.num_row_groups} row groups")
        if overwrite:
            pq.write_table(second, path, filesystem=s3, row_group_size=50_000)
            print(f"{label}: object overwritten with a {second.num_rows}-row file")
        table = pf.read()
        same = table.equals(first)
        print(f"{label}: read {table.num_rows} rows, equal to the original: {same}")
        return f"read {table.num_rows} rows, equal to original={same}"
    except Exception as exc:
        traceback.print_exc()
        return f"FAILED ({type(exc).__name__}: {str(exc)[:110]})"


control = attempt("control", overwrite=False)
overwritten = attempt("overwritten", overwrite=True)

print(f"RESULT: no overwrite: {control}; overwritten between open and read: {overwritten}")
