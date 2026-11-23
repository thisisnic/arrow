# https://github.com/apache/arrow/issues/38804
# Reporter's Python reprex, unchanged up to `ds_out = ...`. Added: version
# prints, the directory listing, printing what the reporter inspected by hand
# (`ds_out.files`, `ds_out.to_table()`), a second read with hive partitioning,
# try/except around the reads, RESULT line.
# Expected on a correct write: 2 files, 3 rows.
import os
import platform
import traceback

import pyarrow as pa
import pyarrow.dataset as ds

print("pyarrow", pa.__version__, "|", platform.platform())

tbl = pa.table({
    'var_1': ("arrow", "arrow", "arrow"),
    'var_2': ("arrow", "arrow", "arroW")
})

part = ds.partitioning(
    pa.schema([("var_2", pa.string())]),
    flavor="hive"
)

ds.write_dataset(tbl, "dataset_output", format="parquet", partitioning=part)

ds_out = ds.dataset("dataset_output")

on_disk = []
for root, _, files in os.walk("dataset_output"):
    for name in files:
        on_disk.append(os.path.join(root, name).replace(os.sep, "/"))
print("files on disk:", sorted(on_disk))
print("ds_out.files:", ds_out.files)

rows = "read FAILED"
try:
    table = ds_out.to_table()
    print(table)
    rows = table.num_rows
except Exception:
    traceback.print_exc()

hive_rows = "read FAILED"
try:
    hive = ds.dataset("dataset_output", partitioning="hive").to_table()
    print(hive)
    hive_rows = f"{hive.num_rows} (var_2 values {sorted(hive['var_2'].to_pylist())})"
except Exception:
    traceback.print_exc()

print(f"RESULT: {len(on_disk)} files on disk, {rows} rows read, hive read {hive_rows}; expected 2 files and 3 rows")
