# https://github.com/apache/arrow/issues/30776
# The issue says "the reproducer in ARROW-15265" (#30758) occasionally fails
# with "Path does not exist", about 2 runs in 100. This is the 8-partition
# write from that reproducer, against a local MinIO instead of real S3,
# repeated 100 times. Changed: the filesystem is built for the emulator,
# bucket name, the loop. Added: counting, RESULT line.
# Caveat: the earlier note attributes the failure to S3 not yet listing a
# just-created directory. MinIO is strongly consistent, so it may be unable
# to show this at all; the run on 7.0.0 (which the note says is before the
# fix) is the check on that.
import os
import platform
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds

print("pyarrow", pa.__version__, "|", platform.platform())

bucket = "my-bucket-30776"
s3 = emu.s3fs()
emu.s3_create_bucket(bucket)

cols_8 = ["a", "b", "c", "d", "e", "f", "g", "h"]
table_8 = pa.table(
    {"col1": cols_8 * 5, "col2": np.random.randn(len(cols_8) * 5)}
)

RUNS = 100
errors = Counter()
for i in range(RUNS):
    try:
        ds.write_dataset(
            data=table_8,
            base_dir=f"{bucket}/test8.parquet",
            format="parquet",
            partitioning=["col1"],
            partitioning_flavor="hive",
            filesystem=s3,
            existing_data_behavior="delete_matching",
        )
    except Exception as exc:
        kind = "Path does not exist" if "Path does not exist" in str(exc) else type(exc).__name__
        errors[kind] += 1
        if sum(errors.values()) <= 3:
            print(f"run {i}: {type(exc).__name__}: {str(exc)[:200]}")

failed = sum(errors.values())
print(f"RESULT: {failed} of {RUNS} writes failed {dict(errors) if errors else ''}")
