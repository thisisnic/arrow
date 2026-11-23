# https://github.com/apache/arrow/issues/39399
# The reporter's repro (runsascoded/parquet-diff-test) writes a 0-row, 1-column
# DataFrame with pandas' pyarrow engine under each codec on each OS and
# compares the bytes. The two DataFrame lines are theirs. Added: a SHA-256 of
# each file so runners can be compared from the logs, the same for a table
# written by pyarrow alone (no pandas metadata), and a dump of the Arrow
# schema metadata key order, which is where the earlier note says the
# difference was.
# The comparison is ACROSS runners: read the hashes for the same pyarrow
# version side by side. gzip is expected to differ by one header byte (the OS
# field), per the issue.
import hashlib
import platform

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

print("pyarrow", pa.__version__, "| pandas", pd.__version__, "|", platform.platform())

df = pd.DataFrame([{ 'a': 111 }])
empty_df = df.iloc[:0]


def sha(path):
    with open(path, "rb") as fh:
        data = fh.read()
    return hashlib.sha256(data).hexdigest()[:16], len(data)


parts = []
for codec in ["snappy", "gzip", "brotli", "lz4", "zstd"]:
    path = f"empty_{codec}.parquet"
    try:
        empty_df.to_parquet(path, engine="pyarrow", compression=codec)
        digest, size = sha(path)
        print(f"pandas  {codec:7} {size:6} bytes  sha256 {digest}")
        parts.append(f"{codec}={digest}")
    except Exception as exc:
        print(f"pandas  {codec:7} FAILED {type(exc).__name__}: {exc}")
        parts.append(f"{codec}=FAILED")

table = pa.table({"a": pa.array([], pa.int64())})
pq.write_table(table, "plain.parquet", compression="snappy")
digest, size = sha("plain.parquet")
print(f"pyarrow-only snappy {size:6} bytes  sha256 {digest}")
parts.append(f"pyarrow-only={digest}")

print("RESULT: " + " ".join(parts))

try:
    meta = pq.read_metadata("empty_snappy.parquet").metadata or {}
    print("file key-value metadata keys, in order:", [k.decode() for k in meta])
except Exception as exc:
    print("could not read metadata back:", type(exc).__name__, exc)
