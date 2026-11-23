# https://github.com/apache/arrow/issues/40589
# The report is about how many S3 requests S3FileSystem makes. It gives a
# loop (write a tiny object, then move it) and three specific claims:
#   - writing a tiny file uses a 3-request multipart upload
#   - move() and delete re-create the parent directory marker with a PUT
#   - delete does a HEAD before the DELETE
# This runs the reporter's loop body, plus delete_file, against a local MinIO
# and counts the requests MinIO saw for each operation, using
# `mc admin trace`. Changed: filesystem built for the emulator, bucket name,
# 20 iterations instead of 10000, each operation traced separately.
import json
import os
import platform
import subprocess
import sys
import time
import uuid
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

import pyarrow as pa

print("pyarrow", pa.__version__, "|", platform.platform())

bucket = "a-bucket-40589"
sfs = emu.s3fs()
emu.s3_create_bucket(bucket)
sfs.create_dir(f"{bucket}/some_directory")
sfs.create_dir(f"{bucket}/some_other_directory")

N = 20


def traced(label, func):
    """Run func while `mc admin trace` records, and return request counts by API."""
    trace_file = f"trace-{label}.jsonl"
    with open(trace_file, "w") as out:
        proc = subprocess.Popen(["mc", "admin", "trace", "--json", "local"],
                                stdout=out, stderr=subprocess.STDOUT)
        time.sleep(2)  # let the trace attach
        func()
        time.sleep(2)  # let the last events arrive
        proc.terminate()
        proc.wait()
    counts = Counter()
    sample = None
    with open(trace_file) as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if sample is None:
                sample = line[:400]
            api = event.get("api") or event.get("funcName") or event.get("name") or "?"
            counts[str(api)] += 1
    print(f"== {label}: {N} calls -> {sum(counts.values())} requests: {dict(counts)}")
    if sample:
        print("   first trace event:", sample)
    else:
        print("   NO TRACE EVENTS CAPTURED; see", trace_file)
        with open(trace_file) as fh:
            print("   trace output:", fh.read()[:500])
    return counts


paths = [f"{bucket}/some_directory/{uuid.uuid4()}" for _ in range(N)]
moved = [f"{bucket}/some_other_directory/{uuid.uuid4()}" for _ in range(N)]


def do_writes():
    for path in paths:
        with sfs.open_output_stream(path) as fd:
            fd.write(b'hi')


def do_moves():
    for src, dst in zip(paths, moved):
        sfs.move(src, dst)


def do_deletes():
    for path in moved:
        sfs.delete_file(path)


def summarise(counts):
    if not counts:
        return "no trace"
    per_call = {k: round(v / N, 2) for k, v in sorted(counts.items())}
    return f"{round(sum(counts.values()) / N, 2)} requests per call {per_call}"


w = summarise(traced("write-tiny-file", do_writes))
m = summarise(traced("move", do_moves))
d = summarise(traced("delete_file", do_deletes))

print(f"RESULT: write: {w}; move: {m}; delete_file: {d}")
