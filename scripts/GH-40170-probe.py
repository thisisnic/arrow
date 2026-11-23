# Follow-up to https://github.com/apache/arrow/issues/40170 and #34081.
# NOT the reporters' code. The pilot run showed `%z` failing on Windows for
# '+0000', '+0100' and '+01:00'. This checks which inputs parse on each OS,
# with a control that has no `%z`.
import platform

import pyarrow as pa
import pyarrow.compute as pc

print("pyarrow", pa.__version__, "|", platform.platform())

cases = [
    ("%Y-%m-%d", "2024-02-21"),
    ("%Y-%m-%d %H:%M:%S", "2024-02-21 01:02:03"),
    ("%Y-%m-%d %z", "2024-02-21 +0000"),
    ("%Y-%m-%d %z", "2024-02-21 +0100"),
    ("%Y-%m-%d %z", "2024-02-21 -0530"),
    ("%Y-%m-%d %z", "2024-02-21 +01:00"),
    ("%Y-%m-%d %z", "2024-02-21 Z"),
    ("%Y-%m-%d %H:%M:%S%z", "2024-02-21 01:02:03+0100"),
    ("%Y-%m-%d %H:%M:%S%z", "2024-02-21 01:02:03+01:00"),
    ("%Y-%m-%dT%H:%M:%S%z", "2024-02-21T01:02:03+0100"),
]

parsed = []
failed = []
for fmt, value in cases:
    try:
        out = pc.strptime(value, fmt, "s")
        print(f"OK    {fmt!r:26} {value!r:30} -> {out}")
        parsed.append(value)
    except Exception as exc:
        print(f"FAIL  {fmt!r:26} {value!r:30} -> {type(exc).__name__}: {exc}")
        failed.append(value)

# error_is_null gives the same answer without raising.
try:
    arr = pc.strptime(pa.array([v for _, v in cases[2:5]]), "%Y-%m-%d %z", "s", error_is_null=True)
    print("error_is_null=True on the three plain offsets:", arr.to_pylist())
except Exception as exc:
    print("error_is_null=True raised:", type(exc).__name__, exc)

with_z_parsed = [v for v in parsed if v not in ("2024-02-21", "2024-02-21 01:02:03")]
print(f"RESULT: {len(parsed)} of {len(cases)} parsed; with %z: {len(with_z_parsed)} of 8 parsed; failed: {failed}")
