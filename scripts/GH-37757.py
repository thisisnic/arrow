# https://github.com/apache/arrow/issues/37757
# The issue has no pyarrow-only reprex from the reporter. This is the
# pyarrow-only script a maintainer posted on the thread (which did not
# reproduce for them), with the empty input skipped and the comparison
# repeated 20000 times because the report is of a sporadic failure.
import platform

import pyarrow as pa
import pyarrow.compute as pc

print("pyarrow", pa.__version__, "|", platform.platform())

string_padded = [
    ["      αβγ      ", "               "],
    [],
    ["     →δε←      ", "     ζz zζ     ", "      abc      "],
]
expected = [
    [["", "αβγ", "", ""], ["", "", ""]],
    [],
    [["", "→δε←", "", ""], ["", "ζz", "zζ", "", ""], ["", "abc", "", ""]],
]

repeats = 20000
mismatches = 0
first = None
for rep in range(repeats):
    for i, item in enumerate(string_padded):
        if not item:
            continue
        got = pc.utf8_split_whitespace(pa.array(item), max_splits=None).to_pylist()
        if got != expected[i]:
            mismatches += 1
            if first is None:
                first = (rep, i, got)

if first is not None:
    print("first mismatch (repeat, input index, got):", ascii(first))
print(f"RESULT: {mismatches} mismatches in {repeats} repeats")
