# https://github.com/apache/arrow/issues/47562
# Reporter's snippet. Changed: `import pyarrow as pa` added, and the two
# unittest `self.assertEqual` calls replaced by printing the `==` result,
# try/except around each call, RESULT line.
import platform
import struct
import sys
import traceback

import pyarrow as pa

print("pyarrow", pa.__version__, "|", platform.platform(), "|", sys.platform,
      "|", struct.calcsize("P") * 8, "bit")

table = pa.Table.from_pydict(
    {
        "A": [1, 3, 1],
        "B": [2, 4, 2],
    }
)
prefix = "col_"
expected = pa.Table.from_pydict(
    {
        f"{prefix}A": [1, 3, 1],
        f"{prefix}B": [2, 4, 2],
    }
)
print("schema types:", table.schema.types)

# OK when passing a list to rename_columns
try:
    r_list = table.rename_columns([f"{prefix}{k}" for k in table.column_names])
    print("list:", r_list == expected, r_list.column_names)
    list_result = f"equal={r_list == expected}"
except Exception:
    traceback.print_exc()
    list_result = "RAISED"

# KO when passing a dict to rename_columns (only in Windows)
try:
    r_dict = table.rename_columns({k: f"{prefix}{k}" for k in table.column_names})
    print("dict:", r_dict == expected, r_dict.column_names)
    dict_result = f"equal={r_dict == expected}"
except Exception:
    traceback.print_exc()
    dict_result = "RAISED"

print(f"RESULT: list {list_result}, dict {dict_result}")
