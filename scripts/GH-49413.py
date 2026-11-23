# https://github.com/apache/arrow/issues/49413
# The report is that `import pyarrow` hangs on macOS arm64 with 23.0.1 and
# works with 15.0.1 (Python 3.11). There is no script in the issue; this is
# the bare import. A driver timeout is the "reproduces" outcome.
import platform
import time

print("python", platform.python_version(), "|", platform.platform(), flush=True)
start = time.time()
import pyarrow

print(f"RESULT: import pyarrow {pyarrow.__version__} completed in {time.time() - start:.2f} s")
