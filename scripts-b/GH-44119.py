# https://github.com/apache/arrow/issues/44119
# The reporter set `endpoint_url` for a profile in ~/.aws/config, set
# AWS_PROFILE, and called pq.read_table("s3://my-bucket/my-parquet"); it did
# not reach their S3-compatible store. They note the AWS_ENDPOINT_URL
# environment variable does work.
# Here the S3-compatible store is a local MinIO. Setup writes a Parquet file
# with an explicitly configured filesystem. The reporter's three lines then
# run in fresh Python processes, so nothing from setup leaks in:
#   A. endpoint_url only in the profile (the report)
#   B. AWS_ENDPOINT_URL in the environment (the reporter's working case)
# Config and credentials files are written to the working directory, not ~.
import os
import platform
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import emu

import pyarrow as pa
import pyarrow.parquet as pq

print("pyarrow", pa.__version__, "|", platform.platform())

bucket = "ci-bug-44119-bucket"
s3 = emu.s3fs()
emu.s3_create_bucket(bucket)
pq.write_table(pa.table({"a": [1, 2, 3]}), f"{bucket}/my-parquet", filesystem=s3)

config = os.path.abspath("aws-config")
credentials = os.path.abspath("aws-credentials")
with open(config, "w") as f:
    f.write(
        "[profile my-bucket-role]\n"
        f"region = {emu.S3_REGION}\n"
        f"endpoint_url = http://{emu.S3_HOST}\n"
    )
with open(credentials, "w") as f:
    f.write(
        "[my-bucket-role]\n"
        f"aws_access_key_id = {emu.S3_ACCESS_KEY}\n"
        f"aws_secret_access_key = {emu.S3_SECRET_KEY}\n"
    )

reader = f'''
import os
import pyarrow.parquet as pq

os.environ["AWS_PROFILE"] = "my-bucket-role"

tbl = pq.read_table("s3://{bucket}/my-parquet")
print("rows:", tbl.num_rows)
'''


def run(label, extra_env):
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    env.update({"AWS_CONFIG_FILE": config, "AWS_SHARED_CREDENTIALS_FILE": credentials})
    env.update(extra_env)
    try:
        proc = subprocess.run([sys.executable, "-c", reader], env=env, timeout=150,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = proc.stdout.decode("utf-8", errors="replace")
        outcome = "ok" if proc.returncode == 0 else "FAILED"
    except subprocess.TimeoutExpired as exc:
        out = (exc.stdout or b"").decode("utf-8", errors="replace")
        outcome = "TIMED OUT after 150 s"
    print(f"== {label}: {outcome}")
    print(out[-1500:])
    last = [l for l in out.strip().splitlines() if l.strip()]
    return f"{outcome}" + (f" ({last[-1][:140]})" if last and outcome != "ok" else "")


a = run("A. endpoint_url only in the profile", {})
b = run("B. AWS_ENDPOINT_URL in the environment", {"AWS_ENDPOINT_URL": f"http://{emu.S3_HOST}"})

print(f"RESULT: profile endpoint_url: {a}; AWS_ENDPOINT_URL env var: {b}")
