"""Run each script in cases.json against the latest pyarrow and the reported version.

Every run gets a fresh working directory and its own log file. A failing or
hanging script is a result, not an error: the driver always carries on and
exits 0 unless the driver itself breaks.
"""

import argparse
import json
import os
import platform
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
LATEST_PYTHON = "3.13"
REPORTED_PYTHON = "3.11"
DEFAULT_TIMEOUT = 600
INSTALL_TIMEOUT = 900


def packages(version, extra):
    """Arguments for `uv run --with ...` for one pyarrow version."""
    if version == "latest":
        pkgs = ["pyarrow", "numpy"]
    else:
        pkgs = [f"pyarrow=={version}"]
        # Older releases were built against NumPy 1.x.
        major = int(version.split(".")[0])
        pkgs.append("numpy<2" if major < 17 else "numpy")
    pkgs += extra
    args = []
    for pkg in pkgs:
        args += ["--with", pkg]
    return args


def kill_tree(proc):
    """Kill the process and everything it started (uv runs the script as a child)."""
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def run_with_timeout(cmd, cwd, timeout):
    """Run cmd in its own process group; return (output, status)."""
    kwargs = {}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
        status = f"exit {proc.returncode}"
    except subprocess.TimeoutExpired:
        kill_tree(proc)
        try:
            out, _ = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            out = b"(output could not be collected after the kill)"
        status = f"TIMEOUT after {timeout}s"
    return (out or b"").decode("utf-8", errors="replace"), status


def run_once(case, version, run_number, out_dir):
    script = HERE / case["script"]
    python = LATEST_PYTHON if version == "latest" else REPORTED_PYTHON
    uv_run = [
        sys.executable, "-m", "uv", "run", "--no-project", "--quiet",
        "--python", python,
        *packages(version, case.get("extra", [])),
    ]
    cmd = [*uv_run, str(script)]
    timeout = case.get("timeout", DEFAULT_TIMEOUT)
    log = out_dir / f"GH-{case['issue']}__pyarrow-{version}__run{run_number}.log"
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as cwd:
        # Build the environment first, so downloads and installs do not count
        # towards the script's timeout or its timing.
        warm_out, warm_status = run_with_timeout(
            [*uv_run, "python", "-c", "import pyarrow"], cwd, INSTALL_TIMEOUT
        )
        if warm_status != "exit 0":
            output, status = warm_out, f"INSTALL FAILED ({warm_status})"
            start = time.time()
        else:
            start = time.time()
            output, status = run_with_timeout(cmd, cwd, timeout)
    elapsed = time.time() - start
    results = [l[len("RESULT:"):].strip() for l in output.splitlines() if l.startswith("RESULT:")]
    log.write_text(
        f"# issue: {case['issue']}\n# command: {' '.join(cmd)}\n"
        f"# status: {status}\n# seconds: {elapsed:.1f}\n\n{output}",
        encoding="utf-8",
    )
    return {
        "issue": case["issue"], "version": version, "run": run_number,
        "status": status, "seconds": round(elapsed, 1),
        "result": "; ".join(results) if results else "(no RESULT line; see log)",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="out")
    parser.add_argument("--label", default=platform.system())
    parser.add_argument("--only", type=int, action="append", help="issue number(s) to run")
    args = parser.parse_args()

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    env_lines = [
        f"label: {args.label}",
        f"platform: {platform.platform()}",
        f"machine: {platform.machine()}",
        f"image: {os.environ.get('ImageOS', '?')} {os.environ.get('ImageVersion', '?')}",
        f"driver python: {sys.version.split()[0]}",
    ]
    print("\n".join(env_lines), flush=True)

    cases = json.loads((HERE / "cases.json").read_text())
    rows = []
    for case in cases:
        if args.only and case["issue"] not in args.only:
            continue
        for version in ["latest", case["reported_version"]]:
            for run_number in range(1, case.get("repeats", 1) + 1):
                row = run_once(case, version, run_number, out_dir)
                rows.append(row)
                print(row, flush=True)

    table = ["| Issue | pyarrow | Run | Status | Seconds | Result |", "|---|---|---|---|---|---|"]
    for r in rows:
        table.append(
            f"| #{r['issue']} | {r['version']} | {r['run']} | {r['status']} | {r['seconds']} | {r['result']} |"
        )
    summary = f"## {args.label}\n\n" + "\n".join(f"- {l}" for l in env_lines) + "\n\n" + "\n".join(table) + "\n"
    (out_dir / "summary.md").write_text(summary, encoding="utf-8")
    (out_dir / "summary.json").write_text(json.dumps({"env": env_lines, "rows": rows}, indent=1))
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(summary)
    print(summary)


if __name__ == "__main__":
    main()
