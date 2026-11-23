"""Run each script in cases.json against the latest pyarrow and the reported version(s).

R scripts (cases with "lang": "R") run once with the Rscript on PATH.

Every run gets a fresh working directory and its own log file. A failing or
hanging script is a result, not an error: the driver always carries on and
exits 0 unless the driver itself breaks.
"""

import argparse
import json
import os
import platform
import shutil
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


def run_r_once(case, run_number, out_dir):
    """Run an R script with the Rscript on PATH (arrow from CRAN, installed by the workflow)."""
    script = HERE / case["script"]
    name = case.get("name", str(case["issue"]))
    log = out_dir / f"GH-{name}__R__run{run_number}.log"
    row = {"issue": case["issue"], "name": name, "version": "R (CRAN)", "run": run_number}
    rscript = shutil.which("Rscript")
    if rscript is None:
        row.update(status="SKIPPED", seconds=0, result="no Rscript on this runner")
        return row
    cmd = [rscript, str(script)]
    timeout = case.get("timeout", DEFAULT_TIMEOUT)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as cwd:
        start = time.time()
        output, status = run_with_timeout(cmd, cwd, timeout)
    elapsed = time.time() - start
    results = [l[len("RESULT:"):].strip() for l in output.splitlines() if l.startswith("RESULT:")]
    log.write_text(
        f"# issue: {case['issue']}\n# command: {' '.join(cmd)}\n"
        f"# status: {status}\n# seconds: {elapsed:.1f}\n\n{output}",
        encoding="utf-8",
    )
    row.update(status=status, seconds=round(elapsed, 1),
               result="; ".join(results) if results else "(no RESULT line; see log)")
    return row


def run_once(case, version, run_number, out_dir):
    script = HERE / case["script"]
    name = case.get("name", str(case["issue"]))
    python = LATEST_PYTHON if version == "latest" else REPORTED_PYTHON
    # Very old releases have no wheels for Python 3.11; a case can name the
    # Python to use for a given pyarrow version.
    python = case.get("python_for", {}).get(version, python)
    uv_run = [
        sys.executable, "-m", "uv", "run", "--no-project", "--quiet",
        # Wheels only: never try to build pyarrow from its sdist.
        "--no-build",
        "--python", python,
        *packages(version, case.get("extra", [])),
    ]
    cmd = [*uv_run, str(script)]
    timeout = case.get("timeout", DEFAULT_TIMEOUT)
    log = out_dir / f"GH-{name}__pyarrow-{version}__run{run_number}.log"
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as cwd:
        # Build the environment first, so downloads and installs do not count
        # towards the script's timeout or its timing. This must not import
        # pyarrow: for some issues the import itself is what is being tested.
        warm_out, warm_status = run_with_timeout(
            [*uv_run, "python", "-c", "pass"], cwd, INSTALL_TIMEOUT
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
        "issue": case["issue"], "name": name, "version": version, "run": run_number,
        "status": status, "seconds": round(elapsed, 1),
        "result": "; ".join(results) if results else "(no RESULT line; see log)",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="out")
    parser.add_argument("--label", default=platform.system())
    parser.add_argument("--only", type=int, action="append", help="issue number(s) to run")
    parser.add_argument("--cases", default="cases.json", help="cases file, relative to this script")
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

    cases = json.loads((HERE / args.cases).read_text())
    rows = []

    def write_summary():
        table = ["| Issue | Version | Run | Status | Seconds | Result |", "|---|---|---|---|---|---|"]
        for r in rows:
            table.append(
                f"| #{r['name']} | {r['version']} | {r['run']} | {r['status']} | {r['seconds']} | {r['result']} |"
            )
        text = f"## {args.label}\n\n" + "\n".join(f"- {l}" for l in env_lines) + "\n\n" + "\n".join(table) + "\n"
        (out_dir / "summary.md").write_text(text, encoding="utf-8")
        (out_dir / "summary.json").write_text(json.dumps({"env": env_lines, "rows": rows}, indent=1))
        return text

    def record(row):
        # Rewrite the summary after every run, so a cancelled job still leaves one.
        rows.append(row)
        print(row, flush=True)
        write_summary()

    # The workflow sets R_CASES=1 only on runners where it installed R and arrow.
    r_enabled = os.environ.get("R_CASES") == "1" or not os.environ.get("GITHUB_ACTIONS")

    # ONLY_CASES is a comma-separated list of case names (the "name" field, or
    # the issue number when a case has no name). Empty means run everything.
    only_names = {n.strip() for n in os.environ.get("ONLY_CASES", "").split(",") if n.strip()}

    for case in cases:
        if args.only and case["issue"] not in args.only:
            continue
        if "only_on" in case and platform.system() not in case["only_on"]:
            continue
        name = case.get("name", str(case["issue"]))
        if only_names and name not in only_names:
            continue
        repeats = range(1, case.get("repeats", 1) + 1)
        if case.get("lang") == "R":
            if not r_enabled:
                record({"issue": case["issue"], "name": name, "version": "R (CRAN)", "run": 1,
                        "status": "SKIPPED", "seconds": 0, "result": "R is not set up on this runner"})
                continue
            for run_number in repeats:
                record(run_r_once(case, run_number, out_dir))
            continue
        versions = [v for v in case.get("versions", ["latest", case.get("reported_version")]) if v]
        for version in versions:
            for run_number in repeats:
                record(run_once(case, version, run_number, out_dir))

    summary = write_summary()
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(summary)
    print(summary)


if __name__ == "__main__":
    main()
