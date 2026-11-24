# Follow-up diagnostic for https://github.com/apache/arrow/issues/38812.
# NOT the reporter's code.
#
# On Windows, after pyarrow.csv.read_csv() raises ArrowInvalid, the input file
# can stay open, so it cannot be renamed or deleted for the rest of the
# process. GH-38812-repeat.py showed that this is all-or-nothing per process,
# which fits one handle leaked early and never released. A leaked handle makes
# every later check in the same process fail, so this script runs every
# condition in its OWN child process (this same file, re-invoked with
# `--child <scenario> <directory>`), each with its own freshly written CSV.
#
# In each child, after every read, the file is renamed away and back. On
# Windows a refused rename means some handle on the file is still open. On
# Linux and macOS an open file can always be renamed, so nothing is refused
# there; those runs are controls (the psutil handle counts still mean
# something on them).
#
# Scenarios (N_READS reads each; the read is meant to fail unless stated):
#   baseline      read_csv(path, ReadOptions(block_size=65536)); the rename
#                 check runs inside the `except` block, exactly as in
#                 GH-38812-repeat.py. Run 5 times, in 5 separate children, to
#                 see whether the same condition flips between processes.
#   threads_false ReadOptions(use_threads=False)
#   threads_true  ReadOptions(use_threads=True) given explicitly
#   block_1m      block_size=1 << 20 (still too small for the long field)
#   pyfile        pass a Python file object from open(path, "rb") in a `with`
#                 block; check after the `with` block has closed it
#   osfile        pass pa.OSFile(path); close it explicitly, then check
#   mmap          pass pa.memory_map(path); close it explicitly, then check
#   io_threads_1  pa.set_io_thread_count(1) before reading
#   cpu_count_1   pa.set_cpu_count(1) before reading
#   del_exc       catch as `exc`, drop its traceback, `del exc`, clear
#                 sys.last_exc etc., leave the `except` block, then check:
#                 rules out the Python exception/traceback pinning the file
#   open_csv      streaming API: csv.open_csv(path) and iterate; drop the
#                 reader, then check
#   read_ok       control: block size large enough, read_csv SUCCEEDS
#   no_pyarrow    control: pyarrow is never imported; only the rename check
#   py_read       control: plain Python open/read/close, then the check
#   held_open     detector check: the file is deliberately held open by
#                 Python during the check. Windows should refuse the rename;
#                 this proves the check can see an open handle on the runner.
#
# On the first refused rename in a child, the check is repeated after
# gc.collect(), then after waiting 0.1 s, 1 s and 5 s more (cumulative),
# stopping as soon as the rename works.
#
# If psutil is installed, the number of handles the process has open on the
# CSV (and the process's total handle/fd/thread counts) is recorded before the
# first read, right after the first read, and at the end.
#
# A child reports after every read, so one that hangs part-way still counts.
# A child that finishes all its reads but then does not exit within
# EXIT_GRACE seconds is killed and reported as HUNG AT EXIT (seen on Linux
# with pyarrow 14.0.1 in children where psutil still saw the file open).
import faulthandler
import gc
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time

N_READS = 20
CHILD_TIMEOUT = 60
EXIT_GRACE = 10  # seconds a child gets to exit after its final report
MARK = "CHILD_JSON: "
SMALL_BLOCK = 1 << 16
BIG_BLOCK = 1 << 24  # larger than the 5,000,000 character field

# (scenario, label used in the RESULT line, what the read is expected to do)
SCENARIOS = [
    ("baseline", "baseline", "raised"),
    ("threads_false", "threads=False", "raised"),
    ("threads_true", "threads=True", "raised"),
    ("block_1m", "block=1M", "raised"),
    ("pyfile", "pyfile", "raised"),
    ("osfile", "OSFile", "raised"),
    ("mmap", "mmap", "raised"),
    ("io_threads_1", "io_threads=1", "raised"),
    ("cpu_count_1", "cpu_count=1", "raised"),
    ("del_exc", "del_exc", "raised"),
    ("open_csv", "open_csv", "raised"),
    ("read_ok", "read_ok", "succeeded"),
    ("no_pyarrow", "no_pyarrow", "no_read"),
    ("py_read", "py_read", "no_read"),
    ("held_open", "held_open", "no_read"),
]
LABELS = {name: label for name, label, _ in SCENARIOS}
EXPECTED = {name: expected for name, _, expected in SCENARIOS}
BASELINE_RUNS = 5


# --------------------------------------------------------------------------
# Child: one scenario, one process, one file
# --------------------------------------------------------------------------

def write_csv(path):
    with open(path, "w") as f:
        f.write("a,b\n")
        f.write("1," + "x" * 5_000_000 + "\n")
        f.write("2,short\n")


def same_path(a, b):
    try:
        if os.path.samefile(a, b):
            return True
    except OSError:
        pass
    norm = lambda p: os.path.normcase(os.path.realpath(p))
    return norm(a) == norm(b)


def handle_snapshot(path):
    """Handle counts from psutil, or None if psutil is not installed."""
    try:
        import psutil
    except Exception:
        return None
    snap = {}
    try:
        proc = psutil.Process()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    try:
        snap["on_file"] = len([f for f in proc.open_files() if same_path(f.path, path)])
    except Exception as exc:
        snap["on_file"] = None
        snap["on_file_error"] = f"{type(exc).__name__}: {exc}"[:120]
    try:
        snap["mapped"] = len([m for m in proc.memory_maps() if m.path and same_path(m.path, path)])
    except Exception:
        snap["mapped"] = None
    for key, attr in (("num_handles", "num_handles"), ("num_fds", "num_fds"), ("threads", "num_threads")):
        try:
            snap[key] = getattr(proc, attr)()  # num_handles: Windows only; num_fds: POSIX only
        except Exception:
            snap[key] = None
    return snap


class RenameChecker:
    """Renames the file away and back; remembers every result."""

    def __init__(self, path, follow_up=True):
        self.path = path
        self.moved = path + ".moved"
        self.follow_up = follow_up
        self.read_index = 0       # set by the caller before each read
        self.checks = 0
        self.refused_at = []      # read indexes whose check was refused
        self.first_error = None
        self.after_first = None   # what happened after the first refusal
        self.snap_after_first_read = None
        self.stranded = False     # file could not be renamed back

    def try_rename(self):
        """(True, None) if the file could be renamed away; always tries to put it back."""
        try:
            os.rename(self.path, self.moved)
        except OSError as exc:
            return False, f"{type(exc).__name__} errno={exc.errno} winerror={getattr(exc, 'winerror', None)}"
        self.put_back()
        return True, None

    def put_back(self):
        for _ in range(100):
            try:
                os.rename(self.moved, self.path)
                self.stranded = False
                return
            except FileNotFoundError:
                return
            except OSError:
                time.sleep(0.1)
        self.stranded = True

    def check(self):
        self.checks += 1
        ok, err = self.try_rename()
        if self.checks == 1:
            # After the rename attempt, so a slow psutil call cannot hide a short lock.
            self.snap_after_first_read = handle_snapshot(self.path)
        if ok:
            return
        self.refused_at.append(self.read_index)
        if len(self.refused_at) == 1:
            self.first_error = err
            if self.follow_up:
                self.after_first = self.recheck_first_refusal()

    def recheck_first_refusal(self):
        """Still refused after gc.collect()? After 0.1 s, 1 s, 5 s more? Stops when it clears."""
        steps = {}
        gc.collect()
        ok, _ = self.try_rename()
        steps["gc"] = "ok" if ok else "refused"
        for wait in (0.1, 1, 5):
            if ok:
                break
            time.sleep(wait)
            ok, _ = self.try_rename()
            steps[f"+{wait}s"] = "ok" if ok else "refused"
        steps["cleared_by"] = next((k for k, v in steps.items() if v == "ok"), "never")
        return steps


def one_read(scenario, path, check, info):
    """Do one read for the scenario and call check() exactly once.

    Returns "raised" (ArrowInvalid), "succeeded" or "no_read".
    """
    if scenario == "no_pyarrow":
        check()
        return "no_read"
    if scenario == "py_read":
        with open(path, "rb") as f:
            f.read()
        check()
        return "no_read"
    if scenario == "held_open":
        with open(path, "rb"):
            check()
        return "no_read"

    import pyarrow as pa
    from pyarrow import csv

    if scenario in ("baseline", "threads_false", "threads_true", "block_1m",
                    "io_threads_1", "cpu_count_1", "read_ok"):
        options = {
            "threads_false": dict(block_size=SMALL_BLOCK, use_threads=False),
            "threads_true": dict(block_size=SMALL_BLOCK, use_threads=True),
            "block_1m": dict(block_size=1 << 20),
            "read_ok": dict(block_size=BIG_BLOCK),
        }.get(scenario, dict(block_size=SMALL_BLOCK))
        try:
            csv.read_csv(path, read_options=csv.ReadOptions(**options))
        except pa.ArrowInvalid:
            # Checked inside the handler, as GH-38812-repeat.py does.
            check()
            return "raised"
        check()
        return "succeeded"

    read_options = csv.ReadOptions(block_size=SMALL_BLOCK)

    if scenario == "pyfile":
        outcome = "succeeded"
        with open(path, "rb") as f:
            try:
                csv.read_csv(f, read_options=read_options)
            except pa.ArrowInvalid:
                outcome = "raised"
        check()
        return outcome

    if scenario in ("osfile", "mmap"):
        source = pa.OSFile(path) if scenario == "osfile" else pa.memory_map(path)
        try:
            csv.read_csv(source, read_options=read_options)
        except pa.ArrowInvalid:
            source.close()
            del source
            check()
            return "raised"
        source.close()
        del source
        check()
        return "succeeded"

    if scenario == "del_exc":
        outcome = "succeeded"
        try:
            csv.read_csv(path, read_options=read_options)
        except pa.ArrowInvalid as exc:
            outcome = "raised"
            exc.__traceback__ = None
            del exc
        for name in ("last_exc", "last_value", "last_traceback", "last_type"):
            if hasattr(sys, name):
                setattr(sys, name, None)
        check()
        return outcome

    if scenario == "open_csv":
        outcome = "succeeded"
        reader = None
        stage = "open_csv()"
        try:
            reader = csv.open_csv(path, read_options=read_options)
            stage = "iteration"
            for _ in reader:
                pass
        except pa.ArrowInvalid:
            outcome = "raised"
            info["raised_in"] = stage
        reader = None  # drop the reader before checking
        check()
        return outcome

    raise ValueError(f"unknown scenario {scenario!r}")


def child_main(scenario, workdir):
    # If the child gets stuck, show where each Python thread is before the parent kills it.
    faulthandler.enable()
    faulthandler.dump_traceback_later(CHILD_TIMEOUT - 10, exit=False)

    out = {"scenario": scenario, "reads": N_READS, "reads_done": 0, "raised": 0, "succeeded": 0,
           "no_read": 0, "other_errors": 0}
    info = {}
    path = os.path.join(workdir, "long_field.csv")
    write_csv(path)

    def report(final):
        out["refused"] = len(checker.refused_at)
        out["refused_at"] = checker.refused_at
        out["first_refusal"] = checker.refused_at[0] if checker.refused_at else None
        out["first_error"] = checker.first_error
        out["after_first_refusal"] = checker.after_first
        out["still_refused_after_handler"] = still_refused_after_handler
        out["handles_after_first_read"] = checker.snap_after_first_read
        out["final"] = final
        out.update(info)
        # One line per read, so a child that hangs part-way still reports.
        print(MARK + json.dumps(out), flush=True)

    if scenario not in ("no_pyarrow", "py_read", "held_open"):
        import pyarrow as pa
        if scenario == "io_threads_1":
            if hasattr(pa, "set_io_thread_count"):
                pa.set_io_thread_count(1)
            else:
                info["note"] = "set_io_thread_count unavailable"
        if scenario == "cpu_count_1":
            pa.set_cpu_count(1)

    checker = RenameChecker(path, follow_up=(scenario != "held_open"))
    out["handles_before"] = handle_snapshot(path)
    still_refused_after_handler = 0

    for i in range(1, N_READS + 1):
        checker.read_index = i
        checks_before = checker.checks
        refusals_before = len(checker.refused_at)
        try:
            outcome = one_read(scenario, path, checker.check, info)
            out[outcome] += 1
        except Exception as exc:  # anything other than ArrowInvalid from the read
            out["other_errors"] += 1
            info.setdefault("first_other_error", f"{type(exc).__name__}: {exc}"[:200])
            if checker.checks == checks_before:
                checker.check()
        # If the check inside the `except` block was refused, look again now
        # that the handler has exited and the exception has been released.
        if len(checker.refused_at) > refusals_before and scenario != "held_open":
            ok, _ = checker.try_rename()
            if not ok:
                still_refused_after_handler += 1
        out["reads_done"] = i
        report(final=False)

    out["handles_end"] = handle_snapshot(path)

    if checker.stranded:
        checker.put_back()
    out["stranded"] = checker.stranded
    try:
        os.remove(checker.moved if checker.stranded else path)
        out["removed"] = True
    except OSError as exc:
        out["removed"] = False
        out["remove_error"] = f"{type(exc).__name__} errno={exc.errno} winerror={getattr(exc, 'winerror', None)}"
    report(final=True)
    faulthandler.cancel_dump_traceback_later()


# --------------------------------------------------------------------------
# Parent: run every scenario in its own child and summarise
# --------------------------------------------------------------------------

def run_child(scenario, workdir):
    """Run one child; always returns a dict with at least 'scenario' and 'status'."""
    start = time.time()
    result = {"scenario": scenario}
    log_path = workdir + ".log"  # beside, not inside, the child's directory

    def reports():
        """(parsed CHILD_JSON lines, other output lines) from the child's log so far."""
        with open(log_path, "rb") as fh:
            text = fh.read().decode("utf-8", errors="replace")
        parsed, other = [], []
        for line in text.splitlines():
            if line.startswith(MARK):
                try:
                    parsed.append(json.loads(line[len(MARK):]))
                except ValueError:
                    pass  # a line still being written
            elif line.strip():
                other.append(line)
        return parsed, other

    try:
        with open(log_path, "wb") as log:
            proc = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), "--child", scenario, workdir],
                stdout=log, stderr=subprocess.STDOUT,
            )
        killed = None
        finished_at = None  # when the child's final report was first seen
        while proc.poll() is None:
            time.sleep(0.2)
            now = time.time()
            if finished_at is None and any(r.get("final") for r in reports()[0]):
                finished_at = now
            if finished_at is not None and now - finished_at > EXIT_GRACE:
                killed = f"HUNG AT EXIT (finished all reads, then did not exit within {EXIT_GRACE}s; killed)"
            elif now - start > CHILD_TIMEOUT:
                killed = f"TIMEOUT after {CHILD_TIMEOUT}s"
            if killed:
                proc.kill()
                proc.wait(timeout=30)
        parsed, other = reports()
        if parsed:
            result.update(parsed[-1])
        if killed:
            result["status"] = killed
            if not result.get("final"):
                result["status"] += f" during read #{result.get('reads_done', 0) + 1}"
                result["output_tail"] = other[-25:]
        elif parsed and parsed[-1].get("final"):
            result["status"] = "ok" if proc.returncode == 0 else f"exit {proc.returncode} after finishing"
        else:
            result["status"] = f"CRASH (exit {proc.returncode}) during read #{result.get('reads_done', 0) + 1}"
            result["output_tail"] = other[-25:]
    except Exception as exc:
        result["status"] = f"PARENT ERROR {type(exc).__name__}: {exc}"
    result["seconds"] = round(time.time() - start, 1)
    return result


def on_file(snap):
    if snap is None:
        return "-"
    value = snap.get("on_file")
    return "?" if value is None else str(value)


def describe(tag, r):
    """One human-readable line for one child."""
    if "refused" not in r:
        line = f"[{tag}] {r.get('status')} ({r.get('seconds')}s)"
        for l in r.get("output_tail", []):
            line += f"\n      | {l}"
        return line
    line = (f"[{tag}] reads {r['reads_done']} of {r['reads']}: raised {r['raised']}, succeeded {r['succeeded']}, "
            f"no pyarrow read {r['no_read']}, other errors {r['other_errors']} | "
            f"rename refused {r['refused']}")
    if r["refused"]:
        line += f" (first at read #{r['first_refusal']}, {r['first_error']}"
        if r.get("after_first_refusal"):
            steps = {k: v for k, v in r["after_first_refusal"].items() if k != "cleared_by"}
            line += f"; re-checks {steps}"
        line += f"; still refused once the except block had exited: {r['still_refused_after_handler']})"
    if r.get("final"):
        line += f" | file removed at end: {'yes' if r['removed'] else 'NO ' + str(r.get('remove_error'))}"
    if r.get("stranded"):
        line += " | COULD NOT RENAME BACK"
    line += (f" | psutil handles on file before/after first read/end: "
             f"{on_file(r.get('handles_before'))}/{on_file(r.get('handles_after_first_read'))}/{on_file(r.get('handles_end'))}")
    threads = [str((r.get(k) or {}).get("threads", "-")) for k in ("handles_before", "handles_after_first_read", "handles_end")]
    line += f", threads {'/'.join(threads)}"
    for key in ("raised_in", "note", "first_other_error"):
        if r.get(key):
            line += f" | {key}: {r[key]}"
    if r["status"] != "ok":
        line += f" | CHILD STATUS: {r['status']}"
    line += f" | {r['seconds']}s"
    for l in r.get("output_tail", []):
        line += f"\n      | {l}"
    return line


def unexpected(r):
    """Text if the reads did not do what the scenario is meant to do, else ''."""
    expected = EXPECTED[r["scenario"]]
    if r.get(expected) == r.get("reads_done"):
        return ""
    return f"UNEXPECTED {r['raised']} raised/{r['succeeded']} ok/{r['other_errors']} other"


def short_status(r):
    """'' for a clean child, else a short word for the RESULT line."""
    status = r["status"]
    if status == "ok":
        return ""
    if status.startswith("HUNG AT EXIT"):
        return "HUNG-AT-EXIT"
    word = status.split(" ")[0]
    return f"{word}@read#{r.get('reads_done', 0) + 1}" if "refused" in r and not r.get("final") else word


def summarise(results):
    parts = []
    reported = [r for _, r in results if "refused" in r]

    base = [r for _, r in results if r["scenario"] == "baseline"]
    base_ok = [r for r in base if "refused" in r]
    leaked = [r for r in base_ok if r["refused"]]
    text = f"baseline x{len(base)}: leaked in {len(leaked)} of {len(base_ok)} runs"
    if leaked:
        text += " (first refusal at read #" + ",".join(str(r["first_refusal"]) for r in leaked)
        text += "; refused " + ",".join(f"{r['refused']}/{r['reads_done']}" for r in leaked) + ")"
    odd = [short_status(r) for r in base if short_status(r)]
    if odd:
        text += " (" + ", ".join(f"{odd.count(w)} {w}" for w in sorted(set(odd))) + ")"
    bad = [unexpected(r) for r in base_ok if unexpected(r)]
    if bad:
        text += f" ({bad[0]})"
    parts.append(text)

    detector = None
    for _, r in results:
        name = r["scenario"]
        if name == "baseline":
            continue
        if "refused" not in r:
            parts.append(f"{LABELS[name]} {short_status(r)}")
            continue
        if name == "held_open":
            detector = r
            continue
        text = f"{LABELS[name]} {r['refused']}/{r['reads_done']}"
        if r["refused"]:
            text += f"@{r['first_refusal']}"
        if unexpected(r):
            text += f" {unexpected(r)}"
        if short_status(r):
            text += f" {short_status(r)}"
        parts.append(text)

    if detector is not None:
        if detector["reads_done"] and detector["refused"] == detector["reads_done"]:
            parts.append("check works (file held open by Python: refused every time)")
        elif platform.system() == "Windows":
            parts.append(f"CHECK DOUBTFUL (held-open file refused only {detector['refused']}/{detector['reads_done']})")
        else:
            parts.append(f"held-open file refused {detector['refused']}/{detector['reads_done']} (not Windows: expected 0)")

    leaking = [r for r in reported if r["refused"] and r["scenario"] != "held_open"]
    if leaking:
        cleared = sorted({(r.get("after_first_refusal") or {}).get("cleared_by", "?") for r in leaking})
        parts.append("first refusal cleared by: " + "/".join(cleared))
        after = sum(r["still_refused_after_handler"] for r in leaking)
        parts.append(f"still refused outside except block: {after}")
    not_removed = [r for r in reported if r.get("final") and not r["removed"]]
    parts.append(f"files not removable at end: {len(not_removed)}")
    # held_open is left out: it holds a handle on purpose.
    counted = [(r, max(s["on_file"] for s in snaps))
               for r in reported if r["scenario"] != "held_open"
               for snaps in [[s for s in (r.get("handles_after_first_read"), r.get("handles_end"))
                              if s and s.get("on_file") is not None]]
               if snaps]
    if counted:
        with_handles = [n for _, n in counted if n]
        parts.append(f"psutil saw a handle still open on the file in {len(with_handles)} of {len(counted)} children"
                     + (f" (max {max(with_handles)})" if with_handles else ""))
    else:
        parts.append("psutil handle counts unavailable")

    line = "refused renames/reads: " + "; ".join(parts)
    if len(line) > 590:
        line = line[:587] + "..."
    return line


def parent_main():
    results = []
    try:
        try:
            import pyarrow as pa
            version = pa.__version__
        except Exception as exc:
            version = f"IMPORT FAILED ({type(exc).__name__}: {exc})"
        try:
            import psutil
            psutil_version = psutil.__version__
        except Exception:
            psutil_version = "not installed"
        print(f"pyarrow {version} | Python {sys.version.split()[0]} | {platform.platform()} | "
              f"psutil {psutil_version}", flush=True)

        base_dir = tempfile.mkdtemp(prefix="gh38812-", dir=os.getcwd())
        # Baseline three times first and twice last, the rest once each.
        plan = [("baseline", f"baseline #{i}") for i in (1, 2, 3)]
        plan += [(name, name) for name, _, _ in SCENARIOS if name != "baseline"]
        plan += [("baseline", f"baseline #{i}") for i in range(4, BASELINE_RUNS + 1)]

        for number, (scenario, tag) in enumerate(plan, 1):
            workdir = os.path.join(base_dir, f"{number:02d}-{scenario}")
            os.makedirs(workdir, exist_ok=True)
            r = run_child(scenario, workdir)
            results.append((tag, r))
            print(describe(tag, r), flush=True)

        shutil.rmtree(base_dir, ignore_errors=True)
        if os.path.exists(base_dir):
            left = [os.path.join(d, f) for d, _, files in os.walk(base_dir) for f in files]
            print(f"could not delete {len(left)} file(s) from the parent either: {left[:5]}", flush=True)
        print("In the RESULT line, `x/20@k` means x of 20 rename checks were refused, the first at read #k.")
        print("RESULT: " + summarise(results), flush=True)
    except BaseException as exc:  # the RESULT line must always be printed
        try:
            partial = summarise(results) if results else "no child finished"
        except Exception:
            partial = "summary failed"
        print(f"RESULT: diagnostic script itself failed ({type(exc).__name__}: {exc}); {partial}"[:600], flush=True)
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--child":
        child_main(sys.argv[2], sys.argv[3])
    else:
        parent_main()
