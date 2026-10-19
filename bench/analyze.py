#!/usr/bin/env python3
"""Turn the result files written by bench/run.sh into a p50 / p95 / max table.

Usage:  python3 bench/analyze.py <label> [<label> ...]

Each label reads bench/results/<label>.<scenario>.log (the controller's job_event log lines)
and bench/results/<label>-handoff.txt (worker exit timestamps). Passing two labels,
e.g. `before after`, prints them side by side.
"""
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.environ.get("OUT", os.path.join(HERE, "results"))


def percentile(sorted_xs, q):
    """Linear-interpolated percentile (q in 0..100) of an already sorted list."""
    if not sorted_xs:
        return float("nan")
    k = (len(sorted_xs) - 1) * q / 100
    lo = int(k)
    hi = min(lo + 1, len(sorted_xs) - 1)
    return sorted_xs[lo] + (sorted_xs[hi] - sorted_xs[lo]) * (k - lo)


def fmt(xs):
    if not xs:
        return "n/a"
    xs = sorted(xs)
    return f"{percentile(xs, 50):.2f} / {percentile(xs, 95):.2f} / {xs[-1]:.2f} (n={len(xs)})"


def log_files(label):
    """<label>.log (older runs) and every <label>.<scenario>.log written by run.sh."""
    return sorted(
        os.path.join(RESULTS, f)
        for f in os.listdir(RESULTS)
        if f == f"{label}.log" or (f.startswith(f"{label}.") and f.endswith(".log"))
    )


def load_events(label):
    """{job: {event: ts_ms}} using the first occurrence of each event."""
    events = {}
    for path in log_files(label):
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                if d.get("msg") == "job_event":
                    events.setdefault(d["job"], {}).setdefault(d["event"], d["ts"])
    return events


def metrics(label):
    ev = load_events(label)
    out = {"kill": [], "run": [], "dag": [], "handoff": [], "kubelet": [], "ctrl": []}

    rounds = sorted({int(j.split("-r")[1].split("-")[0]) for j in ev if j.startswith(f"d-{label}-r")}) if ev else []
    for r in rounds:
        for i in range(1, 6):
            up = ev.get(f"d-{label}-r{r}-c{i}", {})
            down = ev.get(f"d-{label}-r{r}-c{i + 1}", {})
            if "DONE" in up and "SUBMITTED" in down:
                out["dag"].append((down["SUBMITTED"] - up["DONE"]) / 1000)

    prefix = f"e-{label}-r"
    for r in sorted({int(j[len(prefix):].split("-")[0]) for j in ev if j.startswith(prefix)}):
        p = f"{prefix}{r}"
        for victim in ("a", "b", "small"):
            e = ev.get(f"{p}-{victim}", {})
            if "EVICTED" in e and "KILL_CONFIRMED" in e:
                out["kill"].append((e["KILL_CONFIRMED"] - e["EVICTED"]) / 1000)
        u = ev.get(f"{p}-urgent", {})
        if "READY" in u and "RUNNING" in u:
            out["run"].append((u["RUNNING"] - u["READY"]) / 1000)

    # Slot hand-off: the j-th worker to exit frees a slot that the (K+j)-th submission
    # takes, where K children fit at once before anything has finished.
    path = os.path.join(RESULTS, f"{label}-handoff.txt")
    if os.path.exists(path):
        exits = []
        for line in open(path, encoding="utf-8"):
            name, ms = line.split()
            exits.append(int(ms))
        submits = sorted(
            e["SUBMITTED"] for j, e in ev.items() if j.startswith(f"h-{label}-c") and "SUBMITTED" in e
        )
        exits.sort()
        if exits and submits:
            k = sum(1 for s in submits if s < exits[0])
            for j, f in enumerate(exits):
                if k + j < len(submits):
                    out["handoff"].append((submits[k + j] - f) / 1000)
            # Split the hand-off: worker exit -> the controller sees the pod finish (kubelet
            # and API server), and that moment -> the next job SUBMITTED (the controller).
            released = sorted(
                e["POD_RELEASED"] for j, e in ev.items() if j.startswith(f"h-{label}-c") and "POD_RELEASED" in e
            )
            for j, f in enumerate(exits):
                if j < len(released):
                    out["kubelet"].append((released[j] - f) / 1000)
                if k + j < len(submits) and j < len(released):
                    out["ctrl"].append((submits[k + j] - released[j]) / 1000)
    return out


def main(labels):
    if not labels:
        sys.exit(__doc__)
    data = {l: metrics(l) for l in labels}
    rows = [
        ("kill", "Evicted pod gone, capacity usable again (`EVICTED` -> `KILL_CONFIRMED`)"),
        ("run", "Realtime job that needs an eviction: `READY` -> `RUNNING`"),
        ("dag", "Upstream `DONE` -> downstream `SUBMITTED`"),
        ("handoff", "Full cluster: worker exit -> next waiting job `SUBMITTED` (total)"),
        ("kubelet", "&nbsp;&nbsp;of which: worker exit -> controller sees the pod finished (kubelet + API server)"),
        ("ctrl", "&nbsp;&nbsp;of which: pod finished -> next job `SUBMITTED` (controller)"),
    ]
    print("| What (seconds: p50 / p95 / max) | " + " | ".join(labels) + " |")
    print("|---|" + "---|" * len(labels))
    for key, title in rows:
        print(f"| {title} | " + " | ".join(fmt(data[l][key]) for l in labels) + " |")
    if len(labels) == 2:
        a, b = (data[l] for l in labels)
        print()
        for key, title in rows:
            if a[key] and b[key]:
                print(f"{key}: p50 {statistics.median(a[key]):.3f}s -> {statistics.median(b[key]):.3f}s")


if __name__ == "__main__":
    main(sys.argv[1:])
