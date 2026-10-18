# Benchmark

Reproduces the numbers in the main README's *Performance* section.

What is measured, all from the controller's own `job_event` log lines (millisecond timestamps):

| Scenario | Metric |
|---|---|
| `dag` | upstream `DONE` -> downstream `SUBMITTED`, in chains of 6 one-second jobs |
| `evict` | `EVICTED` -> `KILL_CONFIRMED` (the victim's pod is gone) and the realtime job's `READY` -> `RUNNING` |
| `handoff` | on a full cluster (1 root + 60 children of 2000m), worker exit -> the next waiting job `SUBMITTED` |

For `handoff` the worker prints a `worker_exit_ms` timestamp right after it writes its marker; the j-th exit frees a slot that the next not-yet-started submission takes. The time includes the kubelet reporting the pod as finished, which is part of the real latency.

## Run it

You need a kind cluster with an 8-CPU node, MinIO, the credentials Secret and the bucket (README steps 1-3), and the controller deployed.

```bash
# 1. Build the two versions to compare (for example the commit before the change, and HEAD).
git worktree add /tmp/before <old-commit>
docker build -t dag-pipeline-controller:before /tmp/before
docker build -t dag-pipeline-controller:after .
docker build -t pipeline-job:latest config/job-image/
kind load docker-image dag-pipeline-controller:before dag-pipeline-controller:after pipeline-job:latest

# 2. Measure each version (rounds defaults to 20)
for tag in before after; do
  kubectl set image deploy/dag-pipeline-controller controller=dag-pipeline-controller:$tag
  kubectl rollout status deploy/dag-pipeline-controller
  bench/run.sh $tag 20
done

# 3. Print the table
python3 bench/analyze.py before after
```

`bench/run.sh` deletes every `PipelineJob` in the `default` namespace before it starts and between rounds, so use a scratch cluster. One full run takes roughly 15-45 minutes depending on the version (the eviction rounds dominate).

Results are written to `bench/results/` (git-ignored).
