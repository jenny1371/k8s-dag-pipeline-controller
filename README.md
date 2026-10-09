# k8s-dag-pipeline-controller

A Kubernetes custom controller that orchestrates multi-stage data pipeline jobs with DAG-based dependency resolution, admission control, priority-based eviction, and S3 storage-marker completion detection.

---

## Motivation

Running ML pipelines on a shared Kubernetes cluster has two core problems:

- **Schedulers don't understand data dependencies** — Job B occupies resources spinning idle while waiting for Job A's output
- **Orchestrators don't understand resource state** — Argo/Airflow submit jobs unconditionally, causing resource contention

This controller acts as a **dependency-aware gatekeeper** between the orchestrator and the Kubernetes scheduler.

---

## Overview

**Key capabilities:**

- **DAG dependency resolution** — jobs wait until all upstream jobs are `DONE` before becoming `READY`
- **Cycle detection** — the DAG registry rejects any job spec that would create a cycle
- **Admission control** — checks real, per-node free capacity (allocatable minus the requests of all running pods, including non-PipelineJob workloads) before submitting; the worker pods carry matching `resources.requests`, so the Kubernetes scheduler enforces the same numbers
- **Priority eviction** — `realtime` jobs can preempt `batch` jobs when cluster capacity is exhausted; batch jobs leave room for waiting realtime jobs so freed capacity is not taken back
- **Storage-marker completion** — jobs are marked `DONE` only when an S3/MinIO `_SUCCESS` marker is detected; stale markers are cleared before every run, and storage errors surface as errors instead of looking like "not finished"
- **Failure handling** — a failing worker, a timeout, an invalid spec or a failed upstream moves the job to a retry or to `FAILED` with a `status.reason`
- **Timeout & retry** — configurable per-job timeout with a retry budget before permanent failure

---

## Architecture

```
PipelineJob CRD
      │
      ▼
 Reconciler (controller-runtime)
      │
      ├── DAGRegistry       — dependency graph, cycle detection
      ├── AdmissionChecker  — node allocatable vs. in-flight usage
      ├── EvictionManager   — batch job preemption for realtime priority
      └── StorageChecker    — S3/MinIO HeadObject marker polling
```

### Job State Machine

```
WAITING ──► READY ──► SUBMITTED ──► RUNNING ──► DONE
   ▲                      │            │
   │                      └────────────┤
   │                                   ├──► TIMED_OUT ──► WAITING (retry)
   │                                   └──► FAILED
   │
KILLING ──► WAITING  (eviction recovery)
```

- `SUBMITTED` — the underlying Kubernetes Job has been created.
- `RUNNING` — the underlying Job has an active pod. The timeout still counts from submission.
- `DONE` — the storage marker exists (checked in both `SUBMITTED` and `RUNNING`).
- `TIMED_OUT` / `FAILED` — if the timeout expires or the worker exits with an error, the Job is deleted and the job is retried (`TIMED_OUT` → `WAITING`) until `RetryBudget` is used up, then becomes `FAILED`. `status.reason` says why.
- Jobs that can never run fail immediately (no retry): an invalid spec (bad quantities, `storageMarker` not of the form `s3://<bucket>/<key>`), a dependency cycle, or an upstream that is `FAILED` (failures propagate down the DAG instead of leaving downstream jobs in `WAITING` forever).
- Before each submission the controller deletes the job's marker, so a `_SUCCESS` left over from an earlier run (re-applied pipeline, retry) cannot mark the new run `DONE`. The worker only writes the marker after every step has succeeded.
- `KILLING` — an evicted batch job. Its Kubernetes Job is deleted, and it returns to `WAITING` once the Job and its pods are confirmed gone, or after `KillConfirmTimeout` (120s) at the latest.

---

## Project Structure

```
.
├── api/v1/
│   ├── types.go                     # CRD type definitions
│   └── zz_generated.deepcopy.go    # DeepCopy implementations
├── internal/
│   ├── reconciler.go   # Main reconcile loop & state handlers
│   ├── dag.go          # DAGRegistry with DFS cycle detection
│   ├── admission.go    # AdmissionChecker (CPU/memory capacity)
│   ├── eviction.go     # EvictionManager (preemption, kill confirmation)
│   ├── dag_test.go     # Unit tests: cycle detection, upstream resolution
│   ├── eviction_test.go # Unit tests: KILLING -> WAITING, KillConfirmTimeout
│   ├── admission_test.go, storage_test.go, reconciler_test.go
│   └── storage.go      # StorageChecker (S3/MinIO marker polling)
├── config/
│   ├── job-image/
│   │   ├── Dockerfile           # Worker container image
│   │   └── run.sh               # Worker entrypoint script
│   ├── crd.yaml                 # CustomResourceDefinition
│   ├── rbac.yaml                # ServiceAccount, ClusterRole, ClusterRoleBinding
│   ├── minio.yaml               # MinIO deployment (local S3 backend)
│   ├── test-pipeline.yaml       # DAG pipeline scenario
│   ├── test-eviction.yaml       # Eviction scenario
│   └── test-eviction-bench.yaml
├── .github/workflows/ci.yaml   # go build / vet / test on every push
├── main.go
├── go.mod
└── go.sum
```

---

## Custom Resource: PipelineJob

```yaml
apiVersion: pipeline.io/v1
kind: PipelineJob
metadata:
  name: stage-2
  namespace: default
spec:
  priority: realtime          # realtime | batch
  requestCPU: "200m"
  requestMemory: "256Mi"
  storageMarker: "s3://test-bucket/stage2/_SUCCESS"
  timeoutSeconds: 600
  jobDurationSeconds: 45
  stage: "2"
  dependencies:
    - stage-1                 # waits until stage-1 is DONE
```

| Field | Description |
|---|---|
| `priority` | `realtime` jobs can preempt `batch` jobs when capacity is full |
| `requestCPU` / `requestMemory` | Resource request used for admission decisions |
| `storageMarker` | S3 path polled to confirm job completion |
| `timeoutSeconds` | Per-job timeout; triggers retry or permanent failure |
| `jobDurationSeconds` | Simulated workload duration passed to the worker container |
| `dependencies` | Names of upstream `PipelineJob`s that must complete first |

---

## Getting Started

### Prerequisites

- Kubernetes cluster (e.g. [kind](https://kind.sigs.k8s.io/))
- `kubectl` configured
- Docker (to build the worker image)

### 1. Apply CRD and RBAC

```bash
kubectl apply -f config/crd.yaml
kubectl apply -f config/rbac.yaml
```

### 2. Build and load the images

```bash
docker build -t pipeline-job:latest config/job-image/          # worker
docker build -t dag-pipeline-controller:latest .               # controller
kind load docker-image pipeline-job:latest
kind load docker-image dag-pipeline-controller:latest
```

> On Windows, `.gitattributes` keeps `run.sh` on LF line endings; without that the worker fails with `exec /run.sh: no such file or directory`.

### 3. Deploy MinIO and create the bucket

```bash
kubectl apply -f config/minio.yaml
kubectl -n minio rollout status deploy/minio

kubectl run mkbucket --rm -i --restart=Never   --image=pipeline-job:latest --image-pull-policy=Never --command --   sh -c 'mc alias set s http://minio.minio:9000 minioadmin minioadmin && mc mb --ignore-existing s/test-bucket'
```

> `minio/minio` is no longer published on Docker Hub, so `config/minio.yaml` uses the frozen `bitnamilegacy/minio` image, and the worker image copies `mc` from `bitnamilegacy/minio-client`. Load the MinIO image into kind too if it cannot pull from Docker Hub.

### 4. Run the controller

In the cluster (uses `MINIO_ENDPOINT=http://minio.minio:9000`):

```bash
kubectl apply -f deploy/controller-deployment.yaml
kubectl logs -f deploy/dag-pipeline-controller
```

Or locally, against a port-forwarded MinIO (default endpoint `http://localhost:9000`):

```bash
kubectl -n minio port-forward svc/minio 9000:9000 &
go run main.go
```

### 5. Apply a scenario

```bash
# 3-stage pipeline with DAG dependencies
kubectl apply -f config/test-pipeline.yaml

# Eviction scenario: batch jobs preempted by realtime
kubectl apply -f config/test-eviction.yaml
```

### 6. Watch job states

```bash
kubectl get pipelinejobs -w
```

---

## Scenarios

### DAG Pipeline (`test-pipeline.yaml`)

`stage-1` → `stage-2` → `stage-3` in sequence, plus a concurrent `background-1` batch job. Demonstrates dependency resolution and parallel execution.

### Eviction (`test-eviction.yaml`)

Two `batch` jobs take 6 of 8 CPUs (sized for an 8-CPU node; scale the requests to your cluster). A `realtime` job waits for capacity; after `EvictionThreshold` (5s), the controller evicts a batch job to free resources. The evicted job sits in `KILLING` until its Kubernetes Job and pods are gone (or `KillConfirmTimeout` expires), then re-queues to `WAITING`.

---

## Configuration Constants

| Constant | Default | Description |
|---|---|---|
| `DefaultTimeout` | 300s | Job timeout if not specified in spec |
| `RetryBudget` | 2 | Max retries before permanent `FAILED` |
| `EvictionThreshold` | 5s | How long a realtime job waits before triggering eviction |
| `MaxEvictionCount` | 3 | Max times a batch job can be evicted |
| `KillConfirmTimeout` | 120s | Max time an evicted job waits in `KILLING` for its Job/pods to disappear before re-queuing to `WAITING` |

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `http://localhost:9000` | S3/MinIO endpoint the controller uses to poll `_SUCCESS` markers |
| `MINIO_ACCESS_KEY` | `minioadmin` | Access key (also passed to workers) |
| `MINIO_SECRET_KEY` | `minioadmin` | Secret key (also passed to workers) |
| `WORKER_MINIO_ENDPOINT` | `http://minio.minio:9000` | MinIO address as seen from worker pods (differs from `MINIO_ENDPOINT` when the controller runs outside the cluster) |
| `WORKER_IMAGE` | `pipeline-job:latest` | Worker container image |
| `WORKER_IMAGE_PULL_POLICY` | `IfNotPresent` | Pull policy for the worker image |
| `LEADER_ELECT` | `false` | Enable leader election so multiple controller replicas can run (the deployment sets it to `true`) |
| `LEADER_ELECT_NAMESPACE` | `default` | Namespace of the leader-election lease when running outside the cluster |

The bucket comes from each job's `storageMarker`; nothing about the bucket is hard-coded.

### Tests

```bash
go test ./...
```

---

## Future Work

- Admission only looks at CPU/memory requests and node taints. Node selectors, affinity, tolerations, ephemeral storage and GPUs are not considered, so a job can still be admitted and then sit `Pending` in the scheduler.
- Priority-based eviction candidate selection (currently picks the first eligible batch job)
- Storage polling: markers are polled every 3 seconds because object stores have no watch. Event notifications (e.g. MinIO bucket notifications) would remove the polling.
- Credentials are passed to worker pods as plain environment variables; use a Secret (or IRSA / workload identity on AWS) outside of a demo.

### Design notes

- **No finalizer.** Deleting a `PipelineJob` needs no external cleanup: the underlying Job has an `OwnerReference` and is garbage-collected, and the controller drops the job from its in-memory DAG when it sees the deletion. A finalizer would only add a way for jobs to get stuck in `Terminating`.
- **In-memory DAG.** The DAG registry and admission mutex live in the controller process. That is safe with leader election (one active replica; the DAG is rebuilt from the `PipelineJob`s as they reconcile after a failover).

---

## Dependencies

- [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) v0.17.0
- [aws-sdk-go-v2/service/s3](https://github.com/aws/aws-sdk-go-v2) v1.48.0
- [k8s.io/api](https://github.com/kubernetes/api) v0.29.0