#!/bin/sh
# Worker entrypoint. Any failing step aborts the script (set -e) with a non-zero
# exit code, so the Kubernetes Job fails and the _SUCCESS marker is only ever
# written after every step has succeeded.
set -eu

: "${JOB_NAME:?JOB_NAME is required}"
: "${STAGE:?STAGE is required}"
: "${BUCKET:?BUCKET is required}"
: "${MARKER_PATH:?MARKER_PATH is required}"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio.minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
DURATION="${DURATION:-30}"

echo "Job $JOB_NAME starting, stage $STAGE"

mc alias set store "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --insecure

case "$STAGE" in
  1)
    echo "Stage 1: generating synthetic data"
    python3 -c "
import pandas as pd
import numpy as np
df = pd.DataFrame({
    'id': range(500000),
    'value': np.random.randn(500000),
    'category': np.random.choice(['A','B','C','D'], 500000)
})
df.to_csv('/tmp/stage1_output.csv', index=False)
print('Stage 1 done, rows:', len(df))
"
    mc cp /tmp/stage1_output.csv "store/$BUCKET/data/stage1_output.csv" --insecure
    ;;
  2)
    echo "Stage 2: feature transformation"
    # Fails (and so does the job) if stage 1's output is missing.
    mc cp "store/$BUCKET/data/stage1_output.csv" /tmp/stage1_output.csv --insecure
    python3 -c "
import pandas as pd
df = pd.read_csv('/tmp/stage1_output.csv')
result = df.groupby('category').agg({'value': ['mean','std','count']})
result.columns = ['mean','std','count']
result.reset_index().to_csv('/tmp/stage2_output.csv', index=False)
print('Stage 2 done')
"
    mc cp /tmp/stage2_output.csv "store/$BUCKET/data/stage2_output.csv" --insecure
    ;;
  3)
    echo "Stage 3: batch scoring"
    mc cp "store/$BUCKET/data/stage2_output.csv" /tmp/stage2_output.csv --insecure
    python3 -c "
import pandas as pd
df = pd.read_csv('/tmp/stage2_output.csv')
df['score'] = (df['mean'] - df['mean'].mean()) / df['mean'].std()
df.to_csv('/tmp/stage3_output.csv', index=False)
print('Stage 3 done')
"
    mc cp /tmp/stage3_output.csv "store/$BUCKET/data/stage3_output.csv" --insecure
    ;;
  background)
    echo "Background: sorting large dataset, sleeping $DURATION seconds"
    sleep "$DURATION"
    python3 -c "
import pandas as pd
import numpy as np
df = pd.DataFrame({'val': np.random.randn(1000000)})
df.sort_values('val', inplace=True)
print('Background done')
"
    ;;
  *)
    echo "Unknown stage: $STAGE" >&2
    exit 1
    ;;
esac

echo "Writing marker to MinIO"
echo "done" | mc pipe "store/$MARKER_PATH" --insecure

echo "Job $JOB_NAME complete"
