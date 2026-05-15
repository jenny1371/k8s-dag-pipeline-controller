#!/bin/sh

echo "Job $JOB_NAME starting, stage $STAGE"

mc alias set store http://minio.minio:9000 minioadmin minioadmin --insecure

case $STAGE in
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
    mc cp /tmp/stage1_output.csv store/test-bucket/data/stage1_output.csv --insecure || true
    ;;
  2)
    echo "Stage 2: feature transformation"
    mc cp store/test-bucket/data/stage1_output.csv /tmp/stage1_output.csv --insecure || true
    python3 -c "
import pandas as pd, os
if os.path.exists('/tmp/stage1_output.csv'):
    df = pd.read_csv('/tmp/stage1_output.csv')
    result = df.groupby('category').agg({'value': ['mean','std','count']})
    result.columns = ['mean','std','count']
    result.reset_index().to_csv('/tmp/stage2_output.csv', index=False)
    print('Stage 2 done')
else:
    print('Stage 2 skipped: no input data')
" || true
    mc cp /tmp/stage2_output.csv store/test-bucket/data/stage2_output.csv --insecure || true
    ;;
  3)
    echo "Stage 3: batch scoring"
    mc cp store/test-bucket/data/stage2_output.csv /tmp/stage2_output.csv --insecure || true
    python3 -c "
import pandas as pd, os
if os.path.exists('/tmp/stage2_output.csv'):
    df = pd.read_csv('/tmp/stage2_output.csv')
    df['score'] = (df['mean'] - df['mean'].mean()) / df['mean'].std()
    df.to_csv('/tmp/stage3_output.csv', index=False)
    print('Stage 3 done')
else:
    print('Stage 3 skipped: no input data')
" || true
    mc cp /tmp/stage3_output.csv store/test-bucket/data/stage3_output.csv --insecure || true
    ;;
  background)
    echo "Background: sorting large dataset, sleeping $DURATION seconds"
    sleep $DURATION
    python3 -c "
import pandas as pd
import numpy as np
df = pd.DataFrame({'val': np.random.randn(1000000)})
df.sort_values('val', inplace=True)
print('Background done')
" || true
    ;;
esac

echo "Writing marker to MinIO"
echo "done" | mc pipe store/$MARKER_PATH --insecure

echo "Job $JOB_NAME complete"