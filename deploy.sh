#!/bin/bash
set -e

echo "Applying CRDs..."
kubectl apply -f config/crd.yaml

echo "Applying RBAC configuration..."
kubectl apply -f config/rbac.yaml

echo "Deploying MinIO..."
kubectl apply -f config/minio.yaml

echo "Building controller image..."
docker build -t dag-pipeline-controller:latest .

echo "Deploying controller..."
kubectl apply -f deploy/controller-deployment.yaml

echo "Deployment completed successfully."
