#!/usr/bin/env bash
NAMESPACE=${NAMESPACE:-examples}
kubectl delete -f pravega-benchmark.yaml -n ${NAMESPACE}
kubectl apply -f pravega-benchmark.yaml -n ${NAMESPACE}
sleep 5s
kubectl logs -f jobs/pravega-benchmark -n ${NAMESPACE}
