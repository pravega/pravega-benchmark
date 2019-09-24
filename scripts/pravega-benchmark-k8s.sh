#!/usr/bin/env bash
NAMESPACE=${NAMESPACE:-examples}
SCRIPT_DIR=$(dirname $0)
kubectl delete -f ${SCRIPT_DIR}/pravega-benchmark.yaml -n ${NAMESPACE}
kubectl apply -f ${SCRIPT_DIR}/pravega-benchmark.yaml -n ${NAMESPACE}
sleep 5s
kubectl logs -f jobs/pravega-benchmark -n ${NAMESPACE}
