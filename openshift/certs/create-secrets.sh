#!/usr/bin/env bash
# Loads the certs from ./generated into the Kubernetes secrets the CRs in
# ../01-confluent-platform.yaml and ../flink/ reference. Run
# ./generate-certs.sh first.
set -euo pipefail

NAMESPACE="confluent"
OPERATOR_NAMESPACE="operator"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/generated"

create_secret() {
  local component="$1" ns="$2"
  kubectl create secret generic "tls-${component}" \
    --from-file=fullchain.pem="${DIR}/${component}-server.pem" \
    --from-file=privkey.pem="${DIR}/${component}-key.pem" \
    --from-file=cacerts.pem="${DIR}/cacerts.pem" \
    --namespace "$ns" \
    --dry-run=client -o yaml | kubectl apply -f -
}

create_secret "kraftcontroller" "$NAMESPACE"
create_secret "kafka" "$NAMESPACE"
create_secret "controlcenter" "$NAMESPACE"

# Schema Registry's own dual-listener config (../01-confluent-platform.yaml)
# uses spec.mountedSecrets instead of spec.tls.secretRef - that field
# only mounts anything when a listeners.*.tls.enabled block is present,
# which we're not using here. mountedSecrets just mounts this secret at
# a fixed path (/mnt/secrets/sr-ssl-jks/), independent of that machinery.
kubectl create secret generic sr-ssl-jks -n "$NAMESPACE" \
  --from-file=keystore.jks="${DIR}/sr-keystore.jks" \
  --from-file=truststore.jks="${DIR}/sr-truststore.jks" \
  --from-file=jksPassword.txt="${DIR}/sr-jksPassword.txt" \
  --dry-run=client -o yaml | kubectl apply -f -

# CFK's CMFRestClass (../flink/cmfrestclass.yaml) expects this secret name
# and PEM key convention, in the operator namespace where it lives.
kubectl create secret generic cmf-day2-tls -n "$OPERATOR_NAMESPACE" \
  --from-file=fullchain.pem="${DIR}/cmf-server.pem" \
  --from-file=privkey.pem="${DIR}/cmf-key.pem" \
  --from-file=cacerts.pem="${DIR}/cacerts.pem" \
  --dry-run=client -o yaml | kubectl apply -f -

# CMF's own Helm-based mTLS config (../flink/cmf-values.yaml) mounts these
# as Secrets (JKS format).
kubectl create secret generic cmf-keystore -n "$OPERATOR_NAMESPACE" \
  --from-file="${DIR}/cmf-keystore.jks" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic cmf-truststore -n "$OPERATOR_NAMESPACE" \
  --from-file="${DIR}/cmf-truststore.jks" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secrets created/updated in namespace ${NAMESPACE}: tls-kraftcontroller, tls-kafka, tls-controlcenter, sr-ssl-jks"
echo "Secrets created/updated in namespace ${OPERATOR_NAMESPACE}: cmf-day2-tls, cmf-keystore, cmf-truststore"
