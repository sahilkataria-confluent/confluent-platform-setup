#!/usr/bin/env bash
# Generates a self-signed root CA and server certs for KRaftController,
# Kafka, Schema Registry, Control Center, and CMF. Connect stays
# plaintext - its CRD rejects spec.listeners outright, see
# 01-confluent-platform.yaml. Schema Registry gets TLS via its own
# configOverrides (native dual http/https listener)
#
# Output layout (all under ./generated, gitignored - these are private keys):
#   ca-key.pem, cacerts.pem            <- your CA (cacerts.pem doubles as
#                                         the PEM truststore - just the CA cert)
#   kraftcontroller-key.pem, kraftcontroller-server.pem  <- controller listener cert
#   kafka-key.pem, kafka-server.pem      <- Kafka's mTLS listener cert
#   schemaregistry-key.pem, schemaregistry-server.pem  <- Schema Registry's https listener cert (PEM)
#   sr-keystore.jks, sr-truststore.jks, sr-jksPassword.txt  <- same cert,
#                                         JKS + Properties-format password file
#                                         (loaded via spec.mountedSecrets, see below)
#   controlcenter-key.pem, controlcenter-server.pem  <- Control Center's cert
#   cmf-key.pem, cmf-server.pem          <- CMF's cert (PEM)
#   cmf-keystore.jks, cmf-truststore.jks <- CMF's cert, JKS - its Helm chart's
#                                         cmf.ssl fields require JKS specifically,
#                                         this is the one exception to "PEM only"
#                                         in this directory, not a style choice
#   client-appclient.pem, client-appclient-key.pem  <- example client cert
#   client-appclient-full.pem            <- same cert+key combined (Kafka's PEM
#                                         keystore loader needs both in one file)
#
# Also writes 4 files into ../flink/ for CMF's Flink catalog/database over
# Kafka's mTLS external listener: catalog.json/database.json hold only the
# non-secret connection URL + a connectionSecretId reference, while
# catalog-secret.json/database-secret.json hold the actual cert/key
# material, meant to be pasted in as a CMF Secret and exposed to the
# environment before the catalog/database reference it. The *-secret.json
# files are gitignored (../flink/.gitignore) even though flink/ itself is
# meant to be published - only those two files embed a private key.
set -euo pipefail

NAMESPACE="confluent"
SVC_DOMAIN="svc.cluster.local"
ROUTE_DOMAIN="apps.redhat.ibm.com"
DAYS=3650
STORE_PASSWORD="${STORE_PASSWORD:-confluentpass}"
OUT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/generated"

mkdir -p "$OUT"
cd "$OUT"

echo "==> Generating root CA"
openssl genrsa -out ca-key.pem 2048
openssl req -x509 -new -nodes -key ca-key.pem -days "$DAYS" \
  -out cacerts.pem \
  -subj "/C=US/ST=CA/L=SF/O=Confluent/OU=Kafka/CN=kafka-ca"

# $1 = component name, $2 = CN, $3... = extra SAN DNS entries (beyond the
# standard <name>, <name>.confluent.svc.cluster.local, *.<name>.confluent.svc.cluster.local)
gen_server_cert() {
  local name="$1" cn="$2"
  shift 2
  local extra_sans=("$@")

  echo "==> Generating server cert for $name (CN=$cn)"
  openssl genrsa -out "${name}-key.pem" 2048
  openssl req -new -key "${name}-key.pem" -out "${name}.csr" -subj "/CN=${cn}"

  {
    echo "basicConstraints=CA:FALSE"
    echo "keyUsage=digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=serverAuth,clientAuth"
    printf 'subjectAltName=DNS:%s,DNS:%s.%s.%s,DNS:*.%s.%s.%s' \
      "$name" "$name" "$NAMESPACE" "$SVC_DOMAIN" "$name" "$NAMESPACE" "$SVC_DOMAIN"
    if [ "${#extra_sans[@]}" -gt 0 ]; then
      for san in "${extra_sans[@]}"; do
        printf ',%s' "$san"
      done
    fi
    echo
  } > "${name}-ext.cnf"

  openssl x509 -req -in "${name}.csr" -CA cacerts.pem -CAkey ca-key.pem -CAcreateserial \
    -out "${name}-server.pem" -days "$DAYS" -extfile "${name}-ext.cnf"

  rm -f "${name}.csr" "${name}-ext.cnf"
}

gen_server_cert kraftcontroller kraftcontroller

# Kafka's cert needs to work both for in-cluster service DNS (used by the
# plaintext listener's peers too, harmless) and the OpenShift Route
# hostnames for the mTLS external listener:
#   bootstrap -> kafka.<ROUTE_DOMAIN>            (bootstrapPrefix defaults to cluster name "kafka")
#   brokers   -> b0.<ROUTE_DOMAIN>, b1..., b2...  (brokerPrefix defaults to "b", 3 replicas)
gen_server_cert kafka kafka \
  "DNS:kafka.${ROUTE_DOMAIN}" \
  "DNS:b0.${ROUTE_DOMAIN}" "DNS:b1.${ROUTE_DOMAIN}" "DNS:b2.${ROUTE_DOMAIN}" \
  "DNS:*.${ROUTE_DOMAIN}"

# Schema Registry's own dual http/https listener config (no Route/
# externalAccess here - see 01-confluent-platform.yaml) just needs
# in-cluster service DNS coverage.
gen_server_cert schemaregistry schemaregistry

# spec.tls.secretRef only mounts certs when a listeners.*.tls.enabled
# block is present, which we're deliberately not using (see
# 01-confluent-platform.yaml) - confirmed by "KeyStore Path not
# accessible" once that was tried. So this builds its own JKS
# keystore/truststore + a Properties-format password file, loaded via
# spec.mountedSecrets instead (a plain "mount this secret at a fixed
# path" mechanism, independent of the tls/listeners machinery).
echo "==> Generating Schema Registry keystore.jks / truststore.jks (password: ${STORE_PASSWORD})"
rm -f schemaregistry.p12 sr-keystore.jks sr-truststore.jks sr-jksPassword.txt
openssl pkcs12 -export \
  -in schemaregistry-server.pem -inkey schemaregistry-key.pem \
  -out schemaregistry.p12 -name schemaregistry -passout "pass:${STORE_PASSWORD}"
keytool -importkeystore -noprompt \
  -srckeystore schemaregistry.p12 -srcstoretype PKCS12 -srcstorepass "$STORE_PASSWORD" \
  -destkeystore sr-keystore.jks -deststoretype JKS \
  -deststorepass "$STORE_PASSWORD" -destkeypass "$STORE_PASSWORD"
keytool -importcert -noprompt -trustcacerts -alias caroot \
  -file cacerts.pem -keystore sr-truststore.jks -storepass "$STORE_PASSWORD"
rm -f schemaregistry.p12
# FileConfigProvider reads this as a java.util.Properties file, not a
# raw string - it needs a key=value line, not just the bare password.
echo "jksPassword=${STORE_PASSWORD}" > sr-jksPassword.txt

# Control Center's route uses the default prefix "controlcenter":
#   controlcenter.<ROUTE_DOMAIN>
gen_server_cert controlcenter controlcenter "DNS:controlcenter.${ROUTE_DOMAIN}"

# CMF's Helm chart always names its Service "cmf-service" regardless of
# release name (confirmed via `helm template`), plus its own Route hostname.
gen_server_cert cmf cmf \
  "DNS:cmf-service" "DNS:cmf-service.operator.svc.cluster.local" "DNS:*.cmf-service.operator.svc.cluster.local" \
  "DNS:cmf.${ROUTE_DOMAIN}"

# CMF's Helm chart wants JKS, not PEM/PKCS12 - same cert, just repackaged.
echo "==> Generating CMF keystore.jks / truststore.jks (password: ${STORE_PASSWORD})"
rm -f cmf.p12 cmf-keystore.jks cmf-truststore.jks
openssl pkcs12 -export \
  -in cmf-server.pem -inkey cmf-key.pem \
  -out cmf.p12 -name cmf -passout "pass:${STORE_PASSWORD}"
keytool -importkeystore -noprompt \
  -srckeystore cmf.p12 -srcstoretype PKCS12 -srcstorepass "$STORE_PASSWORD" \
  -destkeystore cmf-keystore.jks -deststoretype JKS \
  -deststorepass "$STORE_PASSWORD" -destkeypass "$STORE_PASSWORD"
keytool -importcert -noprompt -trustcacerts -alias caroot \
  -file cacerts.pem -keystore cmf-truststore.jks -storepass "$STORE_PASSWORD"
rm -f cmf.p12

# Example client cert for testing Kafka's mTLS listener from your laptop.
# CN becomes the authenticated principal (RULE:.*CN=... in the Kafka CR).
gen_client_cert() {
  local name="$1" cn="$2"
  echo "==> Generating client cert for $name (CN=$cn)"
  openssl genrsa -out "client-${name}-key.pem" 2048
  openssl req -new -key "client-${name}-key.pem" -out "client-${name}.csr" -subj "/CN=${cn}"
  {
    echo "basicConstraints=CA:FALSE"
    echo "keyUsage=digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=clientAuth"
  } > "client-${name}-ext.cnf"
  openssl x509 -req -in "client-${name}.csr" -CA cacerts.pem -CAkey ca-key.pem -CAcreateserial \
    -out "client-${name}.pem" -days "$DAYS" -extfile "client-${name}-ext.cnf"
  rm -f "client-${name}.csr" "client-${name}-ext.cnf"

  # Kafka's PEM keystore loader (ssl.keystore.type=PEM) wants the cert AND
  # the private key in the ONE file pointed to by ssl.keystore.location -
  # there's no separate "ssl.key.location" config. Build that combined file.
  cat "client-${name}.pem" "client-${name}-key.pem" > "client-${name}-full.pem"
}

gen_client_cert appclient appclient

# CMF catalog/database + secret JSON for testing Flink SQL against Kafka's
# mTLS EXTERNAL listener (the Route) instead of the plaintext internal one.
# Split so only the *-secret.json files carry cert/key material:
#   catalog.json / database.json         -> connection URL + connectionSecretId
#   catalog-secret.json / database-secret.json -> the actual SSL/PEM properties
FLINK_DIR="$(cd "$OUT/../.." && pwd)/flink"
echo "==> Generating flink/{catalog,database}.json + *-secret.json"
python3 -c "
import json

ca = open('cacerts.pem').read()
chain = open('client-appclient.pem').read()
key = open('client-appclient-key.pem').read()

# Kafka's external mTLS listener (Route) requires a client cert.
database = {
    'bootstrap.servers': 'kafka.${ROUTE_DOMAIN}:443'
}
database_secret = {
    'security.protocol': 'SSL',
    'ssl.truststore.type': 'PEM',
    'ssl.keystore.type': 'PEM',
    'ssl.truststore.certificates': ca,
    'ssl.keystore.certificate.chain': chain,
    'ssl.keystore.key': key,
}

# Schema Registry's https listener (8082) only needs server-cert trust -
# no client cert is enforced there - but one's included anyway since
# it's harmless and keeps one identity uniform across both connections.
catalog = {
    'schema.registry.url': 'https://schemaregistry.confluent.svc.cluster.local:8082'
}
catalog_secret = {
    'schema.registry.security.protocol': 'SSL',
    'schema.registry.ssl.truststore.type': 'PEM',
    'schema.registry.ssl.keystore.type': 'PEM',
    'schema.registry.ssl.truststore.certificates': ca,
    'schema.registry.ssl.keystore.certificate.chain': chain,
    'schema.registry.ssl.keystore.key': key,
}

out = '$FLINK_DIR'
for fname, obj in [
    ('catalog.json', catalog),
    ('catalog-secret.json', catalog_secret),
    ('database.json', database),
    ('database-secret.json', database_secret),
]:
    with open(f'{out}/{fname}', 'w') as f:
        json.dump(obj, f, indent=2)
        f.write('\n')
"

echo
echo "==> Done. Files are in: $OUT and $FLINK_DIR"
echo "    Run ../create-secrets.sh next to load these into Kubernetes secrets."
