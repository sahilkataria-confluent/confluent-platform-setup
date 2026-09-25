# Confluent Platform on OpenShift via CFK with TLS

## Prerequisites

- An OpenShift cluster you can `oc login` to, with permission to create namespaces, secrets, and routes.
- CLI tools on your machine: `oc` (or `kubectl`), `helm`, `openssl`,
  `keytool` (ships with any JDK), `python3`.
- A Docker Hub account (username + [PAT](https://app.docker.com/settings/personal-access-tokens)) - anonymous `docker.io` pulls are rate-limited and this repo pulls several images.
- `cert-manager` will be installed for you in step 1 (FKO's admission
  webhook needs it) if you don't already have it on the cluster.
- CFK/FKO/CMF chart versions come from `parameters.env` -
  no separate install needed, `source` it as shown below.

## Before you start

Two things are baked into the manifests as literal values, not
placeholders - swap them for your own before applying:

- **Cluster apps domain**, currently `apps.redhat.ibm.com`. Find yours with:

```bash
oc get ingresses.config/cluster -o jsonpath='{.spec.domain}'
```
then:
  ```bash
  grep -rl 'apps.redhat.ibm.com' . \
    | xargs sed -i '' 's/apps\.redhat\.ibm\.com/YOUR_DOMAIN_HERE/g'
  ```
- **Docker Hub credentials** in the `kubectl create secret docker-registry`
  commands below - use your own username/PAT/email. Images are pulled
  from `docker.io`, and anonymous pulls are rate-limited.

## Setup

### 1. Prerequisites

```bash
kubectl apply -f 00-namespaces.yaml

source parameters.env   # CFK_CHART_VERSION, FKO_CHART_VERSION, CMF_CHART_VERSION

helm repo add confluentinc https://packages.confluent.io/helm
helm repo update

kubectl create secret docker-registry dockerhub-secret \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username="dockerhub-username" \
  --docker-password="dockerhub-personal-access-token" \
  --docker-email="dockerhub-user-email" \
  -n confluent

kubectl create secret docker-registry dockerhub-secret \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username="dockerhub-username" \
  --docker-password="dockerhub-personal-access-token" \
  --docker-email="dockerhub-user-email" \
  -n operator

helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  -n operator --version "$CFK_CHART_VERSION" \
  --set image.registry=docker.io \
  --set imagePullSecretRef="dockerhub-secret" \
  --set enableCMFDay2Ops=true \
  --set namespaced=true \
  --set namespaceList="{operator,confluent,flink}" \
  --set podSecurity.enabled=false
```

### 2. Generate certs

```bash
cd certs
./generate-certs.sh
# override the default password: STORE_PASSWORD=yourpassword ./generate-certs.sh
```

Covers kraftcontroller, kafka, schemaregistry, controlcenter, cmf.

### 3. Load certs into the kubernetes cluster

```bash
./create-secrets.sh
```

Creates `tls-kraftcontroller`, `tls-kafka`, `tls-controlcenter`,
`sr-ssl-jks` in `confluent`, and `cmf-day2-tls`, `cmf-keystore`,
`cmf-truststore` in `operator`.

### 4. Deploy KRaft / Kafka / Connect / Schema Registry / Control Center


```bash
cd ..
kubectl config set-context --current --namespace confluent
kubectl apply -f 01-confluent-platform.yaml

## This command may take a few minutes to deploy all the resources.
## Also if controlcenter pod is showing 2/3 availability, then try deleteing the pod.
## kubectl delete pod controlcenter-0

kubectl apply -f 02-connector.yaml

## Get all the public URLs for kafka and controlcenter
kubectl get routes

```


### 5. Verify Kafka from your laptop

```bash
cd certs
BOOTSTRAP=kafka.apps.redhat.ibm.com:443

## Make sure confluent-platform cli is available 
kafka-topics --bootstrap-server $BOOTSTRAP --command-config client-ssl.properties --list
```

More commands: `certs/kafka-cli-commands.sh`.

### 6. Deploy FKO + CMF

```bash
cd ..

# provisions FKO's own admission-webhook certs 
kubectl apply -f flink/cert-manager.yaml

helm upgrade --install cp-flink-kubernetes-operator confluentinc/flink-kubernetes-operator \
  -n operator --version "$FKO_CHART_VERSION" -f flink/fko-values.yaml

helm upgrade --install cmf confluentinc/confluent-manager-for-apache-flink \
  -n operator --version "$CMF_CHART_VERSION" -f flink/cmf-values.yaml

kubectl apply -f flink/cmf-route.yaml
kubectl apply -f flink/cmfrestclass.yaml
```

### 7. Run a Flink workload, via the CMF UI

Everything below is done by pasting JSON into CMF's UI.
Get CMF public URL from 

`kubectl get routes`
`https://cmf.apps.redhat.ibm.com`

**Create a Flink environment** - name it `flink-env`, Kubernetes
namespace `flink` (already created by `00-namespaces.yaml`).


If you want to quickly spin up Flink SQL use below commands:

**Create a Kafka catalog**, in that environment:
```json
{
  "schema.registry.url": "http://schemaregistry.confluent.svc.cluster.local:8081"
}
```

**Create a Kafka database**, in the same environment:
```json
{
  "bootstrap.servers": "kafka.confluent.svc.cluster.local:9071"
}
```

Flink SQL Deployment with TLS Enabled:

- `flink/catalog.json` / `flink/database.json` - just the connection URL.
  No secret reference goes in this JSON - the catalog/database creation
  form has its own dedicated Secret ID field for that.

- `flink/catalog-secret.json` / `flink/database-secret.json` - the actual
  `ssl.truststore.certificates` / `ssl.keystore.certificate.chain` /
  `ssl.keystore.key` PEM properties.

Wire them up in this order, once per connection (Schema Registry, then
Kafka):

1. **Create a Secret** in the environment, pasting in
   `flink/catalog-secret.json` (for the Schema Registry connection) - give
   it a name, e.g. `sr-tls-secret`.
2. **Expose that secret to the environment** (secret mapping) and name
   the mapping `sr-tls-mapping`.
3. **Create the Kafka catalog**, pasting `flink/catalog.json` into the
   JSON field, and set the form's Secret ID field to `sr-tls-mapping`.
4. Repeat 1-3 for the database: `flink/database-secret.json` as a Secret
   named `kafka-tls-secret`, mapped as `kafka-tls-mapping`, then
   `flink/database.json` in the database form with its Secret ID field
   set to `kafka-tls-mapping`.

If your CMF version's UI doesn't expose secret mapping as its own step,
look for a secret picker directly on the catalog/database creation form
instead - functionally the same link, just without an intermediate
screen.






**Create a compute pool**, in the same environment pasting in `flink/compute-pool.json`

**Run SQL**, in the environment's SQL workspace, against the compute pool above:
```sql
SHOW TABLES;
```

```sql
SELECT * FROM stocks;
```

Check the running Pods for flink SQL
kubectl get pods -n flink

`stocks` is the topic `02-connector.yaml` already creates and the datagen connector already writes to - a Kafka catalog/database surfaces existing topics as tables automatically, no DDL needed. 

## Cleanup

If you did step 7, delete the compute pool, database, and catalog from
the CMF UI first (or via `DELETE` calls against CMF's REST API) - those
are CMF-native resources, not `kubectl`-managed, so nothing below removes
them.

Reverse order of setup otherwise: Flink/CMF first, then the platform,
then certs and secrets.

```bash
# Flink / CMF
kubectl delete -f flink/cmfrestclass.yaml
kubectl delete -f flink/cmf-route.yaml
helm uninstall cmf -n operator
helm uninstall cp-flink-kubernetes-operator -n operator

# Confluent Platform
kubectl delete -f 02-connector.yaml
kubectl delete -f 01-confluent-platform.yaml

# TLS secrets
kubectl delete secret tls-kraftcontroller tls-kafka tls-controlcenter sr-ssl-jks -n confluent
kubectl delete secret cmf-day2-tls cmf-keystore cmf-truststore -n operator

# generated certs on disk (and the catalog/database secret files, which
# embed the same private key)
rm -rf certs/generated
rm -f flink/catalog.json flink/catalog-secret.json flink/database.json flink/database-secret.json
```

Leave alone unless you're tearing down everything, not just this variant
- `confluent`/`operator`/`flink` namespaces, `dockerhub-secret`,
cert-manager, and the CFK operator itself are shared with `quickstart/`
and `security/`:

```bash
helm uninstall confluent-operator -n operator
kubectl delete -f flink/cert-manager.yaml
kubectl delete secret dockerhub-secret -n confluent
kubectl delete secret dockerhub-secret -n operator
kubectl delete -f 00-namespaces.yaml
```
