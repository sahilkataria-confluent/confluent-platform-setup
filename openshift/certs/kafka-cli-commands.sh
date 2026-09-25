# Run these from certs/ using the standard Kafka CLI tools
#
# Requires certs/generated/{cacerts.pem,client-appclient-full.pem} to exist
# (run ./generate-certs.sh first) and the cluster to already have
# ../01-confluent-platform.yaml applied, so the broker trusts this CA.

BOOTSTRAP=kafka.apps.redhat.ibm.com:443

# list topics (via mTLS)
kafka-topics.sh --bootstrap-server $BOOTSTRAP --command-config client-ssl.properties --list

# create a topic
kafka-topics.sh --bootstrap-server $BOOTSTRAP --command-config client-ssl.properties --create --topic test --partitions 3 --replication-factor 3

# produce (type lines, Ctrl-D to stop)
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --producer.config client-ssl.properties --topic test

# consume
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --consumer.config client-ssl.properties --topic test --from-beginning
