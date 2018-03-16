#!/bin/bash

# set -e stops the execution of a script if a command or pipeline has an error
set -e

# modifying config.json
JQ_FILTERS_CONFIG="."

if [[ "$ENDPOINT" ]]; then
    HOST_NAME="$ENDPOINT"
fi

if [[ "$HOST_NAME" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .restEndpoints[\"$HOST_NAME\"]=\"us-east-1\""
    echo "Host name has been modified to $HOST_NAME"
    echo "Note: In your /etc/hosts file on Linux, OS X, or Unix with root permissions, make sure to associate 127.0.0.1 with $HOST_NAME"
fi

if [[ "$LOG_LEVEL" ]]; then
    if [[ "$LOG_LEVEL" == "info" || "$LOG_LEVEL" == "debug" || "$LOG_LEVEL" == "trace" ]]; then
        JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .log.logLevel=\"$LOG_LEVEL\""
        echo "Log level has been modified to $LOG_LEVEL"
    else
        echo "The log level you provided is incorrect (info/debug/trace)"
    fi
fi

if [[ "$SSL" && "$HOST_NAME" ]]; then
# This condition makes sure that the certificates are not generated twice. (for docker restart)
if [ ! -f ./ca.key ] || [ ! -f ./ca.crt ] || [ ! -f ./server.key ] || [ ! -f ./server.crt ] ; then
# Compute config for utapi tests
cat >>req.cfg <<EOF
[req]
distinguished_name = req_distinguished_name
prompt = no
req_extensions = s3_req

[req_distinguished_name]
CN = ${HOST_NAME}

[s3_req]
subjectAltName = @alt_names
extendedKeyUsage = serverAuth, clientAuth

[alt_names]
DNS.1 = *.${HOST_NAME}
DNS.2 = ${HOST_NAME}

EOF

## Generate SSL key and certificates
# Generate a private key for your CSR
openssl genrsa -out ca.key 2048
# Generate a self signed certificate for your local Certificate Authority
openssl req -new -x509 -extensions v3_ca -key ca.key -out ca.crt -days 99999  -subj "/C=US/ST=Country/L=City/O=Organization/CN=S3 CA Server"
# Generate a key for S3 Server
openssl genrsa -out server.key 2048
# Generate a Certificate Signing Request for S3 Server
openssl req -new -key server.key -out server.csr -config req.cfg
# Generate a local-CA-signed certificate for S3 Server
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 99999 -sha256 -extfile req.cfg -extensions s3_req
fi
## Update S3Server config.json
# This condition makes sure that certFilePaths section is not added twice. (for docker restart)
if ! grep -q "certFilePaths" ./config.json; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .certFilePaths= { \"key\": \".\/server.key\", \"cert\": \".\/server.crt\", \"ca\": \".\/ca.crt\" }"
fi
fi

if [[ "$LISTEN_ADDR" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .metadataDaemon.bindAddress=\"$LISTEN_ADDR\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .dataDaemon.bindAddress=\"$LISTEN_ADDR\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .listenOn=[\"$LISTEN_ADDR:8000\"]"
fi

if [[ "$DATA_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .dataClient.host=\"$DATA_HOST\""
fi

if [[ "$METADATA_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .metadataClient.host=\"$METADATA_HOST\""
fi

if [[ "$MONGODB_HOSTS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .mongodb.hosts=\"$MONGODB_HOSTS\""
fi

if [[ "$MONGODB_RS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .mongodb.replicaSet=\"$MONGODB_RS\""
fi

if [[ "$REDIS_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.host=\"$REDIS_HOST\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.port=6379"
fi

if [[ "$REDIS_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.port=$REDIS_PORT"
fi

if [[ "$RECORDLOG_ENABLED" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .recordLog.enabled=true"
fi

if [[ "$CRR_METRICS_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .backbeat.host=\"$CRR_METRICS_HOST\""
fi

if [[ "$CRR_METRICS_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .backbeat.port=$CRR_METRICS_PORT"
fi

if [[ $JQ_FILTERS_CONFIG != "." ]]; then
    jq "$JQ_FILTERS_CONFIG" config.json > config.json.tmp
    mv config.json.tmp config.json
fi

if test -v INITIAL_INSTANCE_ID && test -v S3METADATAPATH && ! test -f ${S3METADATAPATH}/uuid ; then
    echo -n ${INITIAL_INSTANCE_ID} > ${S3METADATAPATH}/uuid
fi

# s3 secret credentials for Zenko
if [ -r /run/secrets/s3-credentials ] ; then
    . /run/secrets/s3-credentials
fi

exec "$@"
