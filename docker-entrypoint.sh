#!/bin/bash

# set -e stops the execution of a script if a command or pipeline has an error
set -e

# modifying config.json
JQ_FILTERS_CONFIG="."

# ENDPOINT var can accept comma separated values
# for multiple endpoint locations
if [[ "$ENDPOINT" ]]; then
    IFS="," read -ra HOST_NAMES <<< "$ENDPOINT"
    for host in "${HOST_NAMES[@]}"; do
        JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .restEndpoints[\"$host\"]=\"us-east-1\""
    done
    echo "Host name has been modified to ${HOST_NAMES[@]}"
    echo "Note: In your /etc/hosts file on Linux, OS X, or Unix with root permissions, make sure to associate 127.0.0.1 with ${HOST_NAMES[@]}"
fi

if [[ "$LOG_LEVEL" ]]; then
    if [[ "$LOG_LEVEL" == "info" || "$LOG_LEVEL" == "debug" || "$LOG_LEVEL" == "trace" ]]; then
        JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .log.logLevel=\"$LOG_LEVEL\""
        echo "Log level has been modified to $LOG_LEVEL"
    else
        echo "The log level you provided is incorrect (info/debug/trace)"
    fi
fi

if [[ "$SSL" && "$HOST_NAMES" ]]; then
# This condition makes sure that the certificates are not generated twice. (for docker restart)
if [ ! -f ./ca.key ] || [ ! -f ./ca.crt ] || [ ! -f ./server.key ] || [ ! -f ./server.crt ] ; then
# Compute config for utapi tests
cat >>req.cfg <<EOF
[req]
distinguished_name = req_distinguished_name
prompt = no
req_extensions = s3_req

[req_distinguished_name]
CN = ${HOST_NAMES[0]}

[s3_req]
subjectAltName = @alt_names
extendedKeyUsage = serverAuth, clientAuth

[alt_names]
DNS.1 = *.${HOST_NAMES[0]}
DNS.2 = ${HOST_NAMES[0]}

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
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .pfsDaemon.bindAddress=\"$LISTEN_ADDR\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .listenOn=[\"$LISTEN_ADDR:8000\"]"
fi

if [[ "$REPLICATION_GROUP_ID" ]] ; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .replicationGroupId=\"$REPLICATION_GROUP_ID\""
fi

if [[ "$DATA_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .dataClient.host=\"$DATA_HOST\""
fi

if [[ "$METADATA_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .metadataClient.host=\"$METADATA_HOST\""
fi

if [[ "$PFSD_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .pfsClient.host=\"$PFSD_HOST\""
fi

if [[ "$MONGODB_HOSTS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .mongodb.replicaSetHosts=\"$MONGODB_HOSTS\""
fi

if [[ "$MONGODB_RS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .mongodb.replicaSet=\"$MONGODB_RS\""
fi

if [[ "$MONGODB_DATABASE" ]]; then
   JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .mongodb.database=\"$MONGODB_DATABASE\""
fi

if [ -z "$REDIS_HA_NAME" ]; then
    REDIS_HA_NAME='mymaster'
fi

if [[ "$REDIS_SENTINELS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.name=\"$REDIS_HA_NAME\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.sentinels=\"$REDIS_SENTINELS\""
elif [[ "$REDIS_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.host=\"$REDIS_HOST\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.port=6379"
fi

if [[ "$REDIS_PORT" ]] && [[ ! "$REDIS_SENTINELS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.port=$REDIS_PORT"
fi

if [[ "$REDIS_SENTINELS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.name=\"$REDIS_HA_NAME\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.sentinels=\"$REDIS_SENTINELS\""
elif [[ "$REDIS_HA_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.host=\"$REDIS_HA_HOST\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.port=6379"
fi

if [[ "$REDIS_HA_PORT" ]] && [[ ! "$REDIS_SENTINELS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.port=$REDIS_HA_PORT"
fi

if [[ "$RECORDLOG_ENABLED" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .recordLog.enabled=true"
fi

if [[ "$STORAGE_LIMIT_ENABLED" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .utapi.metrics[.utapi.metrics | length]=\"location\""
fi

if [[ "$CRR_METRICS_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .backbeat.host=\"$CRR_METRICS_HOST\""
fi

if [[ "$CRR_METRICS_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .backbeat.port=$CRR_METRICS_PORT"
fi

if [[ "$WE_OPERATOR_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .workflowEngineOperator.host=\"$WE_OPERATOR_HOST\""
fi

if [[ "$WE_OPERATOR_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .workflowEngineOperator.port=$WE_OPERATOR_PORT"
fi

if [[ "$HEALTHCHECKS_ALLOWFROM" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .healthChecks.allowFrom=[\"$HEALTHCHECKS_ALLOWFROM\"]"
fi

# external backends http(s) agent config

# AWS
if [[ "$AWS_S3_HTTPAGENT_KEEPALIVE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.aws_s3.httpAgent.keepAlive=$AWS_S3_HTTPAGENT_KEEPALIVE"
fi

if [[ "$AWS_S3_HTTPAGENT_KEEPALIVE_MS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.aws_s3.httpAgent.keepAliveMsecs=$AWS_S3_HTTPAGENT_KEEPALIVE_MS"
fi

if [[ "$AWS_S3_HTTPAGENT_KEEPALIVE_MAX_SOCKETS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.aws_s3.httpAgent.maxSockets=$AWS_S3_HTTPAGENT_KEEPALIVE_MAX_SOCKETS"
fi

if [[ "$AWS_S3_HTTPAGENT_KEEPALIVE_MAX_FREE_SOCKETS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.aws_s3.httpAgent.maxFreeSockets=$AWS_S3_HTTPAGENT_KEEPALIVE_MAX_FREE_SOCKETS"
fi

#GCP
if [[ "$GCP_HTTPAGENT_KEEPALIVE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.gcp.httpAgent.keepAlive=$GCP_HTTPAGENT_KEEPALIVE"
fi

if [[ "$GCP_HTTPAGENT_KEEPALIVE_MS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.gcp.httpAgent.keepAliveMsecs=$GCP_HTTPAGENT_KEEPALIVE_MS"
fi

if [[ "$GCP_HTTPAGENT_KEEPALIVE_MAX_SOCKETS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.gcp.httpAgent.maxSockets=$GCP_HTTPAGENT_KEEPALIVE_MAX_SOCKETS"
fi

if [[ "$GCP_HTTPAGENT_KEEPALIVE_MAX_FREE_SOCKETS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .externalBackends.gcp.httpAgent.maxFreeSockets=$GCP_HTTPAGENT_KEEPALIVE_MAX_FREE_SOCKETS"

if [[ -n "$BUCKET_DENY_FILTER" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .utapi.filter.deny.bucket=[\"$BUCKET_DENY_FILTER\"]"
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
