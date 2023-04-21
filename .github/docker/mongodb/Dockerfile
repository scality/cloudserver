FROM mongo:4.2.24

ENV USER=scality \
    HOME_DIR=/home/scality \
    CONF_DIR=/conf \
    DATA_DIR=/data

# Set up directories and permissions
RUN mkdir -p /data/db /data/configdb && chown -R mongodb:mongodb /data/db /data/configdb; \
    mkdir /logs; \
    adduser --uid 1000 --disabled-password --gecos --quiet --shell /bin/bash scality

# Set up environment variables and directories for scality user
RUN mkdir ${CONF_DIR} && \
    chown -R ${USER} ${CONF_DIR} && \
    chown -R ${USER} ${DATA_DIR}

# Create the mongod.conf file
RUN echo "storage:
   journal:
      enabled: true
   engine: wiredTiger
   dbPath: \"/data/db\"
processManagement:
   fork: false
net:
   port: 27018
   bindIp: 0.0.0.0
replication:
   replSetName: \"rs0\"
   enableMajorityReadConcern: true
security:
    authorization: disabled" > ${CONF_DIR}/mongod.conf

# Create the mongo-run.sh file
RUN echo "#!/bin/bash
set -exo pipefail

init_RS() {
  sleep 5
  mongo --port 27018 /conf/initReplicaSet.js
}
init_RS &

mongod --bind_ip_all --config=/conf/mongod.conf" > ${CONF_DIR}/mongo-run.sh && chmod +x ${CONF_DIR}/mongo-run.sh

# Create the initReplicaSet.js file
RUN echo "rs.initiate({
    _id: \"rs0\",
    members: [{ _id: 0, host: \"127.0.0.1:27018\" }]
});" > ${CONF_DIR}/initReplicaSet.js

EXPOSE 27017/tcp
EXPOSE 27018

# Set up CMD
ENTRYPOINT ["bash", "/conf/mongo-run.sh"]
CMD ["bash", "/conf/mongo-run.sh"]
