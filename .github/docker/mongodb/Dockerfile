FROM mongo:4.4.21

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

# copy the mongo config file
COPY /conf/mongod.conf /conf/mongod.conf
COPY /conf/mongo-run.sh /conf/mongo-run.sh
COPY /conf/initReplicaSet /conf/initReplicaSet.js

EXPOSE 27017/tcp
EXPOSE 27018

# Set up CMD
ENTRYPOINT ["bash", "/conf/mongo-run.sh"]
CMD ["bash", "/conf/mongo-run.sh"]
