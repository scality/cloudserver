ARG CLOUDSERVER_VERSION=latest
FROM ghcr.io/scality/cloudserver:${CLOUDSERVER_VERSION} AS builder

####################################################################################################
FROM ghcr.io/scality/cloudserver:${CLOUDSERVER_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      supervisor \
    && rm -rf /var/lib/apt/lists/*

# Prepare runtime environment
ENV USER="scality"
ENV HOME_DIR="/home/${USER}" \
    LOG_DIR="/logs" \
    CONF_DIR="/conf" \
    DATA_DIR="/data" \
    SUP_RUN_DIR="/var/run/supervisor"

RUN mv /home/node ${HOME_DIR} && usermod --login ${USER} --shell /bin/bash -d ${HOME_DIR} node
RUN mkdir ${LOG_DIR} && chown ${USER} ${LOG_DIR} && \
    mkdir ${CONF_DIR} && chown ${USER} ${CONF_DIR} && \
    mkdir ${DATA_DIR} && chown ${USER} ${DATA_DIR} && \
    mkdir -m 777 ${SUP_RUN_DIR} && chown ${USER} ${SUP_RUN_DIR}

USER ${USER}
WORKDIR ${HOME_DIR}/s3

# Keep same output as chown command without group (use group 0)
COPY --chown=${USER}:0 --from=builder /usr/src/app ${HOME_DIR}/s3

ENV S3_CONFIG_FILE=${CONF_DIR}/config.json
ENV S3_LOCATION_FILE=${CONF_DIR}/locationConfig.json
EXPOSE 8000

CMD [ "/bin/bash", "-c", "source ${CONF_DIR}/env && export && exec supervisord -c ${CONF_DIR}/supervisord.conf" ]
ENTRYPOINT []
