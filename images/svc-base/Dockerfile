FROM registry.scality.com/federation/nodesvc-base:7.10.6.0

ENV S3_CONFIG_FILE=${CONF_DIR}/config.json
ENV S3_LOCATION_FILE=${CONF_DIR}/locationConfig.json

COPY . ${HOME_DIR}/s3
RUN chown -R ${USER} ${HOME_DIR}
RUN pip3 install redis===3.5.3 requests==2.27.1 && \
    apt-get install -y git-lfs

USER ${USER}
WORKDIR ${HOME_DIR}/s3
RUN ln -s /scality-kms node_modules && \
    rm -f ~/.gitconfig && \
    git config --global --add safe.directory . && \
    git lfs install && \
    GIT_LFS_SKIP_SMUDGE=1 && \
    yarn install --frozen-lockfile --production --network-concurrency 1 && \
    yarn cache clean --all

EXPOSE 8000

CMD bash -c "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/supervisord.conf"
