FROM centos:7

ARG BUILDBOT_VERSION=0.9.12

VOLUME /home/eve/workspace

WORKDIR /home/eve/workspace

RUN yum install -y epel-release \
    && yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo \
    && yum install -y \
    python-devel \
    python-pip \
    python36 \
    python36-devel \
    python36-pip \
    git \
    docker-ce-cli-18.09.6 \
    which \
    && adduser -u 1042 --home /home/eve eve --groups docker \
    && chown -R eve:eve /home/eve \
    && pip3 install buildbot-worker==${BUILDBOT_VERSION}


ARG ORAS_VERSION=0.12.0
RUN curl -LO https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_amd64.tar.gz && \
    mkdir -p oras-install/ && \
    tar -zxf oras_${ORAS_VERSION}_*.tar.gz -C /usr/local/bin oras && \
    rm -rf oras_${ORAS_VERSION}_*.tar.gz oras-install/

CMD buildbot-worker create-worker . ${BUILDMASTER}:${BUILDMASTER_PORT} ${WORKERNAME} ${WORKERPASS} && buildbot-worker start --nodaemon
