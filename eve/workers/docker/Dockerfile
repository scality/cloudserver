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
    && adduser -u 1042 --home /home/eve eve --groups docker \
    && chown -R eve:eve /home/eve \
    && pip3 install buildbot-worker==${BUILDBOT_VERSION}

CMD buildbot-worker create-worker . ${BUILDMASTER}:${BUILDMASTER_PORT} ${WORKERNAME} ${WORKERPASS} && buildbot-worker start --nodaemon
