FROM ubuntu:bionic

WORKDIR /home/eve/workspace

RUN apt-get update && apt-get install -y \
    git \
    gnupg2 \
    lsb-release \
    curl \
    python3-buildbot-worker \
    && adduser -u 1042 --home /home/eve eve \
    && chown -R eve:eve /home/eve

# Install docker cli
ARG DOCKER_VERSION=18.06.1~ce~3-0~ubuntu

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
 && echo "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | tee -a /etc/apt/sources.list.d/docker-ce.list \
 && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    docker-ce=${DOCKER_VERSION} \
 && rm -rf /var/lib/apt/lists/* /var/cache/apt

USER eve