FROM node:16.13.2-slim
MAINTAINER Giorgio Regni <gr@scality.com>

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1

EXPOSE 8000
EXPOSE 8002

COPY ./package.json /usr/src/app/
COPY ./yarn.lock /usr/src/app/

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y \
    curl \
    gnupg2

RUN curl -sS http://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
    && echo "deb http://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
        jq \
        python3 \
        ssh \
        yarn \
        wget \
        libffi-dev \
        zlib1g-dev

RUN mkdir -p /root/ssh \
    && ssh-keyscan -H github.com > /root/ssh/known_hosts

ENV PYTHON=python3
RUN yarn cache clean
RUN yarn install --production --ignore-optional --ignore-engines --network-concurrency 1
RUN apt-get install htop
RUN apt-get autoremove --purge -y python git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && yarn cache clean \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-*

COPY . /usr/src/app

RUN echo 'fs.file-max = 2097152' >> /etc/sysctl.conf
RUN echo 'net.ipv4.ip_local_port_range = 2000 65535' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_rfc1337 = 1' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_fin_timeout' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_keepalive_time = 300' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_keepalive_probes = 5' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_keepalive_intvl = 15' >> /etc/sysctl.conf
RUN echo 'net.core.rmem_default = 31457280' >> /etc/sysctl.conf
RUN echo 'net.core.rmem_max = 12582912' >> /etc/sysctl.conf
RUN echo 'net.core.wmem_default = 31457280' >> /etc/sysctl.conf
RUN echo 'net.core.wmem_max = 12582912' >> /etc/sysctl.conf
RUN echo 'net.core.somaxconn = 4096' >> /etc/sysctl.conf
RUN echo 'net.core.netdev_max_backlog = 65536' >> /etc/sysctl.conf
RUN echo 'net.core.optmem_max = 25165824' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_mem = 65536 131072 262144' >> /etc/sysctl.conf
RUN echo 'net.ipv4.udp_mem = 65536 131072 262144' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_rmem = 8192 87380 16777216' >> /etc/sysctl.conf
RUN echo 'net.ipv4.udp_rmem_min = 16384' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_wmem = 8192 65536 16777216' >> /etc/sysctl.conf
RUN echo 'net.ipv4.udp_wmem_min = 16384' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_max_tw_buckets = 1440000' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_tw_recycle = 1' >> /etc/sysctl.conf
RUN echo 'net.ipv4.tcp_tw_reuse = 1' >> /etc/sysctl.conf

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

CMD [ "yarn", "start" ]
