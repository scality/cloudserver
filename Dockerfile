FROM node:10.22.0-slim
MAINTAINER Giorgio Regni <gr@scality.com>

WORKDIR /usr/src/app

# Keep the .git directory in order to properly report version
COPY ./package.json yarn.lock ./

ENV PYTHON=python3.9
ENV PY_VERSION=3.9.7

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    jq \
    python \
    git \
    build-essential \
    ssh \
    ca-certificates \
    wget \
    libffi-dev \
    zlib1g-dev \
    && mkdir -p /root/ssh \
    && ssh-keyscan -H github.com > /root/ssh/known_hosts

RUN cd /tmp \
    && wget https://www.python.org/ftp/python/$PY_VERSION/Python-$PY_VERSION.tgz \
    && tar -C /usr/local/bin -xzvf Python-$PY_VERSION.tgz \
    && cd /usr/local/bin/Python-$PY_VERSION \
    && ./configure --enable-optimizations \
    && make \
    && make altinstall \
    && rm -rf /tmp/Python-$PY_VERSION.tgz

RUN yarn cache clean \
    && yarn install --production --ignore-optional --ignore-engines  \
    && apt-get autoremove --purge -y python git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && yarn cache clean \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-*

COPY ./ ./

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "yarn", "start" ]

EXPOSE 8000
