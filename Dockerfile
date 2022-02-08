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
    yarn \
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

COPY . /usr/src/app

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

CMD [ "yarn", "start" ]
