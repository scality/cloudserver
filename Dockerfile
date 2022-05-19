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
        zlib1g-dev \
    && mkdir -p /root/ssh \
    && ssh-keyscan -H github.com > /root/ssh/known_hosts

ENV PYTHON=python3
RUN yarn cache clean \
    && yarn install --production --ignore-optional --ignore-engines --network-concurrency 1 \
    && apt-get autoremove --purge -y python git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && yarn cache clean \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-*

COPY . /usr/src/app

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

CMD [ "yarn", "start" ]
