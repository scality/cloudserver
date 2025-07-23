ARG NODE_VERSION=22.14.0-bookworm-slim

FROM node:${NODE_VERSION} AS builder

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
        gnupg2 \
        jq \
        python3 \
        ssh \
        wget \
        libffi-dev \
        zlib1g-dev \
    && apt-get clean \
    && mkdir -p /root/ssh \
    && ssh-keyscan -H github.com > /root/ssh/known_hosts

ENV PYTHON=python3
RUN npm install -g \
    node-gyp \
    typescript@4.9.5

# Copy package files first (rarely change)
COPY package.json yarn.lock /usr/src/app/
RUN yarn install --production --ignore-optional --frozen-lockfile --ignore-engines --network-concurrency 1

################################################################################
FROM node:${NODE_VERSION}

ENV NO_PROXY=localhost,127.0.0.1
ENV no_proxy=localhost,127.0.0.1

EXPOSE 8000
EXPOSE 8002

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copy dependencies first (cached)
COPY --from=builder /usr/src/app/node_modules ./node_modules/

# Copy static files that rarely change
COPY package.json yarn.lock index.js docker-entrypoint.sh constants.js config.json /usr/src/app/
COPY .git /usr/src/app/.git
COPY conf/ /usr/src/app/conf/

# Copy your source code last (changes most frequently)
COPY lib/ /usr/src/app/lib/
COPY bin/ /usr/src/app/bin/

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["tini", "--", "/usr/src/app/docker-entrypoint.sh"]

CMD [ "yarn", "start" ]
