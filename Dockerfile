# syntax=docker/dockerfile:1.7

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
# Persist yarn's cache in a fixed location so it can be mounted as a BuildKit
# cache below; this cache also holds the compiled git dependencies (e.g.
# arsenal's tsc output), so a warm cache skips the re-clone and re-compile.
ENV YARN_CACHE_FOLDER=/root/.yarn-cache
RUN npm install -g \
    node-gyp \
    typescript@4.9.5
COPY package.json yarn.lock /usr/src/app/

# BuildKit cache mount: even when this layer misses (e.g. yarn.lock changed),
# yarn reuses already-downloaded and already-built packages from the mount
# instead of re-cloning and re-running tsc for the git dependencies.
RUN --mount=type=cache,target=/root/.yarn-cache,sharing=locked \
    yarn install --production --ignore-optional --frozen-lockfile --ignore-engines --network-concurrency 1

################################################################################
FROM node:${NODE_VERSION} AS production

ENV NO_PROXY=localhost,127.0.0.1
ENV no_proxy=localhost,127.0.0.1

EXPOSE 8000
EXPOSE 8002

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        jq \
        tini \
        python3-redis \
        python3-requests \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Keep the .git directory in order to properly report version
COPY . /usr/src/app
COPY --from=builder /usr/src/app/node_modules ./node_modules/

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["tini", "-g", "--", "/usr/src/app/docker-entrypoint.sh"]

CMD [ "yarn", "start" ]

################################################################################
FROM production AS testcoverage

RUN yarn global add nyc

CMD [ "./docker-test-with-coverage.sh" ]
