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
COPY package.json yarn.lock .yarnrc.yml /usr/src/app/

# `workspaces focus` cannot enforce lockfile immutability (it silently
# re-resolves), so validate the lockfile first with a cheap build-less
# install. Together these preserve what --frozen-lockfile used to give us.
RUN corepack enable \
    && yarn install --immutable --mode=skip-build \
    && yarn workspaces focus --production

################################################################################
FROM node:${NODE_VERSION} AS production

# The production stage runs `yarn start`, and package.json pins Yarn 4 via
# packageManager, so the image's bundled Yarn 1 refuses to run at all. Install
# Yarn into the image rather than only enabling Corepack: a bare `corepack
# enable` leaves the CLI to be downloaded on every container start, which
# breaks air-gapped deployments. COREPACK_HOME is shared and world-readable so
# images that drop privileges (images/federation runs as `scality`) can use it.
ENV COREPACK_HOME=/usr/local/corepack
RUN corepack enable \
    && corepack install -g yarn@4.18.0 \
    && chmod -R a+rX ${COREPACK_HOME}

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

RUN npm install -g nyc

CMD [ "./docker-test-with-coverage.sh" ]
