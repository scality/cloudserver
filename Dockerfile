ARG NODE_VERSION=24.22.1-trixie-slim

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
COPY package.json yarn.lock /usr/src/app/

RUN yarn install --production --frozen-lockfile --network-concurrency 1

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

# Same c8 version as the devDependency, without bringing the other dev deps
RUN yarn global add c8@$(node -p "require('./package.json').devDependencies.c8")

CMD [ "./docker-test-with-coverage.sh" ]
