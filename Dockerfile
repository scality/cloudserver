ARG NODE_VERSION=lts-bullseye-slim

FROM node:${NODE_VERSION} as builder

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
RUN yarn install --production --ignore-optional --frozen-lockfile --ignore-engines --network-concurrency 1

################################################################################
FROM node:${NODE_VERSION}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq \
    && rm -rf /var/lib/apt/lists/*

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1

EXPOSE 8000
EXPOSE 8002

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Keep the .git directory in order to properly report version
COPY . /usr/src/app
COPY --from=builder /usr/src/app/node_modules ./node_modules/


VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["tini", "--", "/usr/src/app/docker-entrypoint.sh"]

CMD [ "yarn", "start" ]
