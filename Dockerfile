FROM node:10-slim
MAINTAINER Giorgio Regni <gr@scality.com>

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1

EXPOSE 8000

COPY ./package.json /usr/src/app/

# Keep the .git directory in order to properly report version
COPY ./package.json yarn.lock ./

RUN apt-get update \
    && apt-get install -y jq python git build-essential ssh --no-install-recommends \
    && mkdir -p /root/ssh \
    && ssh-keyscan -H github.com > /root/ssh/known_hosts \
    && yarn cache clean \
    && yarn install --frozen-lockfile --production --ignore-optional \
    && apt-get autoremove --purge -y python git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && yarn cache clean \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-*

COPY . /usr/src/app

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

CMD [ "yarn", "start" ]
