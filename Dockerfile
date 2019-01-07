FROM node:8-alpine

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1

EXPOSE 8000

COPY ./package.json ./package-lock.json /usr/src/app/

WORKDIR /usr/src/app

RUN apk add --update jq bash coreutils openssl lz4 cyrus-sasl\
    && apk add --virtual build-deps \
                         openssl-dev \
                         lz4-dev \
                         cyrus-sasl-dev \
                         python \
                         git \
                         bsd-compat-headers \
                         make \
                         g++ \
    && npm install --production \
    && npm cache clear --force \
    && apk del build-deps \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-* \
    && rm -rf /var/cache/apk/*

# Keep the .git directory in order to properly report version
COPY . /usr/src/app

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

CMD [ "npm", "start" ]
