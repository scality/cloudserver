FROM node:6-alpine
MAINTAINER Giorgio Regni <gr@scality.com>

EXPOSE 8000

COPY ./package.json ./package-lock.json /usr/src/app/

WORKDIR /usr/src/app

RUN apk add --update jq bash\
    && apk add --virtual build-deps \
                         python \
                         build-base \
                         git \
    && npm install --production \
    && apk del build-deps \
    && npm cache clear \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-* \
    && rm -rf /var/cache/apk/*

COPY . /usr/src/app

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

CMD [ "npm", "start" ]
