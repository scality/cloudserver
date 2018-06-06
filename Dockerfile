FROM node:6-slim
MAINTAINER Giorgio Regni <gr@scality.com>

RUN apt-get update \
    && apt-get install -y jq python git build-essential --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Keep the .git directory in order to properly report version
COPY . /usr/src/app

WORKDIR /usr/src/app

RUN npm install --production \
    && npm cache clear \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-* \
    && apt-get autoremove --purge -y python git build-essential

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "npm", "start" ]

EXPOSE 8000
