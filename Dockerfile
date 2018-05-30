FROM node:6-slim
MAINTAINER Giorgio Regni <gr@scality.com>

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y jq --no-install-recommends

COPY package.json /usr/src/app
RUN apt-get install -y python git build-essential --no-install-recommends \
    && npm install --production \
    && apt-get autoremove --purge -y python git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && npm cache clear \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-*

# Keep the .git directory in order to properly report version
COPY . /usr/src/app

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "npm", "start" ]

EXPOSE 8000
