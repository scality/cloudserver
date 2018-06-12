FROM node:6-slim
MAINTAINER Giorgio Regni <gr@scality.com>

ENV LANG C.UTF-8
ARG BUILDBOT_VERSION
RUN apt-get update \
    && apt-get install -y jq python git build-essential --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Keep the .git directory in order to properly report version
WORKDIR /usr/src/app
COPY ./package.json ./

RUN npm install --production \
    && npm cache clear \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-*
COPY ./eve ./eve
RUN bash -l ./eve/workers/build/build.sh

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "npm", "start" ]

EXPOSE 8000
