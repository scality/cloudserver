FROM node:6-slim
MAINTAINER Giorgio Regni <gr@scality.com>

ENV LANG C.UTF-8
ARG BUILDBOT_VERSION
# Keep the .git directory in order to properly report version
WORKDIR /usr/src/app
RUN apt-get update \
    && apt-get install -y jq python git build-essential --no-install-recommends

COPY ./package.json ./

RUN npm install --production \
    && npm cache clear \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-*
# For CI builds
COPY ./eve/workers/build ./eve/workers/build
RUN bash -l ./eve/workers/build/build.sh

COPY . ./

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "npm", "start" ]

EXPOSE 8000
