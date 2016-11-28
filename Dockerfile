FROM node:4-slim
MAINTAINER Giorgio Regni <gr@scality.com>

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN apt-get update \
    && apt-get install -y python git build-essential \
    && npm install \
    && apt-get autoremove -y python build-essential

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "npm", "start" ]

EXPOSE 8000
