FROM node:4-slim
MAINTAINER Giorgio Regni <gr@scality.com>

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN apt-get update \
    && apt-get install -y python git build-essential \
    && npm install \
    && apt-get autoremove -y python build-essential

CMD [ "npm", "start" ]

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

EXPOSE 8000
