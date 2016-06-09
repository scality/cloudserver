FROM node:4
MAINTAINER Giorgio Regni <gr@scalilty.com>


RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json /usr/src/app/
COPY . /usr/src/app
RUN npm install

ENV S3BACKEND file

CMD [ "npm", "start" ]

# replace this with your application's default port
EXPOSE 8000
