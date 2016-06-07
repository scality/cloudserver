FROM node:4
MAINTAINER Giorgio Regni <gr@scalilty.com>

ADD id_dkr_irm /root/.ssh/id_rsa
RUN chmod 700 /root/.ssh/id_rsa

RUN echo "    IdentityFile ~/.ssh/id_rsa" >> /etc/ssh/ssh_config
RUN echo "Host github.com\n\tStrictHostKeyChecking no\n\tIdentityFile ~/.ssh/id_rsa\n" >> /root/.ssh/config

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json /usr/src/app/
COPY . /usr/src/app
RUN npm install

RUN rm  /root/.ssh/id_rsa

ENV S3BACKEND mem

CMD [ "npm", "start" ]

# replace this with your application's default port
EXPOSE 8000
