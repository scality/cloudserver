FROM zenko/cloudserver:pensieve-0

RUN apt-get update && apt-get install -qqy git

COPY . /usr/src/app
RUN npm cache clear
RUN mkdir -p /root/.ssh/ && touch ~/.ssh/known_hosts && ssh-keygen -R github.com && npm install uuid && npm install --production
VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "npm", "start" ]

EXPOSE 8000
