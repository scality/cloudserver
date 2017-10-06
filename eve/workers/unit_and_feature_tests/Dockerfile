FROM scality/nodesvc-base

#
# Install apt packages needed by s3 and buildbot_worker
#
COPY s3_packages.list buildbot_worker_packages.list /tmp/

RUN apt-get update -q \
    && apt-get install -qy software-properties-common python-software-properties \
    && add-apt-repository ppa:ubuntu-toolchain-r/test \
    && apt-add-repository ppa:brightbox/ruby-ng \
    && apt-get update -q \
    && cat /tmp/s3_packages.list | xargs apt-get install -qy \
    && cat /tmp/buildbot_worker_packages.list | xargs apt-get install -qy \
    && pip install pip==9.0.1 \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /tmp/packages.list

RUN pip install flake8 yamllint \
    && pip install s3cmd==1.6.1 \
    && gem install fog-aws -v 1.3.0 \
    && gem install mime-types -v 3.1 \
    && gem install rspec -v 3.5 \
    && gem install json \
    && gem install digest 

ENV CXX=g++-4.9

# eve configuration
#
RUN adduser -u 1042 --home /home/eve --disabled-password --gecos "" eve \
    && adduser eve sudo \
    && sed -ri 's/(%sudo.*)ALL$/\1NOPASSWD:ALL/' /etc/sudoers

ARG BUILDBOT_VERSION=0.9.7


RUN pip install buildbot-worker==$BUILDBOT_VERSION

USER eve

#
# Install nodejs packages needed by s3
#

RUN mkdir /home/eve/node_reqs

COPY package.json /home/eve/node_reqs/package.json
RUN cd /home/eve/node_reqs && npm install

WORKDIR /home/eve/workspace

CMD buildbot-worker create-worker . "$BUILDMASTER:$BUILDMASTER_PORT" "$WORKERNAME" "$WORKERPASS" \
    && sudo service redis-server start \
    && twistd -ny buildbot.tac
