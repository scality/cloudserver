FROM scality/nodesvc-base

#
# Install apt packages needed by the buildchain
#

COPY s3_packages.list buildbot_worker_packages.list /tmp/
RUN apt-get update -q \
    && apt-get install -qy software-properties-common python-software-properties \
    && add-apt-repository ppa:ubuntu-toolchain-r/test \
    && apt-add-repository ppa:brightbox/ruby-ng \
    && apt-get update -q \
    && cat /tmp/*_packages.list | xargs apt-get install -qy \
    && pip install pip==9.0.1 \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /tmp/*_packages.list

#
# Install dependencies
#

# Install python packages
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

# Install gems
COPY gems.list /tmp/
RUN cat /tmp/gems.list | xargs gem install \
    && rm /tmp/gems.list

# Install nodejs packages
WORKDIR /usr/lib/
COPY package.json .
RUN npm install

#
# Add user eve
#

RUN adduser -u 1042 --home /home/eve --disabled-password --gecos "" eve \
    && adduser eve sudo \
    && sed -ri 's/(%sudo.*)ALL$/\1NOPASSWD:ALL/' /etc/sudoers

#
# Misc
#

RUN echo '127.0.0.1    bucketwebsitetester.s3-website-us-east-1.amazonaws.com' >> /etc/hosts

#
# Run buildbot-worker on startup
#

ARG BUILDBOT_VERSION=0.9.7
RUN pip install buildbot-worker==$BUILDBOT_VERSION

ENV CXX=g++-4.9
USER eve
WORKDIR /home/eve/workspace
CMD buildbot-worker create-worker . "$BUILDMASTER:$BUILDMASTER_PORT" "$WORKERNAME" "$WORKERPASS" \
    && sudo service redis-server start \
    && buildbot-worker start --nodaemon
