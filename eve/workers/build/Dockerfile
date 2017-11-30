FROM node:6-slim

#
# Install packages needed by the buildchain
#

COPY ./s3_packages.list ./buildbot_worker_packages.list /tmp/
RUN apt-get update \
    && cat /tmp/*packages.list | xargs apt-get install -y \
    && pip install pip==9.0.1 \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /tmp/packages.list

#
# Add user eve
#

RUN adduser -u 1042 --home /home/eve --disabled-password --gecos "" eve \
    && adduser eve sudo \
    && sed -ri 's/(%sudo.*)ALL$/\1NOPASSWD:ALL/' /etc/sudoers
#
# Install Dependencies
#

# Install RVM and gems
ENV RUBY_VERSION="2.4.1"
COPY gems.list /tmp/
RUN gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 \
    && curl -sSL https://get.rvm.io | sudo bash -s stable \
    && usermod -a -G rvm eve
RUN /bin/bash -l -c "\
    rvm install ruby-$RUBY_VERSION  \
    && rvm --default use ruby-$RUBY_VERSION \
    && cat /tmp/gems.list | xargs gem install \
    && rm /tmp/gems.list"

# Install Pip packages
COPY ./pip_packages.list /tmp/
RUN cat /tmp/pip_packages.list | xargs pip install \
    && rm -f /tmp/pip_packages.list \
    && mkdir /home/eve/.aws 

# environment vars into the bash profile
COPY ./bash_profile /home/eve/.bash_profilw

#
# Run buildbot-worker on startup
#

ARG BUILDBOT_VERSION
RUN pip install buildbot-worker==$BUILDBOT_VERSION

RUN rm -rf /usr/bin/clear_console \
    && ln -s /usr/bin/clear /usr/bin/clear_console
USER eve

WORKDIR /home/eve/workspace
CMD buildbot-worker create-worker . "$BUILDMASTER:$BUILDMASTER_PORT" "$WORKERNAME" "$WORKERPASS" \
    && buildbot-worker start --nodaemon
