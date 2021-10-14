FROM buildpack-deps:bionic-curl

#
# Install packages needed by the buildchain
#
ENV LANG C.UTF-8
COPY ./s3_packages.list ./buildbot_worker_packages.list /tmp/
RUN curl -sS http://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
    && echo "deb http://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y yarn \
    && cat /tmp/*packages.list | xargs apt-get install -y \
    && update-ca-certificates \
    && git clone https://github.com/tj/n.git \
    && make -C ./n \
    && n 10.22.0 \
    && pip install pip==9.0.1 \
    && rm -rf ./n \
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
ENV RUBY_VERSION="2.5.0"

RUN gpg2 --keyserver hkp://pgp.mit.edu --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 7D2BAF1CF37B13E2069D6956105BD0E739499BDB \
    && curl -sSL https://get.rvm.io | bash -s stable --ruby=$RUBY_VERSION \
   && usermod -a -G rvm eve

COPY ./gems.list /tmp/

RUN /bin/bash -l -c "\
   source /usr/local/rvm/scripts/rvm \
   && cat /tmp/gems.list | xargs gem install \
   && rm /tmp/gems.list"

# Install Pip packages
COPY ./pip_packages.list /tmp/
RUN cat /tmp/pip_packages.list | xargs pip install \
    && rm -f /tmp/pip_packages.list \
    && mkdir /home/eve/.aws \
    && chown eve /home/eve/.aws

#
# Run buildbot-worker on startup
#

ARG BUILDBOT_VERSION
RUN pip install buildbot-worker==$BUILDBOT_VERSION

CMD ["/bin/bash", "-l", "-c", "buildbot-worker create-worker . $BUILDMASTER:$BUILDMASTER_PORT $WORKERNAME $WORKERPASS   && buildbot-worker start --nodaemon"]
