FROM ubuntu:xenial

#
# Install packages needed by the buildchain
#
ENV LANG C.UTF-8
COPY ./s3_packages.list ./buildbot_worker_packages.list /tmp/
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6 \
    && echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list \
    && apt-get update \
    && cat /tmp/*packages.list | xargs apt-get install -y \
    && wget https://nodejs.org/dist/v6.9.5/node-v6.9.5-linux-x64.tar.xz \
    && tar -C /usr/local --strip-components 1 -xvf node-v6.9.5-linux-x64.tar.xz \
    && wget https://raw.githubusercontent.com/scality/S3/development/8.0/package.json \
    && npm install -g \
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
COPY ./gems.list /tmp/
RUN gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 \
    && curl -sSL https://get.rvm.io | bash -s stable --ruby=$RUBY_VERSION \
    && usermod -a -G rvm eve
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


# environment vars into the bash profile
COPY ./bash_profile /home/eve/.bash_profile
RUN chown eve /home/eve/.bash_profile
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

#
# Run buildbot-worker on startup
#

ARG BUILDBOT_VERSION
RUN pip install buildbot-worker==$BUILDBOT_VERSION

#USER eve
COPY ./bash_profile /home/eve/.bashrc
COPY ./bash_profile /home/eve/.bash_profile
#RUN exec bash -l
#WORKDIR /home/eve/workspace
#CMD ["sudo", "supervisord", "-n"]
CMD ["/bin/bash", "-l", "-c", "buildbot-worker create-worker . $BUILDMASTER:$BUILDMASTER_PORT $WORKERNAME $WORKERPASS   && buildbot-worker start --nodaemon"]
