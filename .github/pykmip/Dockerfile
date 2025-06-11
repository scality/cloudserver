FROM python:3.10-alpine

# Build from github repo sources as there is no recent release since 0.10.0
# but the repo has fixes for some crashes that slowed down kmip

RUN apk add --no-cache \
        libressl && \
    apk add --no-cache --virtual .build-deps \
        python3-dev \
        libffi-dev \
        libressl-dev \
        sqlite-dev \
        build-base \
        curl \
        git && \
    git clone https://github.com/openkmip/pykmip.git && \
    cd pykmip && \
    python3 setup.py install && \
    apk del .build-deps && \
    rm -rf /pykmip && \
    mkdir /pykmip

ADD ./bin /usr/local/bin
ADD ./certs /ssl
ADD policy.json /etc/pykmip/policies/policy.json
ADD server.conf /etc/pykmip/server.conf
ADD --chmod=+x docker-entrypoint.sh /

ENTRYPOINT ["/docker-entrypoint.sh"]
