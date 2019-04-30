FROM python:3-alpine


RUN apk add --no-cache \
        libressl && \
    apk add --no-cache --virtual .build-deps \
        python3-dev \
        libffi-dev \
        libressl-dev \
        sqlite-dev \
        build-base && \
    pip install pykmip requests && \
    apk del .build-deps && \
    mkdir /pykmip


ADD ./bin /usr/local/bin
ADD ./certs /ssl
ADD policy.json /etc/pykmip/policies/policy.json
ADD server.conf /etc/pykmip/server.conf
ADD docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
