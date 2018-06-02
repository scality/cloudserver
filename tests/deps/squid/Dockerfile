FROM alpine:3.7

RUN apk update \
    && apk add --no-cache squid acf-squid ca-certificates libressl \
    && /usr/lib/squid/ssl_crtd -c -s /var/lib/ssl_db

COPY squid.conf /etc/squid/squid.conf

EXPOSE 3128 3129
# Squid needs to initialize the SSL generation before properly running
CMD squid -f /etc/squid/squid.conf -N -z && squid -f /etc/squid/squid.conf -NYCd 1
