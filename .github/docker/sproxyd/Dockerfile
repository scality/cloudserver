FROM ghcr.io/scality/federation/sproxyd:9.5.1.0
ADD --chown=root:root ./conf/supervisord.conf ./conf/nginx.conf ./conf/fastcgi_params ./conf/sproxyd0.conf /conf/
RUN mkdir -p /run0
