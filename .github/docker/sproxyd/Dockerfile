FROM registry.scality.com/federation/sproxyd:7.10.6.8
ADD ./conf/supervisord.conf ./conf/nginx.conf ./conf/fastcgi_params ./conf/sproxyd0.conf /conf/
RUN chown root:root /conf/sproxyd0.conf
