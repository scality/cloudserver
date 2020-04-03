FROM ceph/daemon:v3.2.1-stable-3.2-mimic-centos-7

ENV CEPH_DAEMON demo
ENV CEPH_DEMO_DAEMONS mon,mgr,osd,rgw

ENV CEPH_DEMO_UID zenko
ENV CEPH_DEMO_ACCESS_KEY accessKey1
ENV CEPH_DEMO_SECRET_KEY verySecretKey1
ENV CEPH_DEMO_BUCKET zenkobucket

ENV CEPH_PUBLIC_NETWORK 0.0.0.0/0
ENV MON_IP 0.0.0.0
ENV NETWORK_AUTO_DETECT 4
ENV RGW_CIVETWEB_PORT 8001

RUN rm /etc/yum.repos.d/tcmu-runner.repo

ADD ./entrypoint-wrapper.sh /
RUN chmod +x /entrypoint-wrapper.sh && \
    yum install -y python-pip && \
    yum clean all && \
    pip install awscli && \
    rm -rf /root/.cache/pip

ENTRYPOINT [ "/entrypoint-wrapper.sh" ]
