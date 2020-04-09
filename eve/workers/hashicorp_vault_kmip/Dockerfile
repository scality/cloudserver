FROM ubuntu:bionic

RUN apt-get update

# install requirements
RUN apt-get install --no-install-recommends --assume-yes \
    ca-certificates \
    curl \
    jq \
    iproute2 \
    net-tools \
    nginx \
    unzip

# install vault
RUN curl -s -o /tmp/vault.zip https://releases.hashicorp.com/vault/1.3.4+ent/vault_1.3.4+ent_linux_amd64.zip
RUN unzip -p /tmp/vault.zip > /usr/local/bin/vault && chmod u+x /usr/local/bin/vault

# use a volume for vault server data
VOLUME "/mnt/vault/data"

# run
COPY vault.cfg /etc/vault.cfg.template
COPY nginx.conf /etc/nginx/nginx.conf
COPY setup.sh /setup.sh
COPY start.sh /start.sh
CMD ["/start.sh"]
