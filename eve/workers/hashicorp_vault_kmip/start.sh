#!/bin/bash

ipv4=$(ip -4 addr show eth0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')

sed -e "s/__IP_ADDRESS__/${ipv4}/g"  /etc/vault.cfg.template > /etc/vault.cfg

/setup.sh &

exec /usr/local/bin/vault server -config /etc/vault.cfg
