#!/bin/bash

SCOPE="scality-s3c"
ROLE="admin"

# wait vault server to be up
while true
do
  netstat -plnt | grep -q ':8200' && break
  sleep 0.1
done

ipv4=$(ip -4 addr show eth0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')

# find vault server address
export VAULT_ADDR="http://"${ipv4}":8200"

# init vault
vault operator init --format json > /root/operator_init

# unseal
for i in $(seq 0 2); do
  key=$(jq -r ".unseal_keys_b64[$i]" /root/operator_init)
  vault operator unseal "$key"
done

# set root token
export VAULT_TOKEN=$(jq -r ".root_token" /root/operator_init)

# set license
cat << EOF > /root/license.json
{
  "text": "$LICENSE"
}
EOF
curl \
 --silent \
 --fail \
 --show-error \
 --header "X-Vault-Token: ${VAULT_TOKEN}" \
 --request PUT \
 --data @/root/license.json  \
 ${VAULT_ADDR}/v1/sys/license

# enable kmip
vault secrets enable kmip

# configure kmip
vault write kmip/config listen_addrs=0.0.0.0:5696
vault write -f kmip/scope/${SCOPE}
vault write kmip/scope/${SCOPE}/role/${ROLE} operation_all=true

# generate pem files for the client
vault write kmip/config server_ips=${ipv4}
vault read -format=json kmip/ca | jq -r .data.ca_pem > /var/www/html/ca.pem
vault write -format=json kmip/scope/${SCOPE}/role/${ROLE}/credential/generate cert_format=pem > /root/creds.json
jq -r .data.certificate /root/creds.json > /var/www/html/cert.pem
jq -r .data.private_key /root/creds.json > /var/www/html/key.pem

# make the pem files available for the tester
exec /usr/sbin/nginx
