#!/bin/sh

touch /artifacts/ceph.log
mkfifo /tmp/entrypoint_output
# We run this in the background so that we can tail the RGW log after init,
# because entrypoint.sh never returns
bash entrypoint.sh > /tmp/entrypoint_output &
entrypoint_pid="$!"
while read -r line; do
    echo $line
    # When we find this line server has started
    if [ -n "$(echo $line | grep 'Creating bucket')" ]; then
        break
    fi
done < /tmp/entrypoint_output

# Make our buckets - CEPH_DEMO_BUCKET is set to force the "Creating bucket" message, but unused
s3cmd mb s3://cephbucket s3://cephbucket2

mkdir /root/.aws
cat > /root/.aws/credentials <<EOF
[default]
aws_access_key_id = accessKey1
aws_secret_access_key = verySecretKey1
EOF

# Enable versioning on them
for bucket in cephbucket cephbucket2; do
    echo "Enabling versiong for $bucket"
    aws --endpoint http://127.0.0.1:8001 s3api  put-bucket-versioning --bucket $bucket --versioning Status=Enabled
done
tail -f /var/log/ceph/client.rgw.*.log | tee -a /artifacts/ceph.log
wait $entrypoint_pid
