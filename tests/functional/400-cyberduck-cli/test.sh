#! /bin/sh
set -e
# shunit script
# see https://shunit2.googlecode.com/svn/trunk/source/2.1/doc/shunit2.html

setUp() {
	# create a bucket where cyberduck will upload more data 
	export AWS_ACCESS_KEY_ID=accessKey1 
	export AWS_SECRET_ACCESS_KEY=verySecretKey1 
	aws --endpoint "http://${IP}:8000" s3api create-bucket --bucket cyberduck
}

testUpload() {
	su ironman -c "duck -u accessKey1 -p verySecretKey1 -y -v -e overwrite  --upload s3-http://cyberduck/ /mnt/"
	su ironman -c "duck -u accessKey1 -p verySecretKey1 -y -v -e overwrite  --download s3-http://cyberduck/ /tmp/"
	result=$(diff -r /tmp/cyberduck /mnt)
	echo $result
	assertEquals "testUpload" "" "${result}" 
}

# load shunit2
. /usr/share/shunit2/shunit2
