#! /bin/sh
set -e
# shunit script
# see https://shunit2.googlecode.com/svn/trunk/source/2.1/doc/shunit2.html

export expectedEnd="Bucket 's3://s3cmd/' created"

setUp() {
	#fake the DNS resolution
	echo "${IP} s3cmd.s3.amazonaws.com" >> /etc/hosts
	echo "${IP} s3.amazonaws.com" >> /etc/hosts
}
testMakeBucket() {
	result=$(s3cmd -c  ./s3cfg --signature-v2 mb s3://s3cmd | grep "${expectedEnd}" )
	assertEquals "testMakeBucket" "${expectedEnd}" "${result}"
}

# load shunit2
. /usr/share/shunit2/shunit2
