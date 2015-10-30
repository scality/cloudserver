#! /bin/sh
set -e
# shunit script
# see https://shunit2.googlecode.com/svn/trunk/source/2.1/doc/shunit2.html

export resultMakeBucket=\
'{
    "Owner": {
        "DisplayName": "accessKey1",
        "ID": "accessKey1"
    },
    "Buckets": [
        {
            "Name": "testfunc"
        }
    ]
}'

setUp() {
	export AWS_ACCESS_KEY_ID=accessKey1
	export AWS_SECRET_ACCESS_KEY=verySecretKey1
}

testMakeBucket() {
	aws --endpoint "http://${IP}:8000" s3api  create-bucket --bucket testfunc
	result=$(aws --endpoint "http://${IP}:8000" s3api list-buckets | grep -v "CreationDate")
	echo $result > /tmp/cmd
	echo ${resultMakeBucket} > /tmp/template
	same=$(diff -w /tmp/cmd /tmp/template)
	assertEquals "testMakeBucket" "" "${same}"
}

# load shunit2
. /usr/share/shunit2/shunit2
