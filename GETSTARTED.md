# List of applications that have been tested with S3 Server

## GUI

### [Cyberduck](https://cyberduck.io/?l=en)

- https://www.youtube.com/watch?v=-n2MCt4ukUg
- https://www.youtube.com/watch?v=IyXHcu4uqgU

### [Cloud Explorer](https://www.linux-toys.com/?p=945)

## Command Line Tools

### [s3curl](https://github.com/rtdp/s3curl)

https://github.com/scality/S3/blob/master/tests/functional/s3curl/s3curl.pl


### [s3cmd](http://s3tools.org/s3cmd)

~/.s3cfg
```
[default]
access_key = accessKey1
secret_key = verySecretKey1
host_base = 127.0.0.1:8000
host_bucket = %(bucket).127.0.0.1:8000
signature_v2 = False
use_https = False
```

## JavaScript

### [AWS JavaScript SDK](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)

```javascript
const AWS = require('aws-sdk');
const async = require('async');

const s3 = new AWS.S3(
	{ accessKeyId: 'accessKey1',
	secretAccessKey: 'verySecretKey1',
	endpoint: '127.0.0.1:8000',
	sslEnabled: false,
	s3ForcePathStyle: true,
});
```

## JAVA

### [AWS JAVA SDK](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html)

```java
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;

public class S3 {

	public static void main(String[] args) {

		AWSCredentials credentials = new BasicAWSCredentials(
				"accessKey1",
				"verySecretKey1");

		// Create a client connection based on credentials
		AmazonS3 s3client = new AmazonS3Client(credentials);
		s3client.setEndpoint("http://localhost:8000");

		// Using path-style requests
		// (deprecated) s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
		s3client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());

		// Create bucket
		String bucketName = "javabucket";
		s3client.createBucket(bucketName);

		// List off all buckets
		for (Bucket bucket : s3client.listBuckets()) {
			System.out.println(" - " + bucket.getName());
		}

	}
}
```

## Ruby

### [fog](http://fog.io/storage/)

```ruby
require "fog"

connection = Fog::Storage.new(
{
  :provider => "AWS",
	:aws_access_key_id => 'accessKey1',
	:aws_secret_access_key => 'verySecretKey1',
	:endpoint => 'http://127.0.0.1:8000',
	:path_style => true,
  :scheme => 'http',
})
```

## Python

### [boto2](http://boto.cloudhackers.com/en/latest/ref/s3.html)

```python
import boto
from boto.s3.connection import S3Connection, OrdinaryCallingFormat


connection = S3Connection(
		aws_access_key_id='accessKey1',
   	aws_secret_access_key='verySecretKey1',
   	is_secure=False,
   	port=8000,
   	calling_format=OrdinaryCallingFormat(),
   	host='127.0.0.1'
)

connection.create_bucket('mybucket')
```

### [boto3](http://boto3.readthedocs.io/en/latest/index.html)

``` python
import boto3
client = boto3.client(
    's3',
    aws_access_key_id='accessKey1',
    aws_secret_access_key='verySecretKey1',
    endpoint_url='http://127.0.0.1:8000'
)

lists = client.list_buckets()
```
