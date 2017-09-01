Clients
=========

List of applications that have been tested with Zenko CloudServer.

GUI
~~~

`Cyberduck <https://cyberduck.io/?l=en>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  https://www.youtube.com/watch?v=-n2MCt4ukUg
-  https://www.youtube.com/watch?v=IyXHcu4uqgU

`Cloud Explorer <https://www.linux-toys.com/?p=945>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  https://www.youtube.com/watch?v=2hhtBtmBSxE

`CloudBerry Lab <http://www.cloudberrylab.com>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  https://youtu.be/IjIx8g\_o0gY

Command Line Tools
~~~~~~~~~~~~~~~~~~

`s3curl <https://github.com/rtdp/s3curl>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

https://github.com/scality/S3/blob/master/tests/functional/s3curl/s3curl.pl

`aws-cli <http://docs.aws.amazon.com/cli/latest/reference/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``~/.aws/credentials`` on Linux, OS X, or Unix or
``C:\Users\USERNAME\.aws\credentials`` on Windows

.. code:: shell

    [default]
    aws_access_key_id = accessKey1
    aws_secret_access_key = verySecretKey1

``~/.aws/config`` on Linux, OS X, or Unix or
``C:\Users\USERNAME\.aws\config`` on Windows

.. code:: shell

    [default]
    region = us-east-1

Note: ``us-east-1`` is the default region, but you can specify any
region.

See all buckets:

.. code:: shell

    aws s3 ls --endpoint-url=http://localhost:8000

Create bucket:

.. code:: shell

    aws --endpoint-url=http://localhost:8000 s3 mb s3://mybucket

`s3cmd <http://s3tools.org/s3cmd>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If using s3cmd as a client to S3 be aware that v4 signature format is
buggy in s3cmd versions < 1.6.1.

``~/.s3cfg`` on Linux, OS X, or Unix or ``C:\Users\USERNAME\.s3cfg`` on
Windows

.. code:: shell

    [default]
    access_key = accessKey1
    secret_key = verySecretKey1
    host_base = localhost:8000
    host_bucket = %(bucket).localhost:8000
    signature_v2 = False
    use_https = False

See all buckets:

.. code:: shell

    s3cmd ls

`rclone <http://rclone.org/s3/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``~/.rclone.conf`` on Linux, OS X, or Unix or
``C:\Users\USERNAME\.rclone.conf`` on Windows

.. code:: shell

    [remote]
    type = s3
    env_auth = false
    access_key_id = accessKey1
    secret_access_key = verySecretKey1
    region = other-v2-signature
    endpoint = http://localhost:8000
    location_constraint =
    acl = private
    server_side_encryption =
    storage_class =

See all buckets:

.. code:: shell

    rclone lsd remote:

JavaScript
~~~~~~~~~~

`AWS JavaScript SDK <http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: javascript

    const AWS = require('aws-sdk');

    const s3 = new AWS.S3({
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
        endpoint: 'localhost:8000',
        sslEnabled: false,
        s3ForcePathStyle: true,
    });

JAVA
~~~~

`AWS JAVA SDK <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: java

    import com.amazonaws.auth.AWSCredentials;
    import com.amazonaws.auth.BasicAWSCredentials;
    import com.amazonaws.services.s3.AmazonS3;
    import com.amazonaws.services.s3.AmazonS3Client;
    import com.amazonaws.services.s3.S3ClientOptions;
    import com.amazonaws.services.s3.model.Bucket;

    public class S3 {

        public static void main(String[] args) {

            AWSCredentials credentials = new BasicAWSCredentials("accessKey1",
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

Ruby
~~~~

`AWS SDK for Ruby - Version 2 <http://docs.aws.amazon.com/sdkforruby/api/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ruby

    require 'aws-sdk'

    s3 = Aws::S3::Client.new(
      :access_key_id => 'accessKey1',
      :secret_access_key => 'verySecretKey1',
      :endpoint => 'http://localhost:8000',
      :force_path_style => true
    )

    resp = s3.list_buckets

`fog <http://fog.io/storage/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ruby

    require "fog"

    connection = Fog::Storage.new(
    {
        :provider => "AWS",
        :aws_access_key_id => 'accessKey1',
        :aws_secret_access_key => 'verySecretKey1',
        :endpoint => 'http://localhost:8000',
        :path_style => true,
        :scheme => 'http',
    })

Python
~~~~~~

`boto2 <http://boto.cloudhackers.com/en/latest/ref/s3.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    import boto
    from boto.s3.connection import S3Connection, OrdinaryCallingFormat


    connection = S3Connection(
        aws_access_key_id='accessKey1',
        aws_secret_access_key='verySecretKey1',
        is_secure=False,
        port=8000,
        calling_format=OrdinaryCallingFormat(),
        host='localhost'
    )

    connection.create_bucket('mybucket')

`boto3 <http://boto3.readthedocs.io/en/latest/index.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    import boto3
    client = boto3.client(
        's3',
        aws_access_key_id='accessKey1',
        aws_secret_access_key='verySecretKey1',
        endpoint_url='http://localhost:8000'
    )

    lists = client.list_buckets()

PHP
~~~

Should force path-style requests even though v3 advertises it does by default. 

`AWS PHP SDK v3 <https://docs.aws.amazon.com/aws-sdk-php/v3/guide>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: php

    use Aws\S3\S3Client;

    $client = S3Client::factory([
        'region'  => 'us-east-1',
        'version'   => 'latest',
        'endpoint' => 'http://localhost:8000',
        'use_path_style_endpoint' => true,
        'credentials' => [
             'key'    => 'accessKey1',
             'secret' => 'verySecretKey1'
        ]
    ]);

    $client->createBucket(array(
        'Bucket' => 'bucketphp',
    ));
