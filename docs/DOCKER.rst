Docker
======

-  `Environment Variables <environment-variables>`__
-  `Tunables and setup tips <tunables-and-setup-tips>`__
-  `Examples for continuous integration with Docker 
   <continuous-integration-with-docker-hosted-cloudserver>`__
-  `Examples for going into production with Docker 
   <in-production-w-a-Docker-hosted-cloudserver>`__

.. _environment-variables:

Environment Variables
---------------------

S3DATA
~~~~~~

S3DATA=multiple
^^^^^^^^^^^^^^^

This variable enables running CloudServer with multiple data backends, defined
as regions.

For multiple data backends, a custom locationConfig.json file is required.
This file enables you to set custom regions. You must provide associated 
rest_endpoints for each custom region in config.json.

`Learn more about multiple-backend configurations <./GETTING_STARTED#location-configuration>`__

If you are using Scality RING endpoints, refer to your customer documentation.

Running CloudServer with an AWS S3-Hosted Backend
"""""""""""""""""""""""""""""""""""""""""""""""""

To run CloudServer with an S3 AWS backend, add a new section to the 
``locationConfig.json`` file with the ``aws_s3`` location type:

.. code:: json

    (...)
    "awsbackend": {
        "type": "aws_s3",
        "details": {
            "awsEndpoint": "s3.amazonaws.com",
            "bucketName": "yourawss3bucket",
            "bucketMatch": true,
            "credentialsProfile": "aws_hosted_profile"
        }
    }
    (...)

Edit your AWS credentials file to enable your preferred command-line tool.
This file must mention credentials for all backends in use. You can use 
several profiles if multiple profiles are configured.

.. code:: json

    [default]
    aws_access_key_id=accessKey1
    aws_secret_access_key=verySecretKey1
    [aws_hosted_profile]
    aws_access_key_id={{YOUR_ACCESS_KEY}}
    aws_secret_access_key={{YOUR_SECRET_KEY}}

As with locationConfig.json, the AWS credentials file must be mounted at 
run time: ``-v ~/.aws/credentials:/root/.aws/credentials`` on Unix-like 
systems (Linux, OS X, etc.), or 
``-v C:\Users\USERNAME\.aws\credential:/root/.aws/credentials`` on Windows

.. note:: One account cannot copy to another account with a source and
   destination on real AWS unless the account associated with the 
   accessKey/secretKey pairs used for the destination bucket has source 
   bucket access privileges. To enable this, update ACLs directly on AWS.

S3BACKEND
~~~~~~~~~

S3BACKEND=file
^^^^^^^^^^^^^^

For stored file data to persist, you must mount Docker volumes
for both data and metadata. See
`In Production with a Docker-Hosted CloudServer <in-production-w-a-Docker-hosted-cloudserver>`__

S3BACKEND=mem
^^^^^^^^^^^^^

This is ideal for testing: no data remains after the container is shut down.

ENDPOINT
~~~~~~~~

This variable specifies the endpoint. To direct CloudServer requests to 
new.host.com, for example, specify the endpoint with:

.. code-block:: shell

    $ docker run -d --name cloudserver -p 8000:8000 -e ENDPOINT=new.host.com scality/cloudserver

.. note:: On Unix-like systems (Linux, OS X, etc.) edit /etc/hosts
   to associate 127.0.0.1 with new.host.com.

SCALITY\_ACCESS\_KEY\_ID and SCALITY\_SECRET\_ACCESS\_KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These variables specify authentication credentials for an account named
“CustomAccount”.

Set account credentials for multiple accounts by editing conf/authdata.json
(see below for further details). To specify one set for personal use, set these 
environment variables:

.. code-block:: shell

   $ docker run -d --name cloudserver -p 8000:8000 -e SCALITY_ACCESS_KEY_ID=newAccessKey \
   -e SCALITY_SECRET_ACCESS_KEY=newSecretKey scality/cloudserver

.. note:: This takes precedence over the contents of the authdata.json 
	  file. The authdata.json file is ignored. 

.. note:: The ACCESS_KEY and SECRET_KEY environment variables are 
	  deprecated.

LOG\_LEVEL
~~~~~~~~~~

This variable changes the log level. There are three levels: info, debug, 
and trace. The default is info. Debug provides more detailed logs, and trace
provides the most detailed logs.

.. code-block:: shell

    $ docker run -d --name cloudserver -p 8000:8000 -e LOG_LEVEL=trace scality/cloudserver

SSL
~~~

Set true, this variable runs CloudServer with SSL.

If SSL is set true: 

* The ENDPOINT environment variable must also be specified.

* On Unix-like systems (Linux, OS X, etc.), 127.0.0.1 must be associated with
  <YOUR_ENDPOINT> in /etc/hosts.

   .. Warning:: Self-signed certs with a CA generated within the container are 
      suitable for testing purposes only. Clients cannot trust them, and they may
      disappear altogether on a container upgrade. The best security practice for 
      production environments is to use an extra container, such as 
      haproxy/nginx/stunnel, for SSL/TLS termination and to pull certificates
      from a mounted volume, limiting what an exploit on either component
      can expose. 

.. code:: shell

     $ docker run -d --name cloudserver -p 8000:8000 -e SSL=TRUE -e ENDPOINT=<YOUR_ENDPOINT> \
     scality/cloudserver

  For more information about using ClousdServer with SSL, see `Using SSL <./GETTING_STARTED#Using SSL>`__

LISTEN\_ADDR
~~~~~~~~~~~~

This variable causes CloudServer and its data and metadata components to 
listen on the specified address. This allows starting the data or metadata 
servers as standalone services, for example.

.. code:: shell

    docker run -d --name s3server-data -p 9991:9991 -e LISTEN_ADDR=0.0.0.0
    scality/s3server yarn run start_dataserver


DATA\_HOST and METADATA\_HOST
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These variables configure the data and metadata servers to use,
usually when they are running on another host and only starting the stateless
Zenko CloudServer.

.. code:: shell

    $ docker run -d --name cloudserver -e DATA_HOST=cloudserver-data \
    -e METADATA_HOST=cloudserver-metadata scality/cloudserver yarn run start_s3server

REDIS\_HOST
~~~~~~~~~~~

Use this variable to connect to the redis cache server on another host than
localhost.

.. code:: shell

    $ docker run -d --name cloudserver -p 8000:8000 \
    -e REDIS_HOST=my-redis-server.example.com scality/cloudserver

REDIS\_PORT
~~~~~~~~~~~

Use this variable to connect to the Redis cache server on a port other 
than the default 6379.

.. code:: shell

    $ docker run -d --name cloudserver -p 8000:8000 \
    -e REDIS_PORT=6379 scality/cloudserver

.. _tunables-and-setup-tips:

Tunables and Setup Tips
-----------------------

Using Docker Volumes
~~~~~~~~~~~~~~~~~~~~

CloudServer runs with a file backend by default, meaning that data is 
stored inside the CloudServer’s Docker container.

For data and metadata to persist, data and metadata must be hosted in Docker 
volumes outside the CloudServer’s Docker container. Otherwise, the data
and metadata are destroyed when the container is erased.

.. code-block:: shell

    $ docker run -­v $(pwd)/data:/usr/src/app/localData -­v $(pwd)/metadata:/usr/src/app/localMetadata \
    -p 8000:8000 ­-d scality/cloudserver

This command mounts the ./data host directory to the container
at /usr/src/app/localData and the ./metadata host directory to
the container at /usr/src/app/localMetaData. 

.. tip:: These host directories can be mounted to any accessible mount 
   point, such as /mnt/data and /mnt/metadata, for example.

Adding, Modifying, or Deleting Accounts or Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a customized authdata.json file locally based on /conf/authdata.json.

2. Use `Docker volumes <https://docs.docker.com/storage/volumes/>`__
   to override the default ``authdata.json`` through a Docker file mapping.

For example:

.. code-block:: shell

    $ docker run -v $(pwd)/authdata.json:/usr/src/app/conf/authdata.json -p 8000:8000 -d \
    scality/cloudserver

Specifying a Host Name
~~~~~~~~~~~~~~~~~~~~~~

To specify a host name (for example, s3.domain.name), provide your own
`config.json <https://github.com/scality/cloudserver/blob/master/config.json>`__
file using `Docker volumes <https://docs.docker.com/storage/volumes/>`__.

First, add a new key-value pair to the restEndpoints section of your
config.json. Make the key the host name you want, and the value the default 
location\_constraint for this endpoint.

For example, ``s3.example.com`` is mapped to ``us-east-1`` which is one
of the ``location_constraints`` listed in your locationConfig.json file
`here <https://github.com/scality/S3/blob/master/locationConfig.json>`__.

For more information about location configuration, see:
`GETTING STARTED <./GETTING_STARTED#location-configuration>`__

.. code:: json

    "restEndpoints": {
        "localhost": "file",
        "127.0.0.1": "file",
        ...
        "cloudserver.example.com": "us-east-1"
    },

Next, run CloudServer using a `Docker volume 
<https://docs.docker.com/engine/tutorials/dockervolumes/>`__:

.. code-block:: shell

    $ docker run -v $(pwd)/config.json:/usr/src/app/config.json -p 8000:8000 -d scality/cloudserver

The local ``config.json`` file overrides the default one through a Docker 
file mapping.

Running as an Unprivileged User
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CloudServer runs as root by default.

To change this, modify the dockerfile and specify a user before the 
entry point.

The user must exist within the container, and must own the 
/usr/src/app directory for CloudServer to run.

For example, the following dockerfile lines can be modified:

.. code-block:: shell

    ...
    && groupadd -r -g 1001 scality \
    && useradd -u 1001 -g 1001 -d /usr/src/app -r scality \
    && chown -R scality:scality /usr/src/app

    ...

    USER scality
    ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

.. _continuous-integration-with-docker-hosted-cloudserver:

Continuous Integration with a Docker-Hosted CloudServer
-------------------------------------------------------

When you start the Docker CloudServer image, you can adjust the
configuration of the CloudServer instance by passing one or more
environment variables on the ``docker run`` command line.


To run CloudServer for CI with custom locations (one in-memory, 
one hosted on AWS), and custom credentials mounted:

.. code-block:: shell

   $ docker run --name CloudServer -p 8000:8000 \
   -v $(pwd)/locationConfig.json:/usr/src/app/locationConfig.json \
   -v $(pwd)/authdata.json:/usr/src/app/conf/authdata.json \
   -v ~/.aws/credentials:/root/.aws/credentials \
   -e S3DATA=multiple -e S3BACKEND=mem scality/cloudserver

To run CloudServer for CI with custom locations, (one in-memory, one
hosted on AWS, and one file), and custom credentials `set as environment 
variables <./GETTING_STARTED#scality-access-key-id-and-scality-secret-access-key>`__):

.. code-block:: shell

   $ docker run --name CloudServer -p 8000:8000 \
   -v $(pwd)/locationConfig.json:/usr/src/app/locationConfig.json \
   -v ~/.aws/credentials:/root/.aws/credentials \
   -v $(pwd)/data:/usr/src/app/localData -v $(pwd)/metadata:/usr/src/app/localMetadata \
   -e SCALITY_ACCESS_KEY_ID=accessKey1 \
   -e SCALITY_SECRET_ACCESS_KEY=verySecretKey1 \
   -e S3DATA=multiple -e S3BACKEND=mem scality/cloudserver

.. _in-production-w-a-Docker-hosted-cloudserver:

In Production with a Docker-Hosted CloudServer
----------------------------------------------

Because data must persist in production settings, CloudServer offers
multiple-backend capabilities. This requires a custom endpoint 
and custom credentials for local storage.

Customize these with:

.. code-block:: shell

   $ docker run -d --name CloudServer \
   -v $(pwd)/data:/usr/src/app/localData -v $(pwd)/metadata:/usr/src/app/localMetadata \
   -v $(pwd)/locationConfig.json:/usr/src/app/locationConfig.json \
   -v $(pwd)/authdata.json:/usr/src/app/conf/authdata.json \
   -v ~/.aws/credentials:/root/.aws/credentials -e S3DATA=multiple \
   -e ENDPOINT=custom.endpoint.com \
   -p 8000:8000 ­-d scality/cloudserver \
