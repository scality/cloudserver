===============
Getting Started
===============

.. figure:: ../res/scality-cloudserver-logo.png
   :alt: Zenko CloudServer logo

|CircleCI| |Scality CI|


Installation
------------

Dependencies
~~~~~~~~~~~~

Building and running the Scality Zenko CloudServer requires node.js 6.9.5 and
npm v3. Up-to-date versions can be found at
`Nodesource <https://github.com/nodesource/distributions>`__.


Clone the Source Code
~~~~~~~~~~~~~~~~~~~~~

.. code:: shell

    git clone https://github.com/scality/S3.git

Install js Dependencies
~~~~~~~~~~~~~~~~~~~~~~~

Go to the ./S3 folder and enter:

.. code:: shell

    npm install


Run CloudServer with a File Backend
-----------------------------------

.. code:: shell

    npm start

This starts a Zenko CloudServer on port 8000. Two additional ports, 9990 and
9991, are also opened locally for internal transfer of metadata and data,
respectively.

The default access key is accessKey1, with a secret key of verySecretKey1.

By default, metadata files are saved in the localMetadata directory and data
files are saved in the localData directory in your machine's ./S3 directory.
These directories are pre-created in the repository. To choose different
locations to save data or metadata, specify them with absolute paths.
Thus, when starting the server:

.. code:: shell

   mkdir -m 700 $(pwd)/myFavoriteDataPath
    mkdir -m 700 $(pwd)/myFavoriteMetadataPath
    export S3DATAPATH="$(pwd)/myFavoriteDataPath"
    export S3METADATAPATH="$(pwd)/myFavoriteMetadataPath"
    npm start

Run CloudServer with an In-Memory Backend
-----------------------------------------

Entering the command:

.. code:: shell

    npm run mem_backend

starts a Zenko CloudServer on port 8000. The default access key is
accessKey1, with a secret key of verySecretKey1.

Run CloudServer for Continuous Integration Testing or in Production with Docker
-------------------------------------------------------------------------------

`DOCKER <../DOCKER/>`__

Testing
-------

You can run unit tests with the following command:

.. code:: shell

    npm test

You can run multiple backend unit tests with:

.. code:: shell

  CI=true S3DATA=multiple npm start
   npm run multiple_backend_test

You can run the linter with:

.. code:: shell

    npm run lint

Running functional tests locally:

For the AWS and Azure backend tests to pass locally, modify
tests/locationConfigTests.json so that awsbackend specifies the bucket name of
a bucket to which you have access (based on your credentials profile) and modify
"azurebackend" with details for your Azure account.

The test suite requires additional tools, **s3cmd** and **Redis**, installed in
the environment in which the tests are running.

To install these tools:

1.  Install `s3cmd <http://s3tools.org/download>`__
2.  Install `redis <https://redis.io/download>`__ and start Redis.
3.  Add localCache section to your ``config.json``:

  .. code:: json

    "localCache": {
        "host": REDIS_HOST,
        "port": REDIS_PORT
    }

  where ``REDIS_HOST`` is your Redis instance IP address (``"127.0.0.1"``
  if your Redis is running locally) and ``REDIS_PORT`` is your Redis
  instance port (``6379`` by default).

4.  Add the following to your machine's etc/hosts file:

  .. code:: shell

    127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com

5. Start the Zenko CloudServer in memory and run the functional tests:

  .. code:: shell

    CI=true npm run mem_backend
    CI=true npm run ft_test

Configuration
-------------

There are three configuration files for your Scality Zenko CloudServer:

-  ``conf/authdata.json``, described above, for authentication.
-  ``locationConfig.json``, to set up configuration options for where data will be saved.
-  ``config.json``, for general configuration options.

Location Configuration
~~~~~~~~~~~~~~~~~~~~~~

You must specify at least one locationConstraint in your
locationConfig.json (or leave as pre-configured).

You must also specify 'us-east-1' as a locationConstraint, so if you define
only one locationConstraint, make sure it's this one. If you put a bucket to
an unknown endpoint and do not specify a locationConstraint in the PUT
bucket call, us-east-1 is used.

For instance, the following locationConstraint will save data sent to
``myLocationConstraint`` to the file backend:

 .. code:: json

    "myLocationConstraint": {
        "type": "file",
        "legacyAwsBehavior": false,
        "details": {}
    },

Each locationConstraint must include the ``type``, ``legacyAwsBehavior``,
and ``details`` keys. ``type`` indicates which backend will be used for
that region. Currently, mem, file, and scality are the supported
backends. ``legacyAwsBehavior`` indicates whether the region will have
the same behavior as the AWS S3 'us-east-1' region. If the
locationConstraint type is scality, ``details`` should contain connector
information for sproxyd. If the locationConstraint type is mem or file,
``details`` should be empty.

Once you have locationConstraints in locationConfig.json, you can specify
a default locationConstraint for each endpoint.

For instance, the following sets the ``localhost`` endpoint to the
``myLocationConstraint`` data backend defined above:

  .. code:: json

    "restEndpoints": {
         "localhost": "myLocationConstraint"
    },

To use an endpoint other than localhost for your Scality Zenko
CloudServer, you **must** list that endpoint in ``restEndpoints``.
Otherwise if your server is running with a:

-  **file backend**: Your default location constraint will be ``file``
-  **memory backend**: Your default location constraint will be ``mem``

Endpoints
~~~~~~~~~

Zenko CloudServer supports both:

-  path-style: http://myhostname.com/mybucket
-  hosted-style: http://mybucket.myhostname.com

However, if you use an IP address for your host, hosted-style requests
will not hit the server. Make sure to use path-style requests in that
case. For example, if you are using the AWS SDK for JavaScript,
instantiate your client like this:

  .. code:: js

    const s3 = new aws.S3({
       endpoint: 'http://127.0.0.1:8000',
       s3ForcePathStyle: true,
    });

Setting Your Own Access Key and Secret Key Pairs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set credentials for many accounts by editing
``conf/authdata.json``, but to specify one set of your own credentials,
use ``SCALITY_ACCESS_KEY_ID`` and ``SCALITY_SECRET_ACCESS_KEY``
environment variables.

SCALITY_ACCESS_KEY_ID and SCALITY_SECRET_ACCESS_KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These variables specify authentication credentials for an account named
"CustomAccount".

**Note:** Anything in the ``authdata.json`` file is ignored.

  .. code:: shell

    SCALITY_ACCESS_KEY_ID=newAccessKey SCALITY_SECRET_ACCESS_KEY=newSecretKey npm start


Scality with SSL
~~~~~~~~~~~~~~~~

To use https with your local Zenko CloudServer, you must set up SSL certificates.

Deploying Zenko CloudServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, deploy **Zenko CloudServer**. It is easiest to do this using
`our DockerHub page <https://hub.docker.com/r/scality/s3server/>`__ (Run it
with a file backend).

    **Note:** If Docker is not installed on your machine, follow
    `these instructions to install it for your distribution <https://docs.docker.com/engine/installation/>`__

Updating the Zenko CloudServer Container's Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add your certificates to your container. To do this, you must exec inside the
Zenko CloudServer container. Run a
``$> docker ps`` and find your container's id (the corresponding image
name should be ``scality/s3server``. Copy the corresponding container id
(``894aee038c5e`` in this example), and run:

  .. code:: sh

    $> docker exec -it 894aee038c5e bash

This opens an interactive terminal session inside the container.

Generate an SSL Key and Certificates
************************************

There are five steps to this generation. The paths where the different
files are stored are defined after the ``-out`` option in each command.

1. Generate a private key for your CSR.

  .. code:: sh

   $> openssl genrsa -out ca.key 2048

2. Generate a self-signed certificate for your local certificate authority.

  .. code:: sh

    $> openssl req -new -x509 -extensions v3_ca -key ca.key -out ca.crt -days 99999  -subj "/C=US/ST=Country/L=City/O=Organization/CN=scality.test"

3. Generate a key for Zenko CloudServer.

  .. code:: sh

    $> openssl genrsa -out test.key 2048

4. Generate a certificate signing request for S3 Server.

  .. code:: sh

    $> openssl req -new -key test.key -out test.csr -subj "/C=US/ST=Country/L=City/O=Organization/CN=*.scality.test"

5. Generate a local-CA-signed certificate for S3 Server.

  .. code:: sh

   $> openssl x509 -req -in test.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out test.crt -days 99999 -sha256

Update Zenko CloudServer ``config.json``
****************************************

Add a ``certFilePaths`` section to ``./config.json`` with the
appropriate paths:

  .. code:: json

        "certFilePaths": {
            "key": "./test.key",
            "cert": "./test.crt",
            "ca": "./ca.crt"
        }

Run Container with the New Config
****************************************

Exit the container by running ``$> exit``. Then, restart the container.
Normally, ``$> docker restart s3server`` does this.

Update Host Config
^^^^^^^^^^^^^^^^^^^^^^^

Associate Local IP Addresses with hostname
*******************************************

Use root permissions to edit the ``/etc/hosts`` file (in Linux, OS X, or
any other Unix) so that the localhost line looks like:

::

    127.0.0.1      localhost s3.scality.test

Copy the Local Certificate Authority from the Container
*********************************************************

In the above commands, the certificate authority is the file named ``ca.crt``.
Choose the path to save this file (``/root/ca.crt`` in the following example),
and run a command resembling:

.. code:: sh

    $> docker cp 894aee038c5e:/usr/src/app/ca.crt /root/ca.crt

Test the Config
^^^^^^^^^^^^^^^^^

If no aws-sdk is installed, run ``$> npm install aws-sdk``.

Then, paste the following script into a ``test.js`` file:

.. code:: js

   const AWS = require('aws-sdk');
    const fs = require('fs');
    const https = require('https');

    const httpOptions = {
        agent: new https.Agent({
            // path on your host of the self-signed certificate
            ca: fs.readFileSync('./ca.crt', 'ascii'),
        }),
    };

    const s3 = new AWS.S3({
        httpOptions,
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
        // The endpoint must be s3.scality.test, else SSL will not work
        endpoint: 'https://s3.scality.test:8000',
        sslEnabled: true,
        // With this setup, you must use path-style bucket access
        s3ForcePathStyle: true,
    });

    const bucket = 'cocoriko';

    s3.createBucket({ Bucket: bucket }, err => {
        if (err) {
            return console.log('err createBucket', err);
        }
        return s3.deleteBucket({ Bucket: bucket }, err => {
            if (err) {
                return console.log('err deleteBucket', err);
            }
            return console.log('SSL is cool!');
        });
    });

Run this script with ``$> nodejs test.js``. If all goes well, it
will output ``SSL is cool!``. Enjoy the added security!


.. |CircleCI| image:: https://circleci.com/gh/scality/S3.svg?style=svg
   :target: https://circleci.com/gh/scality/S3
.. |Scality CI| image:: http://ci.ironmann.io/gh/scality/S3.svg?style=svg&circle-token=1f105b7518b53853b5b7cf72302a3f75d8c598ae
   :target: http://ci.ironmann.io/gh/scality/S3
