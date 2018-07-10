Getting Started
=================

.. figure:: ../res/scality-cloudserver-logo.png
   :alt: Zenko CloudServer logo

|CircleCI| |Scality CI|

Installation
------------

Dependencies
~~~~~~~~~~~~

Building and running the Scality Zenko CloudServer requires node.js 6.9.5 and
npm v3 . Up-to-date versions can be found at
`Nodesource <https://github.com/nodesource/distributions>`__.

Clone source code
~~~~~~~~~~~~~~~~~

.. code:: shell

    git clone https://github.com/scality/S3.git

Install js dependencies
~~~~~~~~~~~~~~~~~~~~~~~

Go to the ./S3 folder,

.. code:: shell

    npm install

Run it with a file backend
--------------------------

.. code:: shell

    npm start

This starts an Zenko CloudServer on port 8000. Two additional ports 9990 and
9991 are also open locally for internal transfer of metadata and data,
respectively.

The default access key is accessKey1 with a secret key of
verySecretKey1.

By default the metadata files will be saved in the localMetadata
directory and the data files will be saved in the localData directory
within the ./S3 directory on your machine. These directories have been
pre-created within the repository. If you would like to save the data or
metadata in different locations of your choice, you must specify them
with absolute paths. So, when starting the server:

.. code:: shell

    mkdir -m 700 $(pwd)/myFavoriteDataPath
    mkdir -m 700 $(pwd)/myFavoriteMetadataPath
    export S3DATAPATH="$(pwd)/myFavoriteDataPath"
    export S3METADATAPATH="$(pwd)/myFavoriteMetadataPath"
    npm start

Run it with multiple data backends
----------------------------------

.. code:: shell

    export S3DATA='multiple'
    npm start

This starts an Zenko CloudServer on port 8000. The default access key is
accessKey1 with a secret key of verySecretKey1.

With multiple backends, you have the ability to choose where each object
will be saved by setting the following header with a locationConstraint
on a PUT request:

.. code:: shell

    'x-amz-meta-scal-location-constraint':'myLocationConstraint'

If no header is sent with a PUT object request, the location constraint
of the bucket will determine where the data is saved. If the bucket has
no location constraint, the endpoint of the PUT request will be used to
determine location.

See the Configuration section below to learn how to set location
constraints.

Run it with an in-memory backend
--------------------------------

.. code:: shell

    npm run mem_backend

This starts an Zenko CloudServer on port 8000. The default access key is
accessKey1 with a secret key of verySecretKey1.

Run it for continuous integration testing or in production with Docker
----------------------------------------------------------------------

`DOCKER <../DOCKER/>`__

Testing
-------

You can run the unit tests with the following command:

.. code:: shell

    npm test

You can run the multiple backend unit tests with:

.. code:: shell
    CI=true S3DATA=multiple npm start
    npm run multiple_backend_test

You can run the linter with:

.. code:: shell

    npm run lint

Running functional tests locally:

For the AWS backend and Azure backend tests to pass locally,
you must modify tests/locationConfigTests.json so that awsbackend
specifies a bucketname of a bucket you have access to based on
your credentials profile and modify "azurebackend" with details
for your Azure account.

The test suite requires additional tools, **s3cmd** and **Redis**
installed in the environment the tests are running in.

-  Install `s3cmd <http://s3tools.org/download>`__
-  Install `redis <https://redis.io/download>`__ and start Redis.
-  Add localCache section to your ``config.json``:

::

    "localCache": {
        "host": REDIS_HOST,
        "port": REDIS_PORT
    }

where ``REDIS_HOST`` is your Redis instance IP address (``"127.0.0.1"``
if your Redis is running locally) and ``REDIS_PORT`` is your Redis
instance port (``6379`` by default)

-  Add the following to the etc/hosts file on your machine:

.. code:: shell

    127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com

-  Start the Zenko CloudServer in memory and run the functional tests:

.. code:: shell

    CI=true npm run mem_backend
    CI=true npm run ft_test

Configuration
-------------

There are three configuration files for your Scality Zenko CloudServer:

1. ``conf/authdata.json``, described above for authentication

2. ``locationConfig.json``, to set up configuration options for

   where data will be saved

3. ``config.json``, for general configuration options

Location Configuration
~~~~~~~~~~~~~~~~~~~~~~

You must specify at least one locationConstraint in your
locationConfig.json (or leave as pre-configured).

You must also specify 'us-east-1' as a locationConstraint so if you only
define one locationConstraint, that would be it. If you put a bucket to
an unknown endpoint and do not specify a locationConstraint in the put
bucket call, us-east-1 will be used.

For instance, the following locationConstraint will save data sent to
``myLocationConstraint`` to the file backend:

.. code:: json

    "myLocationConstraint": {
        "type": "file",
        "legacyAwsBehavior": false,
        "details": {}
    },

Each locationConstraint must include the ``type``,
``legacyAwsBehavior``, and ``details`` keys. ``type`` indicates which
backend will be used for that region. Currently, mem, file, and scality
are the supported backends. ``legacyAwsBehavior`` indicates whether the
region will have the same behavior as the AWS S3 'us-east-1' region. If
the locationConstraint type is scality, ``details`` should contain
connector information for sproxyd. If the locationConstraint type is mem
or file, ``details`` should be empty.

Once you have your locationConstraints in your locationConfig.json, you
can specify a default locationConstraint for each of your endpoints.

For instance, the following sets the ``localhost`` endpoint to the
``myLocationConstraint`` data backend defined above:

.. code:: json

    "restEndpoints": {
         "localhost": "myLocationConstraint"
    },

If you would like to use an endpoint other than localhost for your
Scality Zenko CloudServer, that endpoint MUST be listed in your
``restEndpoints``. Otherwise if your server is running with a:

-  **file backend**: your default location constraint will be ``file``

-  **memory backend**: your default location constraint will be ``mem``

Endpoints
~~~~~~~~~

Note that our Zenko CloudServer supports both:

-  path-style: http://myhostname.com/mybucket
-  hosted-style: http://mybucket.myhostname.com

However, hosted-style requests will not hit the server if you are using
an ip address for your host. So, make sure you are using path-style
requests in that case. For instance, if you are using the AWS SDK for
JavaScript, you would instantiate your client like this:

.. code:: js

    const s3 = new aws.S3({
       endpoint: 'http://127.0.0.1:8000',
       s3ForcePathStyle: true,
    });

Setting your own access key and secret key pairs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set credentials for many accounts by editing
``conf/authdata.json`` but if you want to specify one set of your own
credentials, you can use ``SCALITY_ACCESS_KEY_ID`` and
``SCALITY_SECRET_ACCESS_KEY`` environment variables.

SCALITY\_ACCESS\_KEY\_ID and SCALITY\_SECRET\_ACCESS\_KEY
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These variables specify authentication credentials for an account named
"CustomAccount".

Note: Anything in the ``authdata.json`` file will be ignored.

.. code:: shell

    SCALITY_ACCESS_KEY_ID=newAccessKey SCALITY_SECRET_ACCESS_KEY=newSecretKey npm start


Scality with SSL
~~~~~~~~~~~~~~~~~~~~~~

If you wish to use https with your local Zenko CloudServer, you need to set up
SSL certificates. Here is a simple guide of how to do it.

Deploying Zenko CloudServer
^^^^^^^^^^^^^^^^^^^

First, you need to deploy **Zenko CloudServer**. This can be done very easily
via `our **DockerHub**
page <https://hub.docker.com/r/scality/s3server/>`__ (you want to run it
with a file backend).

    *Note:* *- If you don't have docker installed on your machine, here
    are the `instructions to install it for your
    distribution <https://docs.docker.com/engine/installation/>`__*

Updating your Zenko CloudServer container's config
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You're going to add your certificates to your container. In order to do
so, you need to exec inside your Zenko CloudServer container. Run a
``$> docker ps`` and find your container's id (the corresponding image
name should be ``scality/s3server``. Copy the corresponding container id
(here we'll use ``894aee038c5e``, and run:

.. code:: sh

    $> docker exec -it 894aee038c5e bash

You're now inside your container, using an interactive terminal :)

Generate SSL key and certificates
**********************************

There are 5 steps to this generation. The paths where the different
files are stored are defined after the ``-out`` option in each command

.. code:: sh

    # Generate a private key for your CSR
    $> openssl genrsa -out ca.key 2048
    # Generate a self signed certificate for your local Certificate Authority
    $> openssl req -new -x509 -extensions v3_ca -key ca.key -out ca.crt -days 99999  -subj "/C=US/ST=Country/L=City/O=Organization/CN=scality.test"

    # Generate a key for Zenko CloudServer
    $> openssl genrsa -out test.key 2048
    # Generate a Certificate Signing Request for S3 Server
    $> openssl req -new -key test.key -out test.csr -subj "/C=US/ST=Country/L=City/O=Organization/CN=*.scality.test"
    # Generate a local-CA-signed certificate for S3 Server
    $> openssl x509 -req -in test.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out test.crt -days 99999 -sha256

Update Zenko CloudServer ``config.json``
**********************************

Add a ``certFilePaths`` section to ``./config.json`` with the
appropriate paths:

.. code:: json

        "certFilePaths": {
            "key": "./test.key",
            "cert": "./test.crt",
            "ca": "./ca.crt"
        }

Run your container with the new config
****************************************

First, you need to exit your container. Simply run ``$> exit``. Then,
you need to restart your container. Normally, a simple
``$> docker restart s3server`` should do the trick.

Update your host config
^^^^^^^^^^^^^^^^^^^^^^^

Associates local IP addresses with hostname
*******************************************

In your ``/etc/hosts`` file on Linux, OS X, or Unix (with root
permissions), edit the line of localhost so it looks like this:

::

    127.0.0.1      localhost s3.scality.test

Copy the local certificate authority from your container
*********************************************************

In the above commands, it's the file named ``ca.crt``. Choose the path
you want to save this file at (here we chose ``/root/ca.crt``), and run
something like:

.. code:: sh

    $> docker cp 894aee038c5e:/usr/src/app/ca.crt /root/ca.crt

Test your config
^^^^^^^^^^^^^^^^^

If you do not have aws-sdk installed, run ``$> npm install aws-sdk``. In
a ``test.js`` file, paste the following script:

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

Now run that script with ``$> nodejs test.js``. If all goes well, it
should output ``SSL is cool!``. Enjoy that added security!


.. |CircleCI| image:: https://circleci.com/gh/scality/S3.svg?style=svg
   :target: https://circleci.com/gh/scality/S3
.. |Scality CI| image:: http://ci.ironmann.io/gh/scality/S3.svg?style=svg&circle-token=1f105b7518b53853b5b7cf72302a3f75d8c598ae
   :target: http://ci.ironmann.io/gh/scality/S3
