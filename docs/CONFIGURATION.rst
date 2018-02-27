Configuration
=================

.. figure:: ../res/scality-cloudserver-logo.png
   :alt: Zenko CloudServer logo

|CircleCI| |Scality CI|

Introduction
------------

There are three configuration files for your Scality Zenko CloudServer:

1. ``conf/authdata.json``, described above for authentication

2. ``locationConfig.json``, to set up configuration options for

   where data will be saved

3. ``config.json``, for general configuration options

Location Configuration
----------------------

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


.. WARNING:
  Each locationConstraint must include the ``type``, ``legacyAwsBehavior``, and
  ``details`` keys.
  All other keys are optional.

``type``
~~~~~~~~

``type`` indicates which backend will be used for that region. Currently,
``mem``, ``file``, ``scality``, ``aws_s3``, ``azure``, and ``gcp`` are the
supported values.

.. NOTE::
   Backblaze B2 support is coming soon! Type will be ``b2``


``legacyAwsBehavior``
~~~~~~~~~~~~~~~~~~~~~

``legacyAwsBehavior`` indicates whether the region will have the same behavior
as the AWS S3 'us-east-1' region.
It's a boolean, thus accepts ``true`` or ``false`` as its only values.


.. NOTE:
   If the locationConstraint type is scality, ``details`` should contain
   connector information for sproxyd. If the locationConstraint type is mem
   or file, ``details`` should be empty.

``details``
~~~~~~~~~~~

``details`` should be an empty object for mem type locations. It can take a
number of informations, some specific to the backend type (please refer to
USING_PUBLIC_CLOUDS.rst), some generic, which we will cover here.

``details.https``
~~~~~~~~~~~~~~~~~
``details.https`` indicates whether the location will be accessed using SSL or
not.
It's a boolean, thus accepts ``true`` or ``false`` as its only values.

``details.bucketMatch``
~~~~~~~~~~~~~~~~~~~~~~~

``details.bucketMatch`` indicates whether local Cloudserver bucketnames will be
used as prefix in the target public cloud bucket, or not.
It's a boolean, thus accepts ``true`` or ``false`` as its only values.

``details.proxy``
~~~~~~~~~~~~~~~~~

``details.proxy`` should be of the form
``protocol//hostname:port/username:password``












For instance, the following sets the ``localhost`` endpoint to the
``myLocationConstraint`` data backend defined above:
   Once you have your locationConstraints in your locationConfig.json, you
   can specify a default locationConstraint for each of your endpoints.

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
