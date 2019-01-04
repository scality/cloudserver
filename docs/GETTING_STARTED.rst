Getting Started
===============

.. figure:: ../res/scality-cloudserver-logo.png
   :alt: Zenko CloudServer logo

|CircleCI| |Scality CI|

Dependencies
------------

Building and running the Scality Zenko CloudServer requires node.js 6.9.5 and
npm v3. Up-to-date versions can be found at
`Nodesource <https://github.com/nodesource/distributions>`__.

Installation
------------

1. Clone the source code

   .. code-block:: shell

      $ git clone https://github.com/scality/cloudserver.git

2. Go to the cloudserver directory and use npm to install the js dependencies.

   .. code-block:: shell

      $ cd cloudserver
      $ npm install

Running CloudServer with a File Backend
---------------------------------------

.. code-block:: shell

   $ npm start

This starts a Zenko CloudServer on port 8000. Two additional ports, 9990 
and 9991, are also open locally for internal transfer of metadata and 
data, respectively.

The default access key is accessKey1. The secret key is verySecretKey1.

By default, metadata files are saved in the localMetadata directory and 
data files are saved in the localData directory in the local ./cloudserver 
directory. These directories are pre-created within the repository. To 
save data or metadata in different locations, you must specify them using 
absolute paths. Thus, when starting the server:

.. code-block:: shell

   $ mkdir -m 700 $(pwd)/myFavoriteDataPath
   $ mkdir -m 700 $(pwd)/myFavoriteMetadataPath
   $ export S3DATAPATH="$(pwd)/myFavoriteDataPath"
   $ export S3METADATAPATH="$(pwd)/myFavoriteMetadataPath"
   $ npm start

Running CloudServer with Multiple Data Backends
-----------------------------------------------

.. code-block:: shell

   $ export S3DATA='multiple'
   $ npm start

This starts a Zenko CloudServer on port 8000. 

The default access key is accessKey1. The secret key is verySecretKey1.

With multiple backends, you can choose where each object is saved by setting
the following header with a location constraint in a PUT request:

.. code-block:: shell

    'x-amz-meta-scal-location-constraint':'myLocationConstraint'

If no header is sent with a PUT object request, the bucket’s location
constraint determines where the data is saved. If the bucket has no
location constraint, the endpoint of the PUT request determines location.

See the Configuration_ section to set location constraints.

Run CloudServer with an In-Memory Backend
-----------------------------------------

.. code-block:: shell

   $ npm run mem_backend

This starts a Zenko CloudServer on port 8000. 

The default access key is accessKey1. The secret key is verySecretKey1.

Run CloudServer for Continuous Integration Testing or in Production with Docker
-------------------------------------------------------------------------------

`DOCKER <./DOCKER>`__

Testing
~~~~~~~

Run unit tests with the command:

.. code-block:: shell

   $ npm test

Run multiple-backend unit tests with:

.. code-block:: shell

   $ CI=true S3DATA=multiple npm start
   $ npm run multiple_backend_test

Run the linter with:

.. code-block:: shell

   $ npm run lint

Running Functional Tests Locally
--------------------------------

To pass AWS and Azure backend tests locally, modify 
tests/locationConfig/locationConfigTests.json so that ``awsbackend`` 
specifies the bucketname of a bucket you have access to based on your
credentials, and modify ``azurebackend`` with details for your Azure account.

The test suite requires additional tools, **s3cmd** and **Redis**
installed in the environment the tests are running in.

1. Install `s3cmd <http://s3tools.org/download>`__

2. Install `redis <https://redis.io/download>`__ and start Redis.

3. Add localCache section to ``config.json``:

   .. code:: json

      "localCache": {
        "host": REDIS_HOST,
        "port": REDIS_PORT
	}

   where ``REDIS_HOST`` is the Redis instance IP address (``"127.0.0.1"``
   if Redis is running locally) and ``REDIS_PORT`` is the Redis instance
   port (``6379`` by default)

4. Add the following to the local etc/hosts file:

   .. code-block:: shell

      127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com

5. Start Zenko CloudServer in memory and run the functional tests:

   .. code-block:: shell

      $ CI=true npm run mem_backend
      $ CI=true npm run ft_test

.. _Configuration:

Configuration
-------------

There are three configuration files for Zenko CloudServer:

* ``conf/authdata.json``, for authentication.

* ``locationConfig.json``, to configure where data is saved.

* ``config.json``, for general configuration options.

.. _location-configuration:

Location Configuration
~~~~~~~~~~~~~~~~~~~~~~

You must specify at least one locationConstraint in locationConfig.json
(or leave it as pre-configured).

You must also specify 'us-east-1' as a locationConstraint. If you put a 
bucket to an unknown endpoint and do not specify a locationConstraint in
the PUT bucket call, us-east-1 is used.

For instance, the following locationConstraint saves data sent to
``myLocationConstraint`` to the file backend:

.. code:: json

   "myLocationConstraint": {
       "type": "file",
       "legacyAwsBehavior": false,
       "details": {}
   },

Each locationConstraint must include the ``type``, ``legacyAwsBehavior``,
and ``details`` keys. ``type`` indicates which backend is used for that
region. Supported backends are mem, file, and scality.``legacyAwsBehavior``
indicates whether the region behaves the same as the AWS S3 'us-east-1' 
region. If the locationConstraint type is ``scality``, ``details`` must 
contain connector information for sproxyd. If the locationConstraint type
is ``mem`` or ``file``, ``details`` must be empty.

Once locationConstraints is set in locationConfig.json, specify a default
locationConstraint for each endpoint.

For instance, the following sets the ``localhost`` endpoint to the
``myLocationConstraint`` data backend defined above:

.. code:: json

    "restEndpoints": {
         "localhost": "myLocationConstraint"
    },

To use an endpoint other than localhost for Zenko CloudServer, the endpoint
must be listed in ``restEndpoints``. Otherwise, if the server is running
with a:

*  **file backend**: The default location constraint is ``file``
*  **memory backend**: The default location constraint is ``mem``

Endpoints
~~~~~~~~~

The Zenko CloudServer supports endpoints that are rendered in either:

* path style: http://myhostname.com/mybucket or
* hosted style: http://mybucket.myhostname.com

However, if an IP address is specified for the host, hosted-style requests
cannot reach the server. Use path-style requests in that case. For example,
if you are using the AWS SDK for JavaScript, instantiate your client like this:

.. code:: js

    const s3 = new aws.S3({
       endpoint: 'http://127.0.0.1:8000',
       s3ForcePathStyle: true,
    });

Setting Your Own Access and Secret Key Pairs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Credentials can be set for many accounts by editing ``conf/authdata.json``, 
but use the ``SCALITY_ACCESS_KEY_ID`` and ``SCALITY_SECRET_ACCESS_KEY`` 
environment variables to specify your own credentials.

_`scality-access-key-id-and-scality-secret-access-key`

SCALITY\_ACCESS\_KEY\_ID and SCALITY\_SECRET\_ACCESS\_KEY
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These variables specify authentication credentials for an account named
“CustomAccount”.

.. note:: Anything in the ``authdata.json`` file is ignored.

.. code-block:: shell

   $ SCALITY_ACCESS_KEY_ID=newAccessKey SCALITY_SECRET_ACCESS_KEY=newSecretKey npm start

.. _Using_SSL:

Using SSL
~~~~~~~~~

To use https with your local CloudServer, you must set up
SSL certificates. 

1. Deploy CloudServer using `our DockerHub page
   <https://hub.docker.com/r/zenko/cloudserver/>`__ (run it with a file
   backend).

   .. Note:: If Docker is not installed locally, follow the
      `instructions to install it for your distribution 
      <https://docs.docker.com/engine/installation/>`__

2. Update the CloudServer container’s config 

   Add your certificates to your container. To do this, 
   #. exec inside the CloudServer container. 

   #. Run ``$> docker ps`` to find the container’s ID (the corresponding 
      image name is ``scality/cloudserver``. 
      
   #. Copy the corresponding container ID (``894aee038c5e`` in the present
      example), and run:

      .. code-block:: shell

         $> docker exec -it 894aee038c5e bash

      This puts you inside your container, using an interactive terminal.

3. Generate the SSL key and certificates. The paths where the different
   files are stored are defined after the ``-out`` option in each of the 
   following commands.

    #. Generate a private key for your certificate signing request (CSR):

       .. code-block:: shell

	  $> openssl genrsa -out ca.key 2048

    #. Generate a self-signed certificate for your local certificate 
       authority (CA):

       .. code:: shell

	  $> openssl req -new -x509 -extensions v3_ca -key ca.key -out ca.crt -days 99999  -subj "/C=US/ST=Country/L=City/O=Organization/CN=scality.test"

    #. Generate a key for the CloudServer:

       .. code:: shell

          $> openssl genrsa -out test.key 2048

    #. Generate a CSR for CloudServer:

       .. code:: shell

          $> openssl req -new -key test.key -out test.csr -subj "/C=US/ST=Country/L=City/O=Organization/CN=*.scality.test"

    #. Generate a certificate for CloudServer signed by the local CA:

       .. code:: shell

          $> openssl x509 -req -in test.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out test.crt -days 99999 -sha256

4. Update Zenko CloudServer ``config.json``. Add a ``certFilePaths`` 
   section to ``./config.json`` with appropriate paths:

   .. code:: json

        "certFilePaths": {
            "key": "./test.key",
            "cert": "./test.crt",
            "ca": "./ca.crt"
        }

5. Run your container with the new config. 

   #. Exit the container by running ``$> exit``. 

   #. Restart the container with ``$> docker restart cloudserver``.

6. Update the host configuration by adding s3.scality.test 
   to /etc/hosts:

   .. code:: bash

      127.0.0.1      localhost s3.scality.test

7. Copy the local certificate authority (ca.crt in step 4) from your 
   container. Choose the path to save this file to (in the present 
   example, ``/root/ca.crt``), and run:

   .. code:: shell

      $> docker cp 894aee038c5e:/usr/src/app/ca.crt /root/ca.crt

   .. note:: Your container ID will be different, and your path to 
	     ca.crt may be different.

Test the Config
^^^^^^^^^^^^^^^

If aws-sdk is not installed, run ``$> npm install aws-sdk``. 

Paste the following script into a file named "test.js":

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

Now run this script with:

.. code::

   $> nodejs test.js
 
On success, the script outputs ``SSL is cool!``.


.. |CircleCI| image:: https://circleci.com/gh/scality/S3.svg?style=svg
   :target: https://circleci.com/gh/scality/S3
.. |Scality CI| image:: http://ci.ironmann.io/gh/scality/S3.svg?style=svg&circle-token=1f105b7518b53853b5b7cf72302a3f75d8c598ae
   :target: http://ci.ironmann.io/gh/scality/S3
