=================
Add A New Backend
=================

Supporting all possible public cloud storage APIs is CloudServer's
ultimate goal. As an open source project, contributions are welcome.

The first step is to get familiar with building a custom Docker image
for CloudServer.

Build a Custom Docker Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone Zenko's CloudServer, install all dependencies and start the
service:

.. code-block:: shell

  $ git clone https://github.com/scality/cloudserver
  $ cd cloudserver
  $ npm install
  $ npm start

.. tip::

    Some optional dependencies may fail, resulting in you seeing `NPM
    WARN` messages; these can safely be ignored.  Refer to the User
    documentation for all available options.

Build the Docker image:

.. code-block:: shell

     # docker build . -t
     # {{YOUR_DOCKERHUB_ACCOUNT}}/cloudserver:{{OPTIONAL_VERSION_TAG}}

Push the newly created Docker image to your own hub:

.. code-block:: shell

     # docker push
     # {{YOUR_DOCKERHUB_ACCOUNT}}/cloudserver:{{OPTIONAL_VERSION_TAG}}

.. note::

    To perform this last operation, you need to be authenticated with DockerHub

There are two main types of backend you could want Zenko to support:

== link:S3_COMPATIBLE_BACKENDS.adoc[S3 compatible data backends]

== link:NON_S3_COMPATIBLE_BACKENDS.adoc[Data backends using another protocol than the S3 protocol]

