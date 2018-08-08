======================
S3 compatible backends
======================

.. important:: S3 compatibility claims are a bit like idols: a lot think they are,
   but very few effectively meet all criteria ;) If the following steps
   don’t work for you, it’s likely to be because the S3 compatibility
   of your target backend is imperfect.

Adding support in Zenko’s Cloudserver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is the easiest case for backend support integration: there is nothing to do
but configuration!  Follow the steps described in our
:ref:`use-public-cloud` and make sure you:

- set ``details.awsEndpoint`` to your storage provider endpoint;

- use ``details.credentials`` and *not* ``details.credentialsProfile`` to set your
  credentials for that S3-compatible backend.

For example, if you’re using a Wasabi bucket as a backend, then your region
definition for that backend will look something like:
::

    "wasabi-bucket-zenkobucket": {
    "type": "aws_s3",
    "legacyAwsBehavior": true,
    "details": {
    "awsEndpoint": "s3.wasabisys.com",
    "bucketName": "zenkobucket",
    "bucketMatch": true,
    "credentials": {
    "accessKey": "\\{YOUR_WASABI_ACCESS_KEY}",
    "secretKey": "\\{YOUR_WASABI_SECRET_KEY}"
    }
    }
    },

Adding support in Zenko Orbit
#############################

This can only be done by our core developpers' team. If that’s what you’re
after, open a feature request on the `Zenko repository`_, and we will
get back to you after we evaluate feasability and maintainability.

.. _Zenko repository: https://www.github.com/scality/Zenko/issues/new
