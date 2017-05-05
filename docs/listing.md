# Listing

## Listing Types

We use three different types of metadata listing for various operations.
Here are the scenarios we use each for:

- 'Delimiter' - when no versions are possible in the bucket since it is an
   internally-used only bucket which is not exposed to a user. Namely,
  1. to list objects in the "user's bucket" to respond to a GET SERVICE
  request and
  2. to do internal listings on an MPU shadow bucket to complete multipart
  upload operations.
- 'DelimiterVersion' - to list all versions in a bucket
- 'DelimiterMaster' - to list just the master versions of objects in a bucket

## Algorithms

The algorithms for each listing type can be found in the open-source
[scality/Arsenal](https://github.com/scality/Arsenal) repository, in [lib/algos/list](https://github.com/scality/Arsenal/tree/master/lib/algos/list).
