# S3 Protocol Test Plan

## Unit Tests

### Architecture

- s3 running on a server with a testing framework such as Mocha with
  an assertion library.

### Features tested

- Authentication
  - Building signature
  - Checking timestamp
  - Canonicalization
  - Error Handling

- Bucket Metadata API
  - GET, PUT, DELETE Bucket Metadata

- s3 API
  - GET Service
  - GET, PUT, DELETE, HEAD Object
  - GET, PUT, DELETE, HEAD Bucket
  - ACL's
  - Bucket Policies
  - Lifecycle
  - Range requests
  - Multi-part upload

- Routes
  - GET, PUT, PUTRAW, DELETE, HEAD for objects and buckets

## Functional Tests

### Architecture

- A Docker instance running S3 and a Docker instance running certain S3
  clients (Node SDK, S3cmd and AWS S3API CLI).

### Features tested

- Same as unit tests plus concurrent access
- Compliance with S3 clients
- http, https, ipv4, ipv6, hosting of website in bucket
- ssl integration
- Accuracy of s3 request logging

## Integration Tests

### Architecture

- Several s3 Docker instances, Metadata Docker instances,
  Data Docker instances and several Vault Docker instances.

### Features tested

- Ability to access the same buckets through different s3 connectors.
- Creating and destroying access and secret keys.
- Stopping and restarting Docker instances of s3, Vault, Metadata and Data
  under load.
- Load balancing between s3 instances?
- Throughput of the system increases as we add s3 instances.
- Operation time as we add disk (the more disk, the faster the system should be
  per operation).
- Large buckets (1 billion objects in a single bucket).
- Large objects (10 terabytes).
- End to end streaming ability for large objects (put from a client through
  s3 to Data and get from Data to s3 out to a
  client).
- End to end range requests.
- Load testing during extended period of time to observe side effects (i.e.
  compaction of metadata).
- Deleting a large number of objects at once and observe effect on data
  (recovery of free space).
- Failures of metadata under load.
- Effect at s3 level of data and metadata being out of sync.
