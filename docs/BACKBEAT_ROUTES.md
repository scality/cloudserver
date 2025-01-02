# Backbeat routes

Backbeat routes are implemented in `lib/routes/routeBackbeat.js`.

This special router is responsible for handling all the requests that are
related to the Backbeat service. Backbeat may call any of the below APIs to
perform operations on either data or s3 objects (metadata).

These routes follow the same authorization and validation as the S3 routes:

- Authorize the request with support for Implicit Denies from the IAM service.
- Retrieve the bucket and object metadata if applicable.
- Evaluate the S3 Bucket Policies and ACLs before authorizing the request.
  - Backbeat routes are only authorized given the right permission, currently,
    `objectReplicate` as a unique permission for all these special routes.
  - In order to be authorized without S3 Bucket Policy, the caller must be
    authorized by the IAM service and the ACLs. Service accounts and accounts
    are allowed.
- Finally, evaluate the quotas before allowing the request to proceed.

## List of supported APIs

```plaintext
PUT /_/backbeat/metadata/<bucket name>/<object key>
```

To edit one existing S3 Object's metadata.
In the CRR case, this is used to put metadata for new objects.

```plaintext
GET /_/backbeat/metadata/<bucket name>/<object key>?versionId=<version id>
```

To get one existing S3 Object's metadata. Version id can be specified to get
the metadata of a specific version.

```plaintext
PUT /_/backbeat/data/<bucket name>/<object key>
```

To put directly to the storage layer the data for an existing S3 Object.

```plaintext
PUT /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=putobject
```

To put directly to the storage layer the data for an existing S3 Object.
Use case: Cross Region Replication (CRR).

```plaintext
PUT /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=putpart
```

To put directly to the storage layer the data for an existing S3 Object part.
Use case: Cross Region Replication (CRR).

```plaintext
DELETE /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=deleteobject
```

To delete the data for an existing S3 Object.
Use case: Cross Region Replication (CRR).

```plaintext
DELETE /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=abortmpu
```

To abort a multipart upload.
Use case: Cross Region Replication (CRR).

```plaintext
DELETE /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=deleteobjecttagging
```

To delete the tagging for an existing S3 Object.
Use case: Cross Region Replication (CRR).

```plaintext
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=initiatempu
```

To initiate a multipart upload.
Use case: Cross Region Replication (CRR).

```plaintext
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=completempu
```

To complete a multipart upload.
Use case: Cross Region Replication (CRR).

```plaintext
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>?operation=puttagging
```

To put the tagging for an existing S3 Object.
Use case: Cross Region Replication (CRR).

```plaintext
GET /_/backbeat/multiplebackendmetadata/<bucket name>/<object key>
```

To get the metadata for an existing S3 Object. Similar to a S3 HeadObject.
Use case: Cross Region Replication (CRR).

```plaintext
POST /_/backbeat/batchdelete
```

Delete a batch of objects froem the storage layer.
Use case: restored S3 Object expiration.

```plaintext
GET /_/backbeat/lifecycle/<bucket name>?list-type=current
```

To list current S3 Object versions from an S3 Bucket.
Use case: lifecycle listings.

```plaintext
GET /_/backbeat/lifecycle/<bucket name>?list-type=noncurrent
```

To list noncurrent S3 Object versions from an S3 Bucket.
Use case: lifecycle listings.

```plaintext
GET /_/backbeat/lifecycle/<bucket name>?list-type=orphan
```

To list delete markers from an S3 Bucket.
Use case: lifecycle listings.

```plaintext
POST /_/backbeat/index/<bucket name>?operation=add
```

To create an index for a bucket.
Use case: MongoDB backend.

```plaintext
POST /_/backbeat/index/<bucket name>?operation=delete
```

To delete an index for a bucket.
Use case: MongoDB backend.

```plaintext
GET /_/backbeat/index/<bucket name>
```

To get the index for a bucket.
Use case: MongoDB backend.
