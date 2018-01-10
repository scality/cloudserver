# Mongoclient

We introduce a new metadata backend called *mongoclient* for
[MongoDB](https://www.mongodb.com). This backend takes advantage of
MongoDB being a document store to store the metadata (bucket and
object attributes) as JSON objects.

## Overall Design

The mongoclient backend strictly follows the metadata interface that
stores bucket and object attributes, which consists of the methods
createBucket(), getBucketAttributes(), getBucketAndObject()
(attributes), putBucketAttributes(), deleteBucket(), putObject(),
getObject(), deleteObject(), listObject(), listMultipartUploads() and
the management methods getUUID(), getDiskUsage() and countItems(). The
mongoclient backend also knows how to deal with versioning, it is also
compatible with the various listing algorithms implemented in Arsenal.

FIXME: There should be a document describing the metadata (currently
duck-typing) interface.

### Why Using MongoDB for Storing Bucket and Object Attributes

We chose MongoDB for various reasons:

- MongoDB supports replication, especially through the Raft protocol.

- MongoDB supports a basic replication scheme called 'Replica Set' and
  more advanced sharding schemes if required.

- MongoDB is open source and an enterprise standard.

- MongoDB is a document store (natively supports JSON) and supports a
  very flexible search interface.

### Choice of Mongo Client Library

We chose to use the official MongoDB driver for NodeJS:
[https://github.com/mongodb/node-mongodb-native](https://github.com/mongodb/node-mongodb-native)

### Granularity for Buckets

We chose to have one collection for one bucket mapping. First because
in a simple mode of replication called 'replica set' it works from the
get-go, but if one or many buckets grow to big it is possible to use
more advanced schemes such as sharding. MongoDB supports a mix of
sharded and non-sharded collections.

### Storing Database Information

We need a special collection called the *Infostore* (stored under the
name __infostore which is impossible to create through the S3 bucket
naming scheme) to store specific database properties such as the
unique *uuid* for Orbit.

### Storing Bucket Attributes

We need to use a special collection called the *Metastore* (stored
under the name __metastore which is impossible to create through the
S3 bucket naming scheme).

### Versioning Format

We chose to keep the same versioning format that we use in some other
Scality products in order to facilitate the compatibility between the
different products.

FIXME: Document the versioning internals in the upper layers and
document the versioning format

### Dealing with Concurrency

We chose not to use transactions (aka
[https://docs.mongodb.com/manual/tutorial/perform-two-phase-commits/)
because it is a known fact there is an overhead of using them, and we
thought there was no real need for them since we could leverage Mongo
ordered operations guarantees and atomic writes.

Example of corner cases:

#### CreateBucket()

Since it is not possible to create a collection AND at the same time
register the bucket in the Metastore we chose to only update the
Metastore. A non-existing collection (NamespaceNotFound error in
Mongo) is one possible normal state for an empty bucket.

#### DeleteBucket()

In this case the bucket is *locked* by the upper layers (use of a
transient delete flag) so we don't have to worry about that and by the
fact the bucket is empty neither (which is also checked by the upper
layers).

We first drop() the collection and then we asynchronously delete the
bucket name entry from the metastore (the removal from the metastore
is atomic which is not absolutely necessary in this case but more
robust in term of design).

If we fail in between we still have an entry in the metastore which is
good because we need to manage the delete flag. For the upper layers
the operation has not completed until this flag is removed. The upper
layers will restart the deleteBucket() which is fine because we manage
the case where the collection does not exist.

#### PutObject() with a Version

We need to store the versioned object then update the master object
(the latest version). For this we use the
[BulkWrite](http://mongodb.github.io/node-mongodb-native/3.0/api/Collection.html#bulkWrite)
method. This is not a transaction but guarantees that the 2 operations
will happen sequentially in the MongoDB oplog. Indeed if the
BulkWrite() fails in between we would end up creating an orphan (which
is not critical) but if the operation succeeds then we are sure that
the master is always pointing to the right object. If there is a
concurrency between 2 clients then we are sure that the 2 groups of
operations will be clearly decided in the oplog (the last writer will
win).

#### DeleteObject()

This is probably the most complex case to manage because it involves a
lot of different cases:

##### Deleting an Object when Versioning is not Enabled

This case is a straightforward atomic delete. Atomicity is not really
required because we assume version IDs are random enough but it is
more robust to do so.

##### Deleting an Object when Versioning is Enabled

This case is more complex since we have to deal with the 2 cases:

Case 1: The caller asks for a deletion of a version which is not a master:
This case is a straight-forward atomic delete.

Case 2: The caller asks for a deletion of a version which is the master: In
this case we need to create a special flag called PHD (as PlaceHolDer)
that indicates the master is no longer valid (with a new unique
virtual version ID). We force the ordering of operations in a
bulkWrite() to first replace the master with the PHD flag and then
physically delete the version. If the call fail in between we will be
left with a master with a PHD flag. If the call succeeds we try to
find if the master with the PHD flag is left alone in such case we
delete it otherwise we trigger an asynchronous repair that will spawn
after AYNC_REPAIR_TIMEOUT=15s that will reassign the master to the
latest version.

In all cases the physical deletion or the repair of the master are
checked against the PHD flag AND the actual unique virtual version
ID. We do this to check against potential concurrent deletions,
repairs or updates. Only the last writer/deleter has the right to
physically perform the operation, otherwise it is superseded by other
operations.

##### Getting an object with a PHD flag

If the caller is asking for the latest version of an object and the
PHD flag is set we perform a search on the bucket to find the latest
version and we return it.

#### Listing Objects

The mongoclient backend implements a readable key/value stream called
*MongoReadStream* that follows the LevelDB duck typing interface used
in Arsenal/lib/algos listing algorithms. Note it does not require any
LevelDB package.

#### Generating the UUID

To avoid race conditions we always (try to) generate a new UUID and we
condition the insertion to the non-existence of the document.
