# Object Locking using S3 Object Lock

Object locking is a feature built for addressing use cases in which the
Write Once Read Many (WORM) model is required. The feature will be built in line
with the AWS specification. Any extensions to the specification will be explicitly
documented. The feature implementation has the goal of meeting
SEC 17a-4 compliance.

## Requirements

* Object lock flag must be set during bucket creation
* Versioning has to be enabled on the bucket
* Object lock must be enabled on a bucket in order to write a lock configuration using the PUT Object Lock API
  Configuration api that has object lock flag set

PS: AWS specification does not have a way of setting object lock on existing
buckets cannot have object lock flag set through the api. To enable
object lock flag on an existing bucket, a migration needs to be performed
on the bucket using a tool. Objects that were created before the lock is set
are not protected by Object Locking

## What happens when an object is locked

When set on an object version, a lock prevents deletes and overwrites on that
version until the lock expires. However, the lock doesn't prevent creation of
delete markers or new versions on top of the locked version.
Object locking is also immune to Lifecycle actions i.e a Lifecycle expiry rule
cannot delete an object version until the lock on the object expires.

## Controlling locking of an object

S3 provides a few ways through which the lock configuration of an object can
be set

### Retention Modes

Both Governance and Compliance modes retain the lock on an object until the set
retention period expires.

#### Governance mode

In addition to preventing deletions on an object version by users,
`GOVERNANCE` mode allows delegating permission to certain users to override
the lock settings, for example by changing the retention mode, period
or deleting the object altogether.
Either the root (account) or a user with `s3:BypassGovernanceRetention`
permission can send a delete request with
`x-amz-bypass-governance-retention:true` header to override and delete the
object.

#### Compliance mode

When a lock is placed on an object using `COMPLIANCE` mode, the object version
cannot be deleted by any user until the retention period expires.
This includes root user (account credentials) in the account and no other user
can be given permission either to override the settings or delete the version.

### Retention Period

Retention period defines the term during which the object is protected from
deletes.
Retention period can be set on the bucket level, which acts as a default for all
objects put in the bucket after the setting is applied. The default retention
period set on a bucket can be overridden on the object level by setting the date
and time using the header `x-amz-object-lock-retain-until-date`.

**Buckets** -  retention period can be set in either `days` or `years`, but not
both at the same time.

**Objects** - retention period can be set as date and time when the object is
expected to expire.

### Legal Hold

Legal hold can be enabled on an object version. Once a legal hold is enabled,
regardless of the object's retention date or retention mode, the object version
cannot be deleted until the legal hold is removed.
Legal hold can be set on an object version during PUT Object request by setting
`x-amz-object-lock-legal-hold` header or using PUT Object Legal Hold API
request.
Root users with account credentials or IAM users who are given the permission
`s3:PutObjectLegalHold` are allowed to set Legal hold on an object version.

### Implementation

#### Storing Object lock configuration

Object lock can be enabled on a bucket during bucket creation using
`x-amz-bucket-object-lock-enabled` header. This is stored along with the
bucket's metadata defined in the BucketInfo model.
Any default lock settings (governance/compliance mode, days/years for retention
period) defined using `PutObjectLockConfiguration` request are stored
as part of the bucket metadata.
When a PUT Object request is received, the lock configuration is evaluated and
the lock expiration date and time is calculated and set as `retain-until-date`
property on the object's metadata.
Lock configuration on a PUT object request is evaluated the following order

1. Bucket's configuration for retention mode/period
2. Object's configuration in the PUT Object for retention mode/period

The object's settings override the bucket's settings when calculating the
`retain-until-date` date and time to be stored on the object's metadata.
The lock configuration on an object version can also be changed using
`PutObjectRetention` api request, when the object has no prior retention mode
set or if the object has `GOVERNANCE` mode set and the user has appropriate
permission to make the request.

Note: Delete markers do not have any object lock protection

#### Processing DELETE requests

Whenever requests such as DELETE object using version-id,
Multi-Object DELETE specifying version-id or a Lifecycle action to
permanently delete the object version are received, the current date and time is
evaluated against the `retain-until-date` set on the object and the client will
receive an `Access Denied` error if the current date and time is less than the
`retain-until-date` set on the object.
When the current date and time exceeds the `retain-until-date`, deletes on the
object version are automatically allowed. There is no cleanup action to remove
the `retain-until-date` set on the object version's metadata once the
`retain-until-date` expires.
DELETE object requests without a version id result in creation of delete markers
on top of the object version, even if the object has a lock configuration set
and the `retain-until-date` is current.

#### Lifecycle actions

Lifecycle jobs can create delete markers on the object but cannot permanently delete an
object version that is locked until the lock expires.

#### Replication

The lock configuration on the object version in a source bucket is copied over
to the destination bucket only if object lock is enabled on the destination
bucket. Otherwise, the lock is ignored in the destination bucket.

## APIs covering S3 Object lock

* **Put Bucket** - extend the bucket creation API to include configuration for
  enabling object lock on the bucket

  **Note:** Versioning is automatically enabled on buckets that have object lock
  enabled as part of a PUT Bucket request

* **Put Object** - extend put object API to parse `x-amz-object-lock-mode`,
  `x-amz-object-lock-retain-until-date`, `x-amz-object-lock-legal-hold`
  headers and store the configuration on an object.

* **Copy Object** - extend the API to accept the same lock configuration
  headers as the PUT Object request

* **Create Multipart Upload** - extend the API to accept the same lock
  configuration headers as the PUT Object request

* **Put Object Lock Configuration** - allows setting a default lock
  configuration for objects that are going to be stored in the bucket. This
  request is accepted only on buckets that have object lock enabled.

* **Get Object Lock Configuration** - gets the object lock configuration set on
  the bucket metadata

* **Put Object Retention** - sets the retention mode/period configuration on a
  object version

* **Get Object Retention**- gets the retention mode/period configuration set on a
  object version

## FAQ

1. Q: What error code does the client get when a delete version request is sent on
   a locked object?

   A: Access Denied

2. Q: Can an object set with GOVERNANCE retention mode be deleted with acccount
   credentials?

   A: Yes, if the delete object request is sent is along with
   `x-amz-bypass-governance-retention:true` header. This header needs to be
   explicitly set and is disabled by default on all AWS SDKs and AWS CLI.

3. Q: Does placing a lock on an object version generate a new version?

   A: No. It updates the object version with retention settings without
   generating a new version-id or updating the `Last-Modified` date on the
   object version.
