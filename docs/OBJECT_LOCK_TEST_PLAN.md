# Object Lock Feature Test Plan

## Feature Component Description

Implementing Object Lock will introduce six new APIs:

- putObjectLockConfiguration
- getObjectLockConfiguration
- putObjectRetention
- getObjectRetention
- putObjectLegalHold
- getObjectLegalHold

Along with these APIs, putBucket, putObject, deleteObject, and multiObjectDelete
be affected. In Arsenal, both the BucketInfo and ObjectMD models will be
updated. Bucket policy and IAM policy permissions will be updated to include
the new API actions.

## Functional Tests

### putBucket tests

- passing option to enable object lock updates bucket metadata and enables
    bucket versioning

### putBucketVersioning tests

- suspending versioning on bucket with object lock enabled returns error

### putObject tests

- putting retention configuration on object should be allowed
- putting invalid retention configuration returns error

### getObject tests

- getting object with retention information should include retention information

### copyObject tests

- copying object with retention information should include retention information

### initiateMultipartUpload tests

- mpu object initiated with retention information should include retention
    information

### putObjectLockConfiguration tests

- putting configuration as non-bucket-owner user returns AccessDenied error
- disabling object lock on bucket created with object lock returns error
- enabling object lock on bucket created without object lock returns
    InvalidBucketState error
- enabling object lock with token on bucket created without object lock succeeds
- putting valid object lock configuration when bucket does not have object
    lock enabled returns error (InvalidRequest?)
- putting valid object lock configuration updates bucket metadata
- putting invalid object lock configuration returns error
    - ObjectLockEnabled !== "Enabled"
    - Rule object doesn't contain DefaultRetention key
    - Mode !== "GOVERNANCE" or "COMPLIANCE"
    - Days are not an integer
    - Years are not an integer

### getObjectLockConfiguration tests

- getting configuration as non-bucket-owner user returns AccessDenied error
- getting configuration when none is set returns
    ObjectLockConfigurationNotFoundError error
- getting configuration returns correct object lock configuration for bucket

### putObjectRetention

- putting retention as non-bucket-owner user returns AccessDenied error
- putting retention on object in bucket without object lock enabled returns
    InvalidRequest error
- putting valid retention period updates object metadata

### getObjectRetention

- getting retention as non-bucket-owner user returns AccessDenied error
- getting retention when none is set returns NoSuchObjectLockConfiguration
    error
- getting retention returns correct object retention period

### putObjectLegalHold

- putting legal hold as non-bucket-owner user returns AccessDenied error
- putting legal hold on object in bucket without object lock enabled returns
        InvalidRequest error
- putting valid legal hold updates object metadata

### getObjectLegalHold

- getting legal hold as non-bucket-owner user returns AccessDenied error
- getting legal hold when none is set returns NoSuchObjectLockConfiguration
    error
- getting legal hold returns correct object legal hold

## End to End Tests

### Scenarios

- Create bucket with object lock enabled. Put object. Put object lock
    configuration. Put another object.
    - Ensure object put before configuration does not have retention period set
    - Ensure object put after configuration does have retention period set

- Create bucket without object lock. Put object. Enable object lock with token
    and put object lock configuration. Put another object.
    - Ensure object put before configuration does not have retention period set
    - Ensure object put after configuration does have retention period set

- Create bucket with object lock enabled and put configuration with COMPLIANCE
    mode. Put object.
    - Ensure object cannot be deleted (returns AccessDenied error).
    - Ensure object cannot be overwritten.

- Create bucket with object lock enabled and put configuration with GOVERNANCE
    mode. Put object.
    - Ensure user without permission cannot delete object
    - Ensure user without permission cannot overwrite object
    - Ensure user with permission can delete object
    - Ensure user with permission can overwrite object
    - Ensure user with permission can lengthen retention period
    - Ensure user with permission cannot shorten retention period

- Create bucket with object lock enabled and put configuration. Edit bucket
    metadata so retention period is expired. Put object.
    - Ensure object can be deleted.
    - Ensure object can be overwritten.

- Create bucket with object lock enabled and put configuration. Edit bucket
    metadata so retention period is expired. Put object. Put new retention
    period on object.
    - Ensure object cannot be deleted.
    - Ensure object cannot be overwritten.

- Create bucket with object locked enabled and put configuration. Put object.
    Edit object metadata so retention period is past expiration.
    - Ensure object can be deleted.
    - Ensure object can be overwritten.

- Create bucket with object lock enabled and put configuration. Edit bucket
    metadata so retention period is expired. Put object. Put legal hold
    on object.
    - Ensure object cannot be deleted.
    - Ensure object cannot be overwritten.

- Create bucket with object lock enabled and put configuration. Put object.
    Check object retention. Change bucket object lock configuration.
    - Ensure object retention period has not changed with bucket configuration.

- Create bucket with object lock enabled. Put object with legal hold.
    - Ensure object cannot be deleted.
    - Ensure object cannot be overwritten.

- Create bucket with object lock enabled. Put object with legal hold. Remove
    legal hold.
    - Ensure object can be deleted.
    - Ensure object can be overwritten.
