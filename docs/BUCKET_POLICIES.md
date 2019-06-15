# Bucket Policy Documentation

## Description

Bucket policy is a method of controlling access to a user's account at the
resource level.
There are three associated APIs:
- PUT Bucket policy (see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTpolicy.html)
- GET Bucket policy (see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETpolicy.html)
- DELETE Bucket policy (see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEpolicy.html)
More information on bucket policies  in general can be found at
https://docs.aws.amazon.com/AmazonS3/latest/dev/using-iam-policies.html.

## Requirements

The root owner of a bucket will always be able to perform any of the three
bucket policy-related operations, even if permission is explicitly denied.
All other users must have permission to performed the desired operation.

## Design

On a PUTBucketPolicy request, the user provides a policy in JSON format.
The policy is evaluated against our policy schema and, once validated,
is stored as part of the bucket's metadata.
On a GETBucketPolicy request, the policy is retrieved from the bucket's
metadata.
On a DELETEBucketPolicy request, the policy is deleted from the bucket's
metadata.

All other APIs are updated to check bucket policy authorization if the action
is performed on a bucket with a bucket policy attached.

### Policy Validation

