# Bucket Policy Documentation

## Description

Bucket policy is a method of controlling access to a user's account at the
resource level.
There are three associated APIs:
- PUT Bucket policy (see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTpolicy.html)
- GET Bucket policy (see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETpolicy.html)
- DELETE Bucket policy (see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEpolicy.html)
More information on bucket policies in general can be found at
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

For general guidelines for bucket policy structure, see examples here: https://docs.aws.amazon.com/AmazonS3/latest/dev//example-bucket-policies.html.

Each bucket policy statement object requires at least four keys: Effect, Principle, Resource, and Action.

Effect defines the effect of the policy and can have a string value of either "Allow" or "Deny."
Resource defines to which bucket or list of buckets a policy is attached. It can be either a single bucket ARN string or an array of ARNs.
Action lists which action(s) the policy controls. Its value can also be either a string or array of S3 APIs. Each action is the API name prepended by "s3:".
Principle specifies which user(s) are granted or denied access to the bucket resource. Its value can be a string or an object containing an array of users. Valid users can be identified with an account ARN, account id, or user ARN.

There are also two optional bucket policy statement keys: Sid and Condition.

Sid stands for "statement id". If this key is not included, one will be generated for the statement.
Condition lists the condition under which a statement will take affect.
The possibilities are as follows:
    'ArnEquals'
    'ArnEqualsIfExists'
    'ArnLike'
    'ArnLikeIfExists'
    'ArnNotEquals'
    'ArnNotEqualsIfExists'
    'ArnNotLike'
    'ArnNotLikeIfExists'
    'BinaryEquals'
    'BinaryEqualsIfExists'
    'BinaryNotEquals'
    'BinaryNotEqualsIfExists'
    'Bool'
    'BoolIfExists'
    'DateEquals'
    'DateEqualsIfExists'
    'DateGreaterThan'
    'DateGreaterThanEquals'
    'DateGreaterThanEqualsIfExists'
    'DateGreaterThanIfExists'
    'DateLessThan'
    'DateLessThanEquals'
    'DateLessThanEqualsIfExists'
    'DateLessThanIfExists'
    'DateNotEquals'
    'DateNotEqualsIfExists'
    'IpAddress'
    'IpAddressIfExists'
    'NotIpAddress'
    'NotIpAddressIfExists'
    'Null'
    'NumericEquals'
    'NumericEqualsIfExists'
    'NumericGreaterThan'
    'NumericGreaterThanEquals'
    'NumericGreaterThanEqualsIfExists'
    'NumericGreaterThanIfExists'
    'NumericLessThan'
    'NumericLessThanEquals'
    'NumericLessThanEqualsIfExists'
    'NumericLessThanIfExists'
    'NumericNotEquals'
    'NumericNotEqualsIfExists'
    'StringEquals'
    'StringEqualsIfExists'
    'StringEqualsIgnoreCase'
    'StringEqualsIgnoreCaseIfExists'
    'StringLike'
    'StringLikeIfExists'
    'StringNotEquals'
    'StringNotEqualsIfExists'
    'StringNotEqualsIgnoreCase'
    'StringNotEqualsIgnoreCaseIfExists'
    'StringNotLike'
    'StringNotLikeIfExists'

