# Get Bucket Version 2 Documentation

## Description

This feature implements version 2 of the GET Bucket (List Objects)
operation, following AWS specifications
(see https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html).

## Requirements

The user must have READ access to the bucket.

## Design

### Request

The `delimiter`, `encoding-type`, `max-keys`, and `prefix` request parameters
from GET Bucket v1 remain unchanged.
In order to specify v2, the parameter `list-type` must be included and
set to `2`.
The `marker` v1 parameter's functionality has been split in two and replaced by
`start-after` and `continuation-token` in v2. The `start-after` parameter is
a specific object key after which the API will return key names. It is only
valid in the first GET request. If both the `start-after` and
`continuation-token` parameters are included in a request, the API will
ignore the `start-after` parameter in favor of the `continuation-token`.
If the GET Bucket v2 response is truncated, a `NextContinuationToken` will
also be included. To list the next set of objects, the `NextContinuationToken`
can be used as the `continuation-token` in the next request. The continuation
token is an obfuscated string of 57 characters that CloudServer understands and
interprets.
By default, the v2 response does not include object owner information. To
include owner information like the default v1 response, use the `fetch-owner`
request parameter set to `true`.

### Response

The GET Bucket v1 and v2 responses are largely the same, with only a few changes.
The `NextMarker` v1 parameter has been replaced by the
`NextContinuationToken`. The `NextContinuationToken` is included with any
truncated response, even if no delimiter is sent in the request. Its value is an
obfuscated string that can be passed at the `continuation-token` in the next
request, which will be interpreted by CloudServer.
The `KeyCounter` parameter is returned in every response. Its value is the
number of keys included in the response. It is always less than or equal to
the `MaxKeys` value.
If the `start-after` or `continuation-token` parameter is used in the
request, it is also included in the response.
By default, the v2 response does not include object owner information, unlike
the v1 response. See the `Request` section for including it.

### Continuation Token

An example continuation token:

```
NextContinuationToken: '1bunC4s+crlZNAAbKUGBLyajJUQKp22TOdUR6/01snxD2cZtjJD0ugA=='
```

In order to generate a comparable token, CloudServer uses base64 encoding to
obfuscate the key name of the next object to be listed.
Encoded continuation tokens are similarly decoded in order for listing to
continue from the correct object.

## Performing Get Bucket V2 Operation

When performing the GET Bucket V2 operation, if the request is built manually,
the parameter `list-type` must be included and set to `2`.
Using the AWS cli client, the command becomes `list-objects-v2`.
