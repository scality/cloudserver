# S3 connector

S3 handles the requests coming from S3 clients. It is based on the
current amazon-S3 documentation.

## Implementation

### Architecture

S3 consists of multiple daemons listening RESTfully to http requests
on a single port. We then route the requests depending on the HTTP verbs
used by the request. Parsing the header allows us to determine the request
type. From there, authentication will be confirmed
by the Vault module using v2 or v4 authentication depending on whether the
client sent a v2 or v4 authentication header. Applicable bucket and object
metadata will be pulled from the metadata backend, IM-Metadata to check proper
authorization (ACL's, IAM and bucket policies). If a user has been
authenticated and is authorized to write/read data, we then proceed to
send the write/read request to our storage backend (RING or IM-Data), before
sending a response to the request sender. Any problem that arises during
the handling of the request due to client error or system error will result
in an error being returned to the client that follows S3's error specifications.

The multi-daemon architecture allows us to restart daemons on the fly in case
of any crash without interrupting the service. The daemon handles
remaining requests even in case of an error, stopping listening while another
daemon is spawned to handle future requests in its stead.

![Arch](res/architecture.png)

### API specifications

Right now, the following operations are implemented:

- PutBucket
- GetBucket
- HeadBucket
- DeleteBucket
- PutBucketACL
- GetBucketACL
- PutObject
- PutObject - Copy
- GetObject
- HeadObject
- DeleteObject
- PutObjectACL
- GetObjectACL
- Multipart Upload
- Upload Part
- Upload Part - Copy
- GetService
- v2 Authentication
- v4 Authentication (Transferring Payload in a Single Chunk)
- v4 Authentication (Transferring Payload in Multiple Chunks)
