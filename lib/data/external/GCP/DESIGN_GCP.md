## Google Cloud Storage Backend

### Overall Design

The Google Cloud Storage backend is implemented using the `aws-sdk` service
class for AWS compatible methods. The structure of these methods are
described in the `gcp-2017-11-01.api.json` file: request inputs, response
outputs, and required parameters. For non-compatible methods, helper methods are
implemented to perform the requests; these can be found under the `GcpApis`
directory.

The implement GCP Service is designed to work as close as possible to the AWS
service.

### Object Tagging

Google Cloud Storage does not have object-level tagging methods.

To be compatible with S3, object tags will be stored as metadata on
Google Cloud Storage.

### Multipart Upload

Google Cloud Storage does not have AWS S3 multipart upload methods, but there
are methods for merging multiple objects into a single composite object.
Utilizing these available methods, GCP is able to perform parallel uploads for
large uploads; however, due to limits set by Google Cloud Storage, the maximum
number of parts possible for a single upload is 1024 (AWS limit is 10000).

As Google Cloud Storage does not have methods for managing mutlipart uploads,
each part is uploaded as a single object in a Google Cloud Bucket.
Because of this, a secondary bucket for handling MPU parts is required for
a GCP multipart upload. The MPU bucket will serve to hide uploaded parts from
being listed as items of the main bucket as well as handling parts of multiple
in-progress mutlipart uploads.

<!-- 
<p style='font-size: 12'>
** The Google Cloud Storage method used for combining multipart objects into a
single object is the `compose` methods.<br/>
** <a>https://cloud.google.com/storage/docs/xml-api/put-object-compose</a>
</p>
 -->

#### Multipart Upload Methods Design:

+ **inititateMultipartUpload**:  
In `initiateMultipartUpload`, new multipart uploads will generate a prefix with
the scheme of `${objectKeyName}-${uploadI}` and each object related to an MPU
will be prefixed with it. This method will also create an `init` file that will
store the metadata related to an MPU for later assignment to the completed
object.

+ **uploadPart**:  
`uploadPart` will prefix the upload with the MPU prefix then perform a
`putObject` request to Google Cloud Storage

+ **uploadPartCopy**:  
`uploadPartCopy` will prefix the copy upload with the MPU prefix then perform a
`copyObject` request to Google Cloud Storage

+ **abortMultipartUpload**:  
`abortMultipartUpload` will perform the action of removing all objects related
to a multipart upload from the MPU bucket. It does this by first making a
`listObjectVersions` request to GCP to list all parts with the
related MPU-prefix then performing a `deleteObject` request on each of the
objects received.

+ **completeMultipartUpload**:  
`completeMultipartUpload` will perform the action of combining the given parts
to be create the single composite object. This method consists of multiple
steps, due to the limitations of the Google Cloud Storage `compose` method:
    + compose round 1: multiple compose calls to merge, at max, 32 objects into
    a single subpart.
    + compose round 2: multiple compose calls to merge the subpart generated
    in compose round 1 to create the final completed object
    + generate MPU ETag: generate the multipart etag that will be returned as
    part of the completeMultipartUpload response
    + copy to main: retrieve the metadata stored in the `init` file created in
    `initiateMultipartUpload` to be assigned the completed object and copy the
    the completed object from the MPU bucket to the Main bucket
    + cleanUp: remove all objects related to a MPU
### Limitations

+ GCP multipart uploads are limited to 1024 parts
+ Each `compose` can merge up to 32 objects per request
+ As Google Cloud Storage doesn't have AWS style MPU methods, GCP MPU will
require a secondary bucket to perform multipart uploads
+ GCP doesn't not have object-level tagging methods; AWS style tags are stored
as metadata on Google Cloud Storage

More information can be found at:
+ https://cloud.google.com/storage/docs/xml-api/overview;
+ https://cloud.google.com/storage/quotas;
+ https://cloud.google.com/storage/docs/xml-api/put-object-compose;
