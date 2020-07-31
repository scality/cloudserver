const crypto = require('crypto');

const constants = {
    /*
     * Splitter is used to build the object name for the overview of a
     * multipart upload and to build the object names for each part of a
     * multipart upload.  These objects with large names are then stored in
     * metadata in a "shadow bucket" to a real bucket.  The shadow bucket
     * contains all ongoing multipart uploads.  We include in the object
     * name some of the info we might need to pull about an open multipart
     * upload or about an individual part with each piece of info separated
     * by the splitter.  We can then extract each piece of info by splitting
     * the object name string with this splitter.
     * For instance, assuming a splitter of '...!*!',
     * the name of the upload overview would be:
     *   overview...!*!objectKey...!*!uploadId
     * For instance, the name of a part would be:
     *   uploadId...!*!partNumber
     *
     * The sequence of characters used in the splitter should not occur
     * elsewhere in the pieces of info to avoid splitting where not
     * intended.
     *
     * Splitter is also used in adding bucketnames to the
     * namespacerusersbucket.  The object names added to the
     * namespaceusersbucket are of the form:
     * canonicalID...!*!bucketname
     */

    splitter: '..|..',
    // BACKWARD: This line will be removed when removing backward compatibility
    oldSplitter: 'splitterfornow',
    usersBucket: 'users..bucket',
    oldUsersBucket: 'namespaceusersbucket',
    // MPU Bucket Prefix is used to create the name of the shadow
    // bucket used for multipart uploads.  There is one shadow mpu
    // bucket per bucket and its name is the mpuBucketPrefix followed
    // by the name of the final destination bucket for the object
    // once the multipart upload is complete.
    mpuBucketPrefix: 'mpuShadowBucket',
    blacklistedPrefixes: { bucket: [], object: [] },
    // PublicId is used as the canonicalID for a request that contains
    // no authentication information.  Requestor can access
    // only public resources
    publicId: 'http://acs.amazonaws.com/groups/global/AllUsers',
    // All Authenticated Users is an ACL group.
    allAuthedUsersId: 'http://acs.amazonaws.com/groups/' +
        'global/AuthenticatedUsers',
    // LogId is used for the AWS logger to write the logs
    // to the destination bucket.  This style of logging is
    // to be implemented later but the logId is used in the
    // ACLs.
    logId: 'http://acs.amazonaws.com/groups/s3/LogDelivery',
    emptyFileMd5: 'd41d8cd98f00b204e9800998ecf8427e',

    // Number of sub-directories for file backend
    folderHash: 3511, // Prime number
    // AWS only returns 1000 on a listing
    // http://docs.aws.amazon.com/AmazonS3/latest/API/
    //      RESTBucketGET.html#RESTBucketGET-requests
    listingHardLimit: 1000,

    // AWS sets a minimum size limit for parts except for the last part.
    // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
    minimumAllowedPartSize: 5242880,

    // Max size on put part or copy part is 5GB. For functional
    // testing use 110 MB as max
    maximumAllowedPartSize: process.env.MPU_TESTING === 'yes' ? 110100480 :
        5368709120,

    // AWS sets a maximum total parts limit
    // https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
    maximumAllowedPartCount: 10000,

    // AWS states max size for user-defined metadata (x-amz-meta- headers) is
    // 2 KB: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
    // In testing, AWS seems to allow up to 88 more bytes, so we do the same.
    maximumMetaHeadersSize: 2136,

    // hex digest of sha256 hash of empty string:
    emptyStringHash: crypto.createHash('sha256')
        .update('', 'binary').digest('hex'),

    // Queries supported by AWS that we do not currently support.
    // Non-bucket queries
    unsupportedQueries: [
        'accelerate',
        'analytics',
        'encryption',
        'inventory',
        'logging',
        'metrics',
        'policyStatus',
        'publicAccessBlock',
        'requestPayment',
        'restore',
        'torrent',
    ],

    // Headers supported by AWS that we do not currently support.
    unsupportedHeaders: [
        'x-amz-server-side-encryption-customer-algorithm',
        'x-amz-server-side-encryption-aws-kms-key-id',
        'x-amz-server-side-encryption-context',
        'x-amz-server-side-encryption-customer-key',
        'x-amz-server-side-encryption-customer-key-md5',
    ],

    // user metadata header to set object locationConstraint
    objectLocationConstraintHeader: 'x-amz-meta-scal-location-constraint',
    legacyLocations: ['sproxyd', 'legacy'],
    /* eslint-disable camelcase */
    externalBackends: { aws_s3: true, azure: true, gcp: true },
    // some of the available data backends  (if called directly rather
    // than through the multiple backend gateway) need a key provided
    // as a string as first parameter of the get/delete methods.
    clientsRequireStringKey: { sproxyd: true, cdmi: true },
    // healthcheck default call from nginx is every 2 seconds
    // for external backends, don't call unless at least 1 minute
    // (60,000 milliseconds) since last call
    externalBackendHealthCheckInterval: 60000,
    versioningNotImplBackends: { azure: true },
    mpuMDStoredExternallyBackend: { aws_s3: true },
    /* eslint-enable camelcase */
    mpuMDStoredOnS3Backend: { azure: true },
    azureAccountNameRegex: /^[a-z0-9]{3,24}$/,
    base64Regex: new RegExp('^(?:[A-Za-z0-9+/]{4})*' +
        '(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$'),
    // user metadata applied on zenko objects
    zenkoIDHeader: 'x-amz-meta-zenko-instance-id',
    bucketOwnerActions: [
        'bucketDeleteCors',
        'bucketDeleteLifecycle',
        'bucketDeletePolicy',
        'bucketDeleteReplication',
        'bucketDeleteWebsite',
        'bucketGetCors',
        'bucketGetLifecycle',
        'bucketGetLocation',
        'bucketGetPolicy',
        'bucketGetReplication',
        'bucketGetVersioning',
        'bucketGetWebsite',
        'bucketPutCors',
        'bucketPutLifecycle',
        'bucketPutPolicy',
        'bucketPutReplication',
        'bucketPutVersioning',
        'bucketPutWebsite',
        'objectDeleteTagging',
        'objectGetTagging',
        'objectPutTagging',
    ],
    // response header to be sent when there are invalid
    // user metadata in the object's metadata
    invalidObjectUserMetadataHeader: 'x-amz-missing-meta',
    // Bucket specific queries supported by AWS that we do not currently support
    // these queries may or may not be supported at object level
    unsupportedBucketQueries: [
        'tagging',
    ],
};

module.exports = constants;
