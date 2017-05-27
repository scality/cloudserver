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
    // testing use 100 MB as max
    maximumAllowedPartSize: process.env.MPU_TESTING === 'yes' ? 104857600 :
        5368709120,

    // hex digest of sha256 hash of empty string:
    emptyStringHash: crypto.createHash('sha256')
        .update('', 'binary').digest('hex'),

    // Queries supported by AWS that we do not currently support.
    unsupportedQueries: {
        'accelerate': true,
        'analytics': true,
        'inventory': true,
        'lifecycle': true,
        'list-type': true,
        'logging': true,
        'metrics': true,
        'notification': true,
        'policy': true,
        'requestPayment': true,
        'restore': true,
        'torrent': true,
    },
    // user metadata header to set object locationConstraint
    objectLocationConstraintHeader: 'x-amz-meta-scal-location-constraint',
};

module.exports = constants;
