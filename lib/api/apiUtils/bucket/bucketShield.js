const invisiblyDelete = require('./invisiblyDelete');
const constants = require('../../../../constants');

/**
 * Checks whether to proceed with a request based on the bucket flags
 * and sends a request to invisibly delete the bucket if applicable
 * @param {object} bucket - bucket metadata
 * @param {string} requestType - type of api request
 * @return {boolean} true if the bucket should be shielded, false otherwise
 */
function bucketShield(bucket, requestType) {
    const invisiblyDeleteRequests = constants.bucketOwnerActions.concat(
        [
            'bucketGet',
            'bucketHead',
            'bucketGetACL',
            'objectGet',
            'objectGetACL',
            'objectHead',
            'objectPutACL',
            'objectDelete',
        ]);
    if (invisiblyDeleteRequests.indexOf(requestType) > -1 &&
        bucket.hasDeletedFlag()) {
        invisiblyDelete(bucket.getName(), bucket.getOwner());
        return true;
    }
     // If request is initiateMultipartUpload (requestType objectPut),
     // objectPut, bucketPutACL or bucketDelete, proceed with request.
     // Otherwise return an error to the client
    if ((bucket.hasDeletedFlag() || bucket.hasTransientFlag()) &&
        (requestType !== 'objectPut' &&
        requestType !== 'initiateMultipartUpload' &&
        requestType !== 'objectPutPart' &&
        requestType !== 'completeMultipartUpload' &&
        requestType !== 'bucketPutACL' &&
        requestType !== 'bucketDelete')) {
        return true;
    }
    return false;
}

module.exports = bucketShield;
