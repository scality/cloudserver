const { waterfall } = require('async');
const { s3middleware } = require('arsenal');


const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const monitoring = require('../utilities/monitoringHandler');
const { parseTagXml } = s3middleware.tagging;

/**
 * Format of xml request:

 <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
 <TagSet>
 <Tag>
 <Key>string</Key>
 <Value>string</Value>
 </Tag>
 </TagSet>
 </Tagging>
 */

/**
 * Bucket Put Tagging - Create or update bucket Tagging
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutTagging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutTagging' });

    const { bucketName, headers } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketPutTagging',
        request,
    };
    let bucket = null;
    return waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log,
            (err, b) => {
                bucket = b;
                return next(err);
            }),
        next => checkExpectedBucketOwner(headers, bucket, log, next),
        next => parseTagXml(request.post, log, next),
        (tags, next) => {
            const tagArray = [];
            Object.keys(tags).forEach(key => {
                tagArray.push({ Value: tags[key], Key: key });
            });
            bucket.setTags(tagArray);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err));
        },
    ], err => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketPutTagging'
            });
            monitoring.promMetrics('PUT', bucketName, err.code,
                'putBucketTagging');
        } else {
            monitoring.promMetrics(
                'PUT', bucketName, '200', 'putBucketTagging');
            pushMetric('putBucketTagging', log, {
                authInfo,
                bucket: bucketName,
            });
        }
        return callback(err, corsHeaders);
    });
}

module.exports = bucketPutTagging;
