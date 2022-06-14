const { waterfall } = require('async');
const { Parser } = require('xml2js');
const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');

const tagKeyNotUnique = 'Cannot provide multiple Tags with the same key';
const tagKeyInvalid = 'The TagKey you have provided is invalid';
const tagValueInvalid = 'The TagValue you have provided is invalid';
const tooManyTags = 'Bucket tag count cannot be greater than 50';

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

function _parseXML(request, log, cb, parser) {
    if (request.post === '') {
        log.debug('request xml is missing');
        return cb(errors.MalformedXML);
    }
    return parser.parseString(request.post, (err, result) => {
        if (err) {
            log.debug('request xml is malformed');
            return cb(errors.MalformedXML);
        }
        return process.nextTick(() => cb(null, result));
    });
}

function checkTags(tagSet, bucket, log, cb) {
    if (tagSet === undefined) {
        return cb(null, []);
    }
    const tags = Array.isArray(tagSet) ? tagSet : [tagSet];
    let errMsg = null;
    const uniqueKeys = [];
    for (let i = 0; i < tags.length && !errMsg; i++) {
        if (uniqueKeys.find(key => tags[i].Key === key) !== undefined) { // Checking that the key is unique
            errMsg = tagKeyNotUnique;
        } else {
            uniqueKeys.push(tags[i].Key); // If unique push it to the array of unique keys
        }
        if (tags[i].Key.length > 128) { // Checking that the key length is less than 128 characters
            errMsg = tagKeyInvalid;
        } else if (tags[i].Value.length > 256) { // Checking that the value lenght is less than 256 characters
            errMsg = tagValueInvalid;
        }
        if (i >= 50) { // If there is more than 50 tags in the tagset, raise a bad request saying too many tags
            log.debug(tooManyTags,
                {
                    method: 'bucketPutTagging',
                    error: errors.BadRequest,
                });
            const error = errors.BadRequest.customizeDescription(
                tooManyTags);
            return cb(error);
        }
    }
    if (errMsg) {
        log.debug(errMsg,
            {
                method: 'bucketPutTagging',
                error: errors.InvalidTag,
            });
        const error = errors.InvalidTag.customizeDescription(
            errMsg);
        return cb(error);
    }
    return cb(null, tags);
}

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

    const parser = new Parser({ explicitArray: false });
    const { bucketName, headers } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutTagging',
    };
    let bucket = null;
    return waterfall([
        next => metadataValidateBucket(metadataValParams, log,
            (err, b) => {
                bucket = b;
                return next(err);
            }),
        next => checkExpectedBucketOwner(headers, bucket, log, next),
        next => _parseXML(request, log, next, parser),
        (result, next) => checkTags(result.Tagging.TagSet.Tag, bucket, log, next),
        (tags, next) => {
            bucket.setTags(tags);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err));
        },
    ], (err) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        pushMetric('putBucketTagging', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(err, corsHeaders);
    });
}

module.exports = bucketPutTagging;
