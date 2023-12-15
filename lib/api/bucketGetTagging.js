const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const { errors, s3middleware } = require('arsenal');
const { waterfall } = require('async');
const escapeForXml = s3middleware.escapeForXml;

//  Sample XML response:
/*
    <Tagging>
        <TagSet>
            <Tag>
                <Key>string</Key>
                <Value>string</Value>
            </Tag>
            <Tag>
                <Key>string</Key>
                <Value>string</Value>
            </Tag>
        </TagSet>
    </Tagging>
*/

/**
 * @typedef Tag
 * @type {object}
 * @property {string} Value - Value of the tag.
 * @property {string} Key - Key of the tag.
 */
/**
 * Convert Versioning Configuration object of a bucket into xml format.
 * @param {array.<Tag>} tags - set of bucket tag
 * @return {string} - the converted xml string of the versioning configuration
 */
function tagsToXml(tags) {
    const xml = [];

    xml.push('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Tagging><TagSet>');

    tags.forEach(tag => {
        xml.push('<Tag>');
        xml.push(`<Key>${escapeForXml(tag.Key)}</Key>`);
        xml.push(`<Value>${escapeForXml(tag.Value)}</Value>`);
        xml.push('</Tag>');
    });

    xml.push('</TagSet></Tagging>');

    return xml.join('');
}

/**
 * bucketGetVersioning - Return Versioning Configuration for bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
function bucketGetTagging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetTagging' });

    const { bucketName, headers } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketGetTagging',
        request,
    };
    let bucket = null;
    let xml = null;
    let tags = null;

    return waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log,
            (err, b) => {
                bucket = b;
                return next(err);
            }),
        next => checkExpectedBucketOwner(headers, bucket, log, next),
        next => {
            tags = bucket.getTags();
            if (!tags || !tags.length) {
                log.debug('bucket TagSet does not exist', {
                    method: 'bucketGetTagging',
                });
                return next(errors.NoSuchTagSet);
            }
            xml = tagsToXml(tags);
            return next();
        }
    ], err => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetTagging'
            });
            monitoring.promMetrics('GET', bucketName, err.code,
                'getBucketTagging');
        } else {
            pushMetric('getBucketTagging', log, {
                authInfo,
                bucket: bucketName,
            });
            monitoring.promMetrics(
                'GET', bucketName, '200', 'getBucketTagging');
        }
        return callback(err, xml, corsHeaders);
    });
}

module.exports = bucketGetTagging;
