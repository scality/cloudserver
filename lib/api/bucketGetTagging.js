const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const util = require('node:util');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/metrics');
const { errors, s3middleware } = require('arsenal');
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

    xml.push('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Tagging> <TagSet>');

    tags.forEach(tag => {
        xml.push('<Tag>');
        xml.push(`<Key>${escapeForXml(tag.Key)}</Key>`);
        xml.push(`<Value>${escapeForXml(tag.Value)}</Value>`);
        xml.push('</Tag>');
    });

    xml.push('</TagSet> </Tagging>');

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
async function bucketGetTagging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetTagging' });

    const { bucketName, headers } = request;
    const metadataValidateBucketPromise = util.promisify(standardMetadataValidateBucket);
    const checkExpectedBucketOwnerPromise = util.promisify(checkExpectedBucketOwner);

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketGetTagging',
        request,
    };

    let bucket;
    let xml = null;

    try {
        bucket = await metadataValidateBucketPromise(metadataValParams, request.actionImplicitDenies, log);
        // eslint-disable-next-line no-unused-expressions
        await checkExpectedBucketOwnerPromise(headers, bucket, log);
        const tags = bucket.getTags();
        if (!tags || !tags.length) {
            log.debug('bucket TagSet does not exist', {
                method: 'bucketGetTagging',
            });
            throw errors.NoSuchTagSet;
        }
        xml = tagsToXml(tags);
        pushMetric('getBucketTagging', log, {
            authInfo,
            bucket: bucketName,
        });
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        monitoring.promMetrics(
            'GET', bucketName, '200', 'getBucketTagging');
        return callback(null, xml, corsHeaders);
    } catch (err) {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        log.debug('error processing request',
            { method: 'bucketGetTagging', error: err });
        monitoring.promMetrics('GET', bucketName, err.code,
            'getBucketTagging');
        return callback(err, corsHeaders);
    }
}

module.exports = bucketGetTagging;
