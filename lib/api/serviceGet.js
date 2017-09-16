const { errors } = require('arsenal');

const constants = require('../../constants');
const services = require('../services');
const { generateCorsResHeaders } = require('./apiUtils/object/corsResponse');

/*
 *  Format of xml response:
 *
 *  <?xml version="1.0" encoding="UTF-8"?>
 *  <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
 *  <Owner>
 *  <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
 *  <DisplayName>webfile</DisplayName>
 *  </Owner>
 *  <Buckets>
 *  <Bucket>
 *  <Name>quotes</Name>
 *  <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
 *  </Bucket>
 *  <Bucket>
 *  <Name>samples</Name>
 *  <CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
 *  </Bucket>
 *  </Buckets>
 *  </ListAllMyBucketsResult>
 */

function generateXml(xml, owner, userBuckets, splitter) {
    const splitterLen = splitter.length;
    userBuckets.forEach(bucket => {
        const index = bucket.key.indexOf(splitter);
        const key = bucket.key.substring(index + splitterLen);
        xml.push(
            '<Bucket>',
            `<Name>${key}</Name>`,
            `<CreationDate>${bucket.value.creationDate}` +
                '</CreationDate>',
            '</Bucket>'
        );
    });
    xml.push('</Buckets></ListAllMyBucketsResult>');
    return xml.join('');
}

function _collectCorsHeaders(origin, httpMethod) {
    if (!origin) {
        return {};
    }
    // we permit requests from all origins for a get service call, since the
    // call must be authenticated and it's not possible to use putBucketCORS to
    // permit Get Service requests
    const corsRule = {
        allowedMethods: ['GET'],
        allowedOrigins: ['*'],
    };
    return generateCorsResHeaders(corsRule, origin, httpMethod, null);
}

/**
 * GET Service - Get list of buckets owned by user
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback
 * @return {undefined}
 */
function serviceGet(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'serviceGet' });
    const corsHeaders = _collectCorsHeaders(request.headers.origin,
        request.method);

    if (authInfo.isRequesterPublicUser()) {
        log.debug('operation not available for public user');
        return callback(errors.AccessDenied, null, corsHeaders);
    }
    const xml = [];
    const canonicalId = authInfo.getCanonicalID();
    xml.push(
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/' +
            '2006-03-01/">',
        '<Owner>',
        `<ID>${canonicalId}</ID>`,
        `<DisplayName>${authInfo.getAccountDisplayName()}` +
            '</DisplayName>',
        '</Owner>',
        '<Buckets>'
    );
    return services.getService(authInfo, request, log, constants.splitter,
        (err, userBuckets, splitter) => {
            if (err) {
                return callback(err, null, corsHeaders);
            }
            // TODO push metric for serviceGet
            // pushMetric('getService', log, {
            //      bucket: bucketName,
            // });
            const xmlStr = generateXml(xml, canonicalId, userBuckets, splitter);
            return callback(null, xmlStr, corsHeaders);
        });
}

module.exports = serviceGet;
