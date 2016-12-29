import { errors } from 'arsenal';
import { parseString } from 'xml2js';

import api from '../api/api';
import routesUtils from './routesUtils';
import utils from '../utils';
import pushMetrics from '../utilities/pushMetrics';
import statsReport500 from '../utilities/statsReport500';

const encryptionHeaders = [
    'x-amz-server-side-encryption',
    'x-amz-server-side-encryption-customer-algorithm',
    'x-amz-server-side-encryption-aws-kms-key-id',
    'x-amz-server-side-encryption-context',
    'x-amz-server-side-encryption-customer-key',
    'x-amz-server-side-encryption-customer-key-md5',
];

const validStatuses = ['Enabled', 'Suspended'];
const validMfaDeletes = [undefined, 'Enabled', 'Disabled'];

/* eslint-disable no-param-reassign */
export default function routePUT(request, response, log, utapi, statsClient) {
    log.debug('routing request', { method: 'routePUT' });

    if (request.objectKey === undefined) {
        // PUT bucket - PUT bucket ACL

        // content-length for object is handled separately below
        const contentLength = request.headers['content-length'];
        if (contentLength && (isNaN(contentLength) || contentLength === '' ||
        contentLength < 0)) {
            log.debug('invalid content-length header');
            return routesUtils.responseNoBody(
              errors.BadRequest, null, response, null, log);
        }
        request.post = '';
        request.on('data', chunk => {
            request.post += chunk.toString();
        });

        request.on('end', () => {
            // PUT bucket ACL
            if (request.query.acl !== undefined) {
                api.callApiMethod('bucketPutACL', request, log, err => {
                    statsReport500(err, statsClient);
                    pushMetrics(err, log, utapi, 'bucketPutACL',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, null, response, 200,
                        log);
                });
            } else if (request.query.versioning !== undefined) {
                if (request.post === '') {
                    log.debug('request xml is missing');
                    return routesUtils.responseNoBody(
                        errors.MalformedXML, null, response, null, log);
                }
                const xmlToParse = request.post;
                return parseString(xmlToParse, (err, result) => {
                    if (err) {
                        log.debug('request xml is malformed');
                        return routesUtils.responseNoBody(
                            errors.MalformedXML, null, response, null, log);
                    }
                    const status = result.VersioningConfiguration.Status ?
                        result.VersioningConfiguration.Status[0] : undefined;
                    const mfaDelete = result.VersioningConfiguration.MfaDelete ?
                        result.VersioningConfiguration.MfaDelete[0] : undefined;
                    if (validStatuses.indexOf(status) < 0 ||
                        validMfaDeletes.indexOf(mfaDelete) < 0) {
                        log.debug('illegal versioning configuration');
                        return routesUtils.responseNoBody(
                            errors.IllegalVersioningConfigurationException,
                            null, response, null, log);
                    }
                    if (mfaDelete) {
                        log.debug('mfa deletion is not implemented');
                        return routesUtils.responseNoBody(
                            errors.NotImplemented.customizedDescription(
                                'MFA Deletion is not supported yet.'), null,
                            response, null, log);
                    }
                    return api.callApiMethod('bucketPutVersioning', request,
                        log, err => {
                            // TODO push metrics for bucketPutVersioning
                            // pushMetrics(err, log, utapi,
                            //         'bucketPutVersioning',
                            //         request.bucketName);
                            statsReport500(err, statsClient);
                            routesUtils.responseNoBody(
                                err, null, response, 200, log);
                        });
                });
            } else if (request.query.acl === undefined) {
                // PUT bucket
                if (request.post) {
                    const xmlToParse = request.post;
                    return parseString(xmlToParse, (err, result) => {
                        if (err || !result.CreateBucketConfiguration
                            || !result.CreateBucketConfiguration
                                .LocationConstraint
                            || !result.CreateBucketConfiguration
                                .LocationConstraint[0]) {
                            log.debug('request xml is malformed');
                            return routesUtils.responseNoBody(errors
                                .MalformedXML,
                                null, response, null, log);
                        }
                        const locationConstraint =
                            result.CreateBucketConfiguration
                            .LocationConstraint[0];
                        log.trace('location constraint',
                            { locationConstraint });
                        return api.callApiMethod('bucketPut', request, log,
                        err => {
                            statsReport500(err, statsClient);
                            pushMetrics(err, log, utapi, 'bucketPut',
                                request.bucketName);
                            return routesUtils.responseNoBody(err, null,
                              response, 200, log);
                        }, locationConstraint);
                    });
                }
                return api.callApiMethod('bucketPut', request, log, err => {
                    statsReport500(err, statsClient);
                    pushMetrics(err, log, utapi, 'bucketPut',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, null, response, 200,
                        log);
                });
            }
            return undefined;
        });
    } else {
        // PUT object, PUT object ACL, PUT object multipart or
        // PUT object copy
        // if content-md5 is not present in the headers, try to
        // parse content-md5 from meta headers

        if (request.headers['content-md5'] === '') {
            log.debug('empty content-md5 header', {
                method: 'routePUT',
            });
            return routesUtils
            .responseNoBody(errors.InvalidDigest, null, response, 200, log);
        }
        if (request.headers['content-md5']) {
            request.contentMD5 = request.headers['content-md5'];
        } else {
            request.contentMD5 = utils.parseContentMD5(request.headers);
        }
        if (request.contentMD5 && request.contentMD5.length !== 32) {
            request.contentMD5 = Buffer.from(request.contentMD5, 'base64')
                .toString('hex');
            if (request.contentMD5 && request.contentMD5.length !== 32) {
                log.warn('invalid md5 digest', {
                    contentMD5: request.contentMD5,
                });
                return routesUtils
                    .responseNoBody(errors.InvalidDigest, null, response, 200,
                                    log);
            }
        }
        // object level encryption
        if (encryptionHeaders.some(i => request.headers[i] !== undefined)) {
            return routesUtils.responseXMLBody(errors.NotImplemented, null,
                response, log);
        }
        if (request.query.partNumber) {
            if (request.headers['x-amz-copy-source']) {
                api.callApiMethod('objectPutCopyPart', request, log,
                (err, xml, additionalHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseXMLBody(err, xml, response, log,
                            additionalHeaders);
                });
            } else {
                api.callApiMethod('objectPutPart', request, log,
                    (err, calculatedHash) => {
                        // ETag's hex should always be enclosed in quotes
                        const resMetaHeaders = { ETag: `"${calculatedHash}"` };
                        statsReport500(err, statsClient);
                        pushMetrics(err, log, utapi, 'objectPutPart',
                            request.bucketName, request.parsedContentLength);
                        routesUtils.responseNoBody(err, resMetaHeaders,
                            response, 200, log);
                    });
            }
        } else if (request.query.acl !== undefined) {
            request.post = '';
            request.on('data', chunk => {
                request.post += chunk.toString();
            });
            request.on('end', () => {
                api.callApiMethod('objectPutACL', request, log, err => {
                    statsReport500(err, statsClient);
                    pushMetrics(err, log, utapi, 'objectPutACL',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, null, response, 200,
                        log);
                });
            });
        } else if (request.headers['x-amz-copy-source']) {
            return api.callApiMethod('objectCopy', request, log, (err, xml,
                additionalHeaders, sourceObjSize, destObjPrevSize) => {
                statsReport500(err, statsClient);
                pushMetrics(err, log, utapi, 'objectCopy', request.bucketName,
                    sourceObjSize, destObjPrevSize);
                routesUtils.responseXMLBody(err, xml, response, log,
                    additionalHeaders);
            });
        } else {
            if (request.headers['content-length'] === undefined &&
            request.headers['x-amz-decoded-content-length'] === undefined) {
                return routesUtils.responseNoBody(errors.MissingContentLength,
                    null, response, 411, log);
            }
            if (Number.isNaN(request.parsedContentLength) ||
            request.parsedContentLength < 0) {
                return routesUtils.responseNoBody(errors.BadRequest,
                    null, response, 400, log);
            }
            log.end().addDefaultFields({
                contentLength: request.parsedContentLength,
            });

            api.callApiMethod('objectPut', request, log,
                (err, contentMD5, prevContentLen) => {
                    // ETag's hex should always be enclosed in quotes
                    statsReport500(err, statsClient);
                    const resMetaHeaders = {
                        ETag: `"${contentMD5}"`,
                    };
                    pushMetrics(err, log, utapi, 'objectPut',
                        request.bucketName, request.parsedContentLength,
                        prevContentLen);
                    return routesUtils.responseNoBody(err, resMetaHeaders,
                        response, 200, log);
                });
        }
    }
    return undefined;
}
/* eslint-enable no-param-reassign */
