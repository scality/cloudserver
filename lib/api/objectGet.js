const { errors, s3middleware } = require('arsenal');
const { parseRange } = require('arsenal').network.http.utils;

const { data } = require('../data/wrapper');

const { decodeVersionId } = require('./apiUtils/object/versioning');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const collectResponseHeaders = require('../utilities/collectResponseHeaders');
const { pushMetric } = require('../utapi/utilities');
const { getVersionIdResHeader } = require('./apiUtils/object/versioning');
const setPartRanges = require('./apiUtils/object/setPartRanges');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { getPartCountFromMd5 } = require('./apiUtils/object/partInfo');
const { setExpirationHeaders } = require('./apiUtils/object/expirationHeaders');

const validateHeaders = s3middleware.validateConditionalHeaders;

/**
 * GET Object - Get an object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param {boolean} returnTagCount - returns the x-amz-tagging-count header
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
function objectGet(authInfo, request, returnTagCount, log, callback) {
    log.debug('processing request', { method: 'objectGet' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return callback(decodedVidResult);
    }
    const versionId = decodedVidResult;

    const mdValParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId,
        requestType: 'objectGet',
        request,
    };

    return metadataValidateBucketAndObj(mdValParams, request.actionImplicitDenies, log,
    (err, bucket, objMD) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'metadataValidateBucketAndObj',
            });
            return callback(err, null, corsHeaders);
        }
        if (!objMD) {
            const err = versionId ? errors.NoSuchVersion : errors.NoSuchKey;
            return callback(err, null, corsHeaders);
        }
        const verCfg = bucket.getVersioningConfiguration();
        if (objMD.isDeleteMarker) {
            const responseMetaHeaders = Object.assign({},
                { 'x-amz-delete-marker': true }, corsHeaders);
            if (!versionId) {
                return callback(errors.NoSuchKey, null, responseMetaHeaders);
            }
            // return MethodNotAllowed if requesting a specific
            // version that has a delete marker
            responseMetaHeaders['x-amz-version-id'] =
                getVersionIdResHeader(verCfg, objMD);
            return callback(errors.MethodNotAllowed, null,
                responseMetaHeaders);
        }
        const headerValResult = validateHeaders(request.headers,
            objMD['last-modified'], objMD['content-md5']);
        if (headerValResult.error) {
            return callback(headerValResult.error, null, corsHeaders);
        }
        const responseMetaHeaders = collectResponseHeaders(objMD,
            corsHeaders, verCfg, returnTagCount);

        setExpirationHeaders(responseMetaHeaders, {
            lifecycleConfig: bucket.getLifecycleConfiguration(),
            objectParams: {
                key: objectKey,
                tags: objMD.tags,
                date: objMD['last-modified'],
            },
            isVersionedReq: !!versionId,
        });

        const objLength = (objMD.location === null ?
                           0 : parseInt(objMD['content-length'], 10));
        let byteRange;
        const streamingParams = {};
        if (request.headers.range) {
            const { range, error } = parseRange(request.headers.range,
                                                objLength);
            if (error) {
                return callback(error, null, corsHeaders);
            }
            responseMetaHeaders['Accept-Ranges'] = 'bytes';
            if (range) {
                byteRange = range;
                // End of range should be included so + 1
                responseMetaHeaders['Content-Length'] =
                    range[1] - range[0] + 1;
                responseMetaHeaders['Content-Range'] =
                    `bytes ${range[0]}-${range[1]}/${objLength}`;
                streamingParams.rangeStart = range[0] ?
                range[0].toString() : undefined;
                streamingParams.rangeEnd = range[1] ?
                range[1].toString() : undefined;
            }
        }
        let dataLocator = null;
        if (objMD.location !== null) {
            // To provide for backwards compatibility before
            // md-model-version 2, need to handle cases where
            // objMD.location is just a string
            dataLocator = Array.isArray(objMD.location) ?
                objMD.location : [{ key: objMD.location }];
            // if the data backend is azure, there will only ever be at
            // most one item in the dataLocator array
            if (dataLocator[0] && dataLocator[0].dataStoreType === 'azure') {
                dataLocator[0].azureStreamingOptions = streamingParams;
            }

            let partNumber = null;
            if (request.query && request.query.partNumber !== undefined) {
                if (byteRange) {
                    const error = errors.InvalidRequest
                        .customizeDescription('Cannot specify both Range ' +
                            'header and partNumber query parameter.');
                    return callback(error, null, corsHeaders);
                }
                partNumber = Number.parseInt(request.query.partNumber, 10);
                if (Number.isNaN(partNumber)) {
                    const error = errors.InvalidArgument
                        .customizeDescription('Part number must be a number.');
                    return callback(error, null, corsHeaders);
                }
                if (partNumber < 1 || partNumber > 10000) {
                    const error = errors.InvalidArgument
                        .customizeDescription('Part number must be an ' +
                            'integer between 1 and 10000, inclusive.');
                    return callback(error, null, corsHeaders);
                }
            }
            // If have a data model before version 2, cannot support
            // get range for objects with multiple parts
            if (byteRange && dataLocator.length > 1 &&
                dataLocator[0].start === undefined) {
                return callback(errors.NotImplemented, null, corsHeaders);
            }
            if (objMD['x-amz-server-side-encryption']) {
                for (let i = 0; i < dataLocator.length; i++) {
                    dataLocator[i].masterKeyId =
                        objMD['x-amz-server-side-encryption-aws-kms-key-id'];
                    dataLocator[i].algorithm =
                        objMD['x-amz-server-side-encryption'];
                }
            }
            if (partNumber) {
                const locations = [];
                let locationPartNumber;
                for (let i = 0; i < objMD.location.length; i++) {
                    const { dataStoreETag } = objMD.location[i];

                    if (dataStoreETag) {
                        locationPartNumber =
                            Number.parseInt(dataStoreETag.split(':')[0], 10);
                    } else {
                        /**
                         * Location objects prior to GA7.1 do not include the
                         * dataStoreETag field so we cannot find the part range,
                         * the objects are treated as if they only have 1 part
                         */
                        locationPartNumber = 1;
                    }

                    // Get all parts that belong to the requested part number
                    if (partNumber === locationPartNumber) {
                        locations.push(objMD.location[i]);
                    } else if (locationPartNumber > partNumber) {
                        break;
                    }
                }
                if (locations.length === 0) {
                    return callback(errors.InvalidPartNumber, null,
                        corsHeaders);
                }
                const { start } = locations[0];
                const endLocation = locations[locations.length - 1];
                const end = endLocation.start + endLocation.size - 1;
                responseMetaHeaders['Content-Length'] = end - start + 1;
                const partByteRange = [start, end];
                dataLocator = setPartRanges(dataLocator, partByteRange);
                const partsCount = getPartCountFromMd5(objMD);
                if (partsCount) {
                    responseMetaHeaders['x-amz-mp-parts-count'] =
                        partsCount;
                }
            } else {
                dataLocator = setPartRanges(dataLocator, byteRange);
            }
        }
        return data.head(dataLocator, log, err => {
            if (err) {
                log.error('error from external backend checking for ' +
                'object existence', { error: err });
                return callback(err);
            }
            pushMetric('getObject', log, {
                authInfo,
                bucket: bucketName,
                keys: [objectKey],
                newByteLength:
                    Number.parseInt(responseMetaHeaders['Content-Length'], 10),
                versionId: objMD.versionId,
                location: objMD.dataStoreName,
            });
            return callback(null, dataLocator, responseMetaHeaders,
                byteRange);
        });
    });
}

module.exports = objectGet;
