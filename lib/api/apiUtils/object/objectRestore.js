
const async = require('async');

const { errors } = require('arsenal');

const ObjectMD = require('arsenal').models.ObjectMD;
const coldStorage = require('./coldStorage');


const METHOD = 'objectRestore';


/**
 * POST Object restore process
 *
 * @param {MetadataWrapper} metadata metadata wrapper
 * @param {object} mdUtils utility object to treat metadata
 * @param {object} func object with a reference to each function of cloudserver
 * @param {function(object):string|Error} func.decodeVersionId 
 * @param {function(object, string, BucketInfo):object} func.collectCorsHeaders 
 * @param {function(object, object):string} func.getVersionIdResHeader
 * @param {AuthInfo} userInfo Instance of AuthInfo class with requester's info
 * @param {IncomingMessage} request request info
 * @param {werelogs.Logger} log Werelogs instance
 * @param {module:api/objectRestore~NoBodyResultCallback} callback callback function
 * @return {void}
 */
function objectRestore(metadata, mdUtils, func, userInfo, request, log, callback) {

    const { bucketName, objectKey } = request;
    const requestedAt = request['x-sdt-requested-at'];

    log.debug('processing request', { method: METHOD });

    const decodedVidResult = func.decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query',
            { method: METHOD, versionId: request.query.versionId, error: decodedVidResult });
        return callback(decodedVidResult, decodedVidResult.code);
    }

    const reqVersionId = decodedVidResult;

    const mdValueParams = {
        authInfo: userInfo,
        bucketName,
        objectKey,
        versionId: reqVersionId,
        requestType: 'bucketOwnerAction',
    };

    return async.waterfall([

        // get metadata of bucket and object
        function validateBucketAndObject(next) {

            return mdUtils.metadataValidateBucketAndObj(mdValueParams, log, (err, bucketMD, objectMD) => {

                if (err) {
                    log.trace('request authorization failed', { method: METHOD, error: err });
                    return next(err);
                }

                // Call back error if object metadata could not be obtained
                if (!objectMD) {
                    const err = reqVersionId ? errors.NoSuchVersion : errors.NoSuchKey;
                    log.trace('error no object metadata found', { method: METHOD, error: err });
                    return next(err, bucketMD);
                }

                const instance = new ObjectMD(objectMD);

                // If object metadata is delete marker,
                // call back NoSuchKey or MethodNotAllowed depending on specifying versionId
                if (objectMD.isDeleteMarker) {
                    let err = errors.NoSuchKey;
                    if (reqVersionId) {
                        err = errors.MethodNotAllowed;
                    }
                    log.trace('version is a delete marker', { method: METHOD, error: err });
                    return next(err, bucketMD, instance);
                }

                log.info('it acquired the object metadata.', {
                    'method': METHOD,
                    'x-coldstorage-uuid': instance.getColdstorageUuid(),
                    'x-coldstorage-zenko-id': instance.getColdstorageZenkoId(),
                });

                return next(null, bucketMD, instance);
            });
        },

        // generate restore param obj from xml of request body
        function parseRequestXml(bucketMD, objectMD, next) {

            return parsePostObjectRestoreXml(request.post, log, (err, params) => {

                if (err) {
                    return next(err, bucketMD, objectMD);
                }

                log.info('it parsed xml of the request body.', { method: METHOD, value: params });

                return next(null, bucketMD, objectMD, params);
            });
        },

        // start restore process
        function startRestore(bucketMD, objectMD, next) {
            return coldStorage.startRestore(bucketName, objectKey, objectMD, params, 
                (err, result) => next(err, bucketMD, objectMD, result));
        },
    ],
    (err, bucketMD, objectMD, result) => {

        // generate CORS response header
        const responseHeaders = func.collectCorsHeaders(request.headers.origin, request.method, bucketMD);

        if (err) {
            log.trace('error processing request', { method: METHOD, error: err });

            // If object metadata is delete marker and error is MethodNotAllowed,
            // set response header of x-amz-delete-marker and x-amz-version-id (S3 API compliant)
            if (objectMD && objectMD.getIsDeleteMarker() && err.MethodNotAllowed) {
                const vConfig = bucketMD.getVersioningConfiguration();
                responseHeaders['x-amz-delete-marker'] = true;
                responseHeaders['x-amz-version-id'] = func.getVersionIdResHeader(vConfig, objectMD.getValue());
            }

            return callback(err, err.code, responseHeaders);
        }

        // If versioning configuration is setting, set response header of x-amz-version-id
        const vConfig = bucketMD.getVersioningConfiguration();
        responseHeaders['x-amz-version-id'] = func.getVersionIdResHeader(vConfig, objectMD.getValue());

        return callback(null, result.statusCode, responseHeaders);
    });

    /**
     * Generate request parameter object by parsing XML ofrequest body
     *
     * @param {convertableToString} xml XML of request body
     * @param {werelogs.Logger} log logger
     * @param {module:api/utils~ObjectResultCallback} callback callback function
     * @returns {void}
     */
    function parsePostObjectRestoreXml(xml, log, callback) {

        log.debug('parsing xml string of request body.', alCreateLogParams(
            this, this.parsePostObjectRestoreXml, {
                xmlString: xml,
            // eslint-disable-next-line comma-dangle
            }
        ));

        return xml2js.parseString(xml, { explicitArray: false }, (err, result) => {

            // If cause an error, callback MalformedXML
            if (err) {
                log.info('parse xml string of request body was failed.', { error: err });
                return callback(errors.MalformedXML);
            }

            // If restore parameter is invalid, callback MalformedXML
            const validateResult = validateRestoreRequestParameters(result);
            if (validateResult) {
                log.info('invalid restore parameters.', { error: validateResult.message });
                return callback(errors.MalformedXML);
            }

            // normalize restore request parameters
            const normalizedResult = normalizeRestoreRequestParameters(result);

            log.debug('parse xml string of request body.', alCreateLogParams(
                this, this.parsePostObjectRestoreXml, {
                    resultObject: normalizedResult,
                // eslint-disable-next-line comma-dangle
                }
            ));

            return callback(null, normalizedResult);
        });
    };


    /**
     * validate restore parameter object
     *
     * @private
     * @param {object} params restore parameter object
     * @returns {Error} Error instance
     */
    function validateRestoreRequestParameters(params) {

        if (!params) {
            return new Error('request body is required.');
        }

        const rootElem = getSafeValue(params, 'RestoreRequest');
        if (!rootElem) {
            return new Error('RestoreRequest element is required.');
        }

        if (!rootElem['Days']) {
            return new Error('RestoreRequest.Days element is required.');
        }

        // RestoreRequest.Days must be greater than or equal to 1
        const daysValue = Number.parseInt(rootElem['Days'], 10);
        if (Number.isNaN(daysValue)) {
            return new Error(`RestoreRequest.Days is invalid type. [${rootElem['Days']}]`);
        }
        if (daysValue < 1) {
            return new Error(`RestoreRequest.Days must be greater than 0. [${rootElem['Days']}]`);
        }

        if (daysValue > 2147483647) {
            return new Error(`RestoreRequest.Days must be less than 2147483648. [${rootElem['Days']}]`);
        }

        // If RestoreRequest.GlacierJobParameters.Tier is specified,
        // Must be "Expedited" or "Standard" or "Bulk"
        const tierValue = getSafeValue(rootElem,
            'GlacierJobParameters', 'Tier');
        const tierList = {
            EXPEDITED: 'Expedited',
            STANDARD: 'Standard',
            BULK: 'Bulk',
        }
        const tierConstants = getValues(tierList);
        if (tierValue && !tierConstants.includes(tierValue)) {
            return new Error(`RestoreRequest.GlacierJobParameters.Tier is invalid value. [${tierValue}]`);
        }

        return undefined;
    }

    /**
     * Normalize restore request parameters.
     *
     * @private
     * @param {object} params restore request parameters object
     * @return {object} restore request parameters object(normalized)
     */
    function normalizeRestoreRequestParameters(params) {

        const normalizedParams = {};

        // set RestoreRequest.Days
        const rootElem = getSafeValue(params, 'RestoreRequest');
        const daysValue = Number.parseInt(rootElem['Days'], 10);
        setSafeValue(normalizedParams, daysValue, 'Days');

        // set RestoreRequest.GlacierJobParameters.Tier
        // If do not specify, set "Standard"
        const tierValue = getSafeValue(rootElem,
            'GlacierJobParameters', 'Tier')
            || 'Standard';
        setSafeValue(normalizedParams, tierValue,
            'GlacierJobParameters', 'Tier');

        return normalizedParams;
    }

    /**
     * Attribute values ​​that the object has are returned as an array.
     * Node v6 does not support Object.values, so prepare a function with the same result.
     *
     * @param {object} obj object
     * @returns {Array<object>} UTC date infomation(string)
     */
    function getValues(obj) {

        const results = [];

        Object.keys(obj).forEach(key => {
            results.push(obj[key]);
        });

        return results;
    }



    /**
     * For layered objects, safely get the value corresponding to the key passed in the variable length argument.
     *
     * @param {object} obj object
     * @param  {...string} args array of keys
     * @returns {object} 
     */
    function getSafeValue(obj, ...args) {

        let result = obj;

        if (!result || !Array.isArray(args) || args.length === 0) {
            return result;
        }

        args.some(value => {
            result = result[value];
            return !result;
        });

        return result;
    }


}


module.exports = {
    objectRestore,
};
