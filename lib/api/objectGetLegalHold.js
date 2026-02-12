const { promisify } = require('util');
const { errors, errorInstances, s3middleware } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader }
    = require('./apiUtils/object/versioning');

const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

const { convertToXml } = s3middleware.objectLegalHold;

/**
 * Returns legal hold status of object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @return {Promise<object>} - object containing xml and additionalResHeaders
 */
async function objectGetLegalHold(authInfo, request, log, callback) {
    if (callback) {
        return objectGetLegalHold(authInfo, request, log)
            .then(result => callback(null, ...result))
            .catch(err => callback(err, null, err.additionalResHeaders));
    }

    log.debug('processing request', { method: 'objectGetLegalHold' });

    const { bucketName, objectKey, query } = request;

    const decodedVidResult = decodeVersionId(query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', { versionId: query.versionId, error: decodedVidResult });
        throw decodedVidResult;
    }
    const versionId = decodedVidResult;

    // FIXME pass 'getDeleteMarker: true' option to set 'x-amz-delete-marker' header (see S3C-7592)
    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId,
        requestType: request.apiMethods || 'objectGetLegalHold',
        request,
    };

    let bucket, objectMD;

    try {
        const standardMetadataValidateBucketAndObjPromised = promisify(standardMetadataValidateBucketAndObj);
        ({ bucket, objectMD } = await standardMetadataValidateBucketAndObjPromised(
            metadataValParams,
            request.actionImplicitDenies,
            log,
        ));
    } catch (err) {
        log.trace('request authorization failed', { method: 'objectGetLegalHold', error: err });
        throw err;
    }

    if (!objectMD) {
        const err = versionId ? errors.NoSuchVersion : errors.NoSuchKey;
        log.trace('error no object metadata found', { method: 'objectGetLegalHold', error: err });
        throw err;
    }

    if (objectMD.isDeleteMarker) {
        if (versionId) {
            log.trace('requested version is delete marker', { method: 'objectGetLegalHold' });
            // FIXME we should return a `x-amz-delete-marker: true` header, see S3C-7592
            throw errors.MethodNotAllowed;
        }

        log.trace('most recent version is delete marker', { method: 'objectGetLegalHold' });
        // FIXME we should return a `x-amz-delete-marker: true` header, see S3C-7592
        throw errors.NoSuchKey;
    }

    if (!bucket.isObjectLockEnabled()) {
        log.trace('object lock not enabled on bucket', { method: 'objectGetRetention' });
        throw errorInstances.InvalidRequest.customizeDescription('Bucket is missing Object Lock Configuration');
    }

    const { legalHold } = objectMD;
    const xml = convertToXml(legalHold);
    if (xml === '') {
        throw errors.NoSuchObjectLockConfiguration;
    }

    const additionalResHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);

    pushMetric('getObjectLegalHold', log, {
        authInfo,
        bucket: bucketName,
        keys: [objectKey],
        versionId: objectMD ? objectMD.versionId : undefined,
        location: objectMD ? objectMD.dataStoreName : undefined,
    });
    const verCfg = bucket.getVersioningConfiguration();
    additionalResHeaders['x-amz-version-id'] = getVersionIdResHeader(verCfg, objectMD);

    return [xml, additionalResHeaders];
}

module.exports = (...args) => {
    const callback = args.at(-1);
    const argsWithoutCallback = args.slice(0, -1);

    objectGetLegalHold(...argsWithoutCallback)
        .then(result => callback(null, ...result))
        .catch(err => callback(err, null, err.additionalResHeaders));
};
