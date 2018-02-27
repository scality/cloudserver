const { errors } = require('arsenal');
const { getSourceInfo, logger } = require('../GcpUtils');

/**
 * copyObject - minimum required functionality to perform object copy
 * for GCP Backend
 * @param {object} params - update metadata params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} param.CopySource - source object
 * @param {function} callback - callback function to call with the copy object
 * result
 * @return {undefined}
 */
function copyObject(params, callback) {
    const { CopySource } = params;
    if (!CopySource || typeof CopySource !== 'string') {
        return callback(errors.InvalidArgument);
    }
    const { sourceBucket, sourceObject } = getSourceInfo(CopySource);
    if (!sourceBucket || !sourceObject) {
        return callback(errors.InvalidArgument);
    }
    this.setupGoogleClient();
    const sourceFile = this.getFileObject(sourceBucket, sourceObject);
    const destFile = this.getFileObject(params.Bucket, params.Key);
    const objectResource = {};
    if (params.MetadataDirective === 'REPLACE') {
        objectResource.contentType = params.ContentType;
        objectResource.contentEncoding = params.ContentEndcoding;
        objectResource.contentDisposition = params.ContentDisposition;
        objectResource.contentLanguage = params.ContentLanguage;
        objectResource.metadata = params.Metadata;
        objectResource.cacheControl = params.CacheControl;
    }
    return sourceFile.copy(destFile, objectResource, (err, file, resp) => {
        if (err) {
            logger.error('GCP Copy Object Error', { error: err });
            return callback(err);
        }
        const result = resp.resource;
        const md5Hash = result.md5Hash ?
            Buffer.from(result.md5Hash, 'base64').toString('hex') : undefined;
        const resObj = { CopyObjectResult: {} };
        if (md5Hash !== undefined) {
            resObj.CopyObjectResult.ETag = md5Hash;
        }
        if (result.updated !== undefined) {
            resObj.CopyObjectResult.LastModified = result.updated;
        }
        if (result.size !== undefined && !isNaN(result.size) &&
        (typeof result.size === 'string' || typeof result.size === 'number')) {
            resObj.ContentLength = parseInt(result.size, 10);
        }
        if (result.generation !== undefined) {
            resObj.VersionId = result.generation;
        }
        return callback(null, resObj);
    });
}

module.exports = copyObject;
