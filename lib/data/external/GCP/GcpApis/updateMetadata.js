const async = require('async');
const { logger } = require('../GcpUtils');

/**
 * updateMetadata - update the metadata of an object. Only used when
 * changes to an object metadata should not affect the version id. Example:
 * objectTagging, in which creation/deletion of medatadata is required for GCP,
 * and copyObject.
 * @param {object} params - update metadata params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.VersionId - object version id
 * @param {function} callback - callback function to call with the object result
 * @return {undefined}
 */
function updateMetadata(params, callback) {
    this.setupGoogleClient();
    const file = this.getFileObject(params.Bucket, params.Key, params.VesionId);
    async.waterfall([
        next => file.getMetadata((err, resource) => {
            if (err) {
                logger.error('GCP Update Metadata: Retrieve', { error: err });
                return next(err);
            }
            return next(null, resource.metadata);
        }),
        (oldMetadata, next) => {
            const newMetadata = {};
            Object.keys(oldMetadata).forEach(key => {
                newMetadata[key] = null;
            });
            Object.assign(newMetadata, params.Metadata);
            const objectResource = {
                contentType: params.ContentType,
                contentEncoding: params.ContentEncoding,
                contentDisposition: params.ContentDisposition,
                contnentLanguage: params.ContentLanguage,
                cacheControl: params.cacheControl,
                metadata: newMetadata,
            };
            file.setMetadata(objectResource, err => {
                if (err) {
                    logger.error('GCP Update Metadata: set', { error: err });
                    return next(err);
                }
                return next();
            });
        },
    ], callback);
}

module.exports = updateMetadata;
