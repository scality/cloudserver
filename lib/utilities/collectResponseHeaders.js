const { getVersionIdResHeader } = require('../api/apiUtils/object/versioning');
const checkUserMetadataSize
    = require('../api/apiUtils/object/checkUserMetadataSize');
const { getAmzRestoreResHeader } = require('../api/apiUtils/object/coldStorage');

/**
 * Pulls data from saved object metadata to send in response
 * @param {object} objectMD - object's metadata
 * @param {object} corsHeaders - if cors headers exist, use as basis for adding
 * other response headers
 * @param {object} versioningCfg - if bucket is configured for versioning,
 * return version ID
 * @param {boolean} returnTagCount - returns the x-amz-tagging-count header
 * @return {object} responseMetaHeaders headers with object metadata to include
 * in response to client
 */
function collectResponseHeaders(objectMD, corsHeaders, versioningCfg,
    returnTagCount) {
    // Add user meta headers from objectMD
    let responseMetaHeaders = Object.assign({}, corsHeaders);
    Object.keys(objectMD).filter(val => (val.startsWith('x-amz-meta-')))
        .forEach(id => { responseMetaHeaders[id] = objectMD[id]; });
    // Check user metadata size
    responseMetaHeaders = checkUserMetadataSize(responseMetaHeaders);

    // TODO: When implement lifecycle, add additional response headers
    // http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html

    responseMetaHeaders['x-amz-version-id'] =
        getVersionIdResHeader(versioningCfg, objectMD);

    if (objectMD['x-amz-website-redirect-location']) {
        responseMetaHeaders['x-amz-website-redirect-location'] =
        objectMD['x-amz-website-redirect-location'];
    }
    if (objectMD['x-amz-storage-class'] !== 'STANDARD') {
        responseMetaHeaders['x-amz-storage-class'] =
            objectMD['x-amz-storage-class'];
    }
    if (objectMD['x-amz-server-side-encryption']) {
        responseMetaHeaders['x-amz-server-side-encryption']
            = objectMD['x-amz-server-side-encryption'];
    }
    if (objectMD['x-amz-server-side-encryption-aws-kms-key-id'] &&
        objectMD['x-amz-server-side-encryption'] === 'aws:kms') {
        responseMetaHeaders['x-amz-server-side-encryption-aws-kms-key-id']
            = objectMD['x-amz-server-side-encryption-aws-kms-key-id'];
    }

    const restoreHeader = getAmzRestoreResHeader(objectMD);
    if (restoreHeader) {
        responseMetaHeaders['x-amz-restore'] = restoreHeader;
    }

    if (objectMD['x-amz-scal-transition-in-progress']) {
        responseMetaHeaders['x-amz-meta-scal-s3-transition-in-progress'] = true;
    }

    responseMetaHeaders['Accept-Ranges'] = 'bytes';

    if (objectMD['cache-control']) {
        responseMetaHeaders['Cache-Control'] = objectMD['cache-control'];
    }
    if (objectMD['content-disposition']) {
        responseMetaHeaders['Content-Disposition']
          = objectMD['content-disposition'];
    }
    if (objectMD['content-encoding']) {
        responseMetaHeaders['Content-Encoding'] = objectMD['content-encoding'];
    }
    if (objectMD.expires) {
        responseMetaHeaders.Expires = objectMD.expires;
    }
    responseMetaHeaders['Content-Length'] = objectMD['content-length'];
    // Note: ETag must have a capital "E" and capital "T" for cosbench
    // to work.
    responseMetaHeaders.ETag = `"${objectMD['content-md5']}"`;
    responseMetaHeaders['Last-Modified'] =
        new Date(objectMD['last-modified']).toUTCString();
    if (objectMD['content-type']) {
        responseMetaHeaders['Content-Type'] = objectMD['content-type'];
    }
    if (returnTagCount && objectMD.tags &&
    Object.keys(objectMD.tags).length > 0) {
        responseMetaHeaders['x-amz-tagging-count'] =
          Object.keys(objectMD.tags).length;
    }
    const hasRetentionInfo = objectMD.retentionMode
        && objectMD.retentionDate;
    if (hasRetentionInfo) {
        responseMetaHeaders['x-amz-object-lock-retain-until-date']
            = objectMD.retentionDate;
        responseMetaHeaders['x-amz-object-lock-mode']
            = objectMD.retentionMode;
    }
    if (objectMD.legalHold !== undefined) {
        responseMetaHeaders['x-amz-object-lock-legal-hold']
            = objectMD.legalHold ? 'ON' : 'OFF';
    }
    if (objectMD.replicationInfo && objectMD.replicationInfo.status) {
        responseMetaHeaders['x-amz-replication-status'] =
            objectMD.replicationInfo.status;
    }
    if (objectMD.replicationInfo &&
        // Use storageType to determine if user metadata is needed.
        objectMD.replicationInfo.storageType &&
        Array.isArray(objectMD.replicationInfo.backends)) {
        objectMD.replicationInfo.backends.forEach(backend => {
            const { status, site, dataStoreVersionId } = backend;
            responseMetaHeaders[`x-amz-meta-${site}-replication-status`] =
                status;
            if (status === 'COMPLETED' && dataStoreVersionId) {
                responseMetaHeaders[`x-amz-meta-${site}-version-id`] =
                    dataStoreVersionId;
            }
        });
    }
    return responseMetaHeaders;
}

module.exports = collectResponseHeaders;
