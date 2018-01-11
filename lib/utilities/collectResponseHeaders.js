const { getVersionIdResHeader } = require('../api/apiUtils/object/versioning');

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
    const responseMetaHeaders = Object.assign({}, corsHeaders);
    Object.keys(objectMD).filter(val => (val.substr(0, 11) === 'x-amz-meta-' ||
        val === 'x-amz-website-redirect-location'))
        .forEach(id => { responseMetaHeaders[id] = objectMD[id]; });

    // TODO: When implement lifecycle, add additional response headers
    // http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html

    responseMetaHeaders['x-amz-version-id'] =
        getVersionIdResHeader(versioningCfg, objectMD);

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
