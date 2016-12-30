/**
 * Pulls data from saved object metadata to send in response
 * @param {object} objectMD - object's metadata
 * @param {object} headers - contains headers from request object
 * @return {object} responseMetaHeaders headers with object metadata to include
 * in response to client
 */
function collectResponseHeaders(objectMD) {
    // Add user meta headers from objectMD
    const responseMetaHeaders = {};
    Object.keys(objectMD).filter(val => (val.substr(0, 11) === 'x-amz-meta-' ||
        val === 'x-amz-website-redirect-location'))
        .forEach(id => { responseMetaHeaders[id] = objectMD[id]; });

    // TODO: When implement versioning and lifecycle,
    // add additional response headers
    // http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
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
    return responseMetaHeaders;
}

export default collectResponseHeaders;
