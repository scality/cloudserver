const { maximumMetaHeadersSize, invalidObjectUserMetadataHeader } = require('../../../../constants');

/**
 * Checks the size of the user metadata in the object metadata and removes
 * them from the response if the size of the user metadata is larger than
 * the maximum size allowed. A custom metadata key is added to the response
 * with the number of user metadata keys not returned as its value
 * @param {object} responseMetadata - response metadata
 * @return {object} headers: the headers with object metadata to include in
 * the response to the client; removedCount: how many user metadata headers
 * were left out
 */
function checkUserMetadataSize(responseMetadata) {
    let userMetadataSize = 0;
    // collect the user metadata keys from the object metadata
    const userMetadataHeaders = Object.keys(responseMetadata).filter(key => key.startsWith('x-amz-meta-'));
    // compute the size in bytes of all user metadata keys and values, as sent
    // on the wire
    userMetadataHeaders.forEach(header => {
        userMetadataSize += Buffer.byteLength(header, 'utf8') + Buffer.byteLength(responseMetadata[header], 'utf8');
    });
    // check the size computed against the maximum allowed
    // if the computed size is greater, then remove all the
    // user metadata from the response object
    if (userMetadataSize > maximumMetaHeadersSize) {
        const md = Object.assign({}, responseMetadata);
        userMetadataHeaders.forEach(header => {
            delete md[header];
        });
        // add the prescribed/custom metadata with number of user metadata
        // as its value
        md[invalidObjectUserMetadataHeader] = userMetadataHeaders.length;
        return { headers: md, removedCount: userMetadataHeaders.length };
    }
    return { headers: responseMetadata, removedCount: 0 };
}

module.exports = checkUserMetadataSize;
