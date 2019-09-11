const { maximumMetaHeadersSize,
    invalidObjectUserMetadataHeader } = require('../../../../constants');

/**
 * Checks the size of the user metadata in the object metadata and removes
 * them from the response if the size of the user metadata is larger than
 * the maximum size allowed. A custome metadata key is added to the response
 * with the number of user metadata keys not returned as its value
 * @param {object} responseMetadata - response metadata
 * @return {object} responseMetaHeaders headers with object metadata to include
 * in response to client
 */
function checkUserMetadataSize(responseMetadata) {
    let userMetadataSize = 0;
    // collect the user metadata keys from the object metadata
    const userMetadataHeaders = Object.keys(responseMetadata)
        .filter(key => key.startsWith('x-amz-meta-'));
    // compute the size of all user metadata key and its value
    userMetadataHeaders.forEach(header => {
        userMetadataSize += header.length + responseMetadata[header].length;
    });
    // check the size computed against the maximum allowed
    // if the computed size is greater, then remove all the
    // user metadata from the response object
    if (userMetadataSize > maximumMetaHeadersSize) {
        let md = {};
        md = Object.assign({}, responseMetadata);
        userMetadataHeaders.forEach(header => {
            delete md[header];
        });
        // add the prescribed/custom metadata with number of user metadata
        // as its value
        md[invalidObjectUserMetadataHeader] = userMetadataHeaders.length;
        return md;
    }
    return responseMetadata;
}

module.exports = checkUserMetadataSize;
