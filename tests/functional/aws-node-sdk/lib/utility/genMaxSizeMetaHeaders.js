const constants = require('../../../../../constants');

function genMaxSizeMetaHeaders() {
    const metaHeaders = {};
    const counter = 8;
    const bytesPerHeader =
        (constants.maximumMetaHeadersSize / counter);
    for (let i = 0; i < counter; i++) {
        const key = `header${i}`;
        const valueLength = bytesPerHeader -
            ('x-amz-meta-'.length + key.length);
        metaHeaders[key] = '0'.repeat(valueLength);
    }
    return metaHeaders;
}

module.exports = genMaxSizeMetaHeaders;
