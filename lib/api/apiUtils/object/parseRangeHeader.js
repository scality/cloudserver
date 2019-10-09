const { parseRangeSpec } = require('arsenal/lib/network/http/utils');

function parseRangeHeader(header) {
    const { error } = parseRangeSpec(header);
    if (error) {
        const description = 'The x-amz-copy-source-range value must be ' +
            'of the form bytes=first-last where first and last are the ' +
            'zero-based offsets of the first and last bytes to copy';
        return error.customizeDescription(description);
    }
    return null;
}

module.exports = parseRangeHeader;
