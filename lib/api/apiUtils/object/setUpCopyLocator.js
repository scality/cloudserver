const { errors } = require('arsenal');
const {
    parseRangeSpec,
    parseRange,
} = require('arsenal/lib/network/http/utils');

const constants = require('../../../../constants');
const setPartRanges = require('./setPartRanges');

/**
 * Ensure an object copy part range header is of the form 'bytes=first-last'.
 * @param {string | undefined} header - header from request, if any
 * @return {object | null} custom error if header is incorrect form or null
 */
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

/**
 * Uses the source object metadata and the requestHeaders
 * to determine the location of the data to be copied and the
 * size of the resulting part
 * @param {object} sourceObjMD - source object metadata
 * @param {string | undefined } rangeHeader - rangeHeader from request if any
 * @param {object} log - logger object
 * @return {object} object containing error if any or a dataLocator (array)
 * and objectSize (number) if no error
 */
function setUpCopyLocator(sourceObjMD, rangeHeader, log) {
    let dataLocator;
    // If 0 byte object just set dataLocator to empty array
    if (!sourceObjMD.location) {
        dataLocator = [];
    } else {
        // To provide for backwards compatibility before
        // md-model-version 2, need to handle cases where
        // objMD.location is just a string
        dataLocator = Array.isArray(sourceObjMD.location) ?
        sourceObjMD.location : [{ key: sourceObjMD.location }];
    }

    if (sourceObjMD['x-amz-server-side-encryption']) {
        for (let i = 0; i < dataLocator.length; i++) {
            dataLocator[i].masterKeyId =
                sourceObjMD['x-amz-server-side-encryption-aws-kms-key-id'];
            dataLocator[i].algorithm =
                sourceObjMD['x-amz-server-side-encryption'];
        }
    }

    const sourceSize =
        parseInt(sourceObjMD['content-length'], 10);
    let copyObjectSize = sourceSize;
    if (rangeHeader) {
        const rangeHeaderError = parseRangeHeader(rangeHeader);
        if (rangeHeaderError) {
            return { error: rangeHeaderError };
        }
        const { range, error } = parseRange(rangeHeader, sourceSize);
        if (error) {
            return { error };
        }
        // If have a data model before version 2, cannot
        // support get range copy (do not have size
        // stored with data locations)
        if ((range && dataLocator.length >= 1) &&
            (dataLocator[0].start === undefined
            || dataLocator[0].size === undefined)) {
            log.trace('data model before version 2 so ' +
            'cannot support get range copy part');
            return { error: errors.NotImplemented
                    .customizeDescription('Stored object ' +
                    'has legacy data storage model so does' +
                    ' not support range headers on copy part'),
            };
        }
        if (range) {
            dataLocator = setPartRanges(dataLocator, range);
            copyObjectSize = range[1] - range[0] + 1;
        }
    }
    if (copyObjectSize > constants.maximumAllowedPartSize) {
        log.debug('copy part size too large', { sourceSize, rangeHeader,
            copyObjectSize });
        return { error: errors.EntityTooLarge };
    }
    return { dataLocator, copyObjectSize };
}

module.exports = setUpCopyLocator;
