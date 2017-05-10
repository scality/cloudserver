import { errors } from 'arsenal';
import { parseRange } from 'arsenal/lib/network/http/utils';

import constants from '../../../../constants';
import routesUtils from '../../../routes/routesUtils';
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
export default
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
            dataLocator[i].masterKeyId = sourceObjMD
            ['x-amz-server-side-encryption-aws-kms-key-id'];
            dataLocator[i].algorithm =
                sourceObjMD['x-amz-server-side-encryption'];
        }
    }

    const sourceSize =
        parseInt(sourceObjMD['content-length'], 10);
    let copyObjectSize = sourceSize;
    if (rangeHeader) {
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
            dataLocator = routesUtils.setPartRanges(dataLocator, range);
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
