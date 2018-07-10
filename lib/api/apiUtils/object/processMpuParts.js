const { errors } = require('arsenal');
const crypto = require('crypto');
const constants = require('../../../../constants');

/**
 * createAggregateETag - creates ETag from concatenated MPU part ETags to
 * mimic AWS
 * @param {string} concatETags - string of concatenated MPU part ETags
 * @param {array} partList - list of parts to complete MPU with
 * @return {string} aggregateETag - final complete MPU obj ETag
 */
function createAggregateETag(concatETags, partList) {
    // AWS documentation is unclear on what the MD5 is that it returns
    // in the response for a complete multipart upload request.
    // The docs state that they might or might not
    // return the MD5 of the complete object. It appears
    // they are returning the MD5 of the parts' MD5s so that is
    // what we have done here. We:
    // 1) concatenate the hex version of the
    // individual ETags
    // 2) convert the concatenated hex to binary
    // 3) take the md5 of the binary
    // 4) create the hex digest of the md5
    // 5) add '-' plus the number of parts at the end

    // Convert the concatenated hex ETags to binary
    const bufferedHex = Buffer.from(concatETags, 'hex');
    // Convert the buffer to a binary string
    const binaryString = bufferedHex.toString('binary');
    // Get the md5 of the binary string
    const md5Hash = crypto.createHash('md5');
    md5Hash.update(binaryString, 'binary');
    // Get the hex digest of the md5
    let aggregateETag = md5Hash.digest('hex');
    // Add the number of parts at the end
    aggregateETag = `${aggregateETag}-${partList.length}`;

    return aggregateETag;
}

/**
 * generateMpuPartStorageInfo - generates info needed for storage of
 * completed MPU object
 * @param {array} filteredPartList - list of parts filtered from metadata
 * @return {object} partsInfo - contains three keys: aggregateETag,
 * dataLocations, and calculatedSize
 */
function generateMpuPartStorageInfo(filteredPartList) {
    // Assemble array of part locations, aggregate size
    // and build string to create aggregate ETag
    let calculatedSize = 0;
    const dataLocations = [];
    let concatETags = '';
    const partsInfo = {};

    filteredPartList.forEach((storedPart, index) => {
        const partETagWithoutQuotes =
            storedPart.ETag.slice(1, -1);
        const dataStoreETag = `${index + 1}:${partETagWithoutQuotes}`;
        concatETags += partETagWithoutQuotes;

        // If part was put by a regular put part rather than a
        // copy it is always one location.  With a put part
        // copy, could be multiple locations so loop over array
        // of locations.
        for (let j = 0; j < storedPart.locations.length; j++) {
            // If the piece has parts (was a put part object
            // copy) each piece will have a size attribute.
            // Otherwise, the piece was put by a regular put
            // part and the size the of the piece is the full
            // part size.
            const location = storedPart.locations[j];
            // If there is no location, move on
            if (!location || typeof location !== 'object') {
                continue;
            }
            let pieceSize = Number.parseInt(storedPart.size, 10);
            if (location.size) {
                pieceSize = Number.parseInt(location.size, 10);
            }
            const pieceRetrievalInfo = {
                key: location.key,
                size: pieceSize,
                start: calculatedSize,
                dataStoreName: location.dataStoreName,
                dataStoreETag,
                cryptoScheme: location.sseCryptoScheme,
                cipheredDataKey: location.sseCipheredDataKey,
            };
            dataLocations.push(pieceRetrievalInfo);
            // eslint-disable-next-line no-param-reassign
            calculatedSize += pieceSize;
        }
    });

    partsInfo.aggregateETag =
        createAggregateETag(concatETags, filteredPartList);
    partsInfo.dataLocations = dataLocations;
    partsInfo.calculatedSize = calculatedSize;
    return partsInfo;
}

/**
 * validateAndFilterMpuParts - validates part list sent by user and filters
 * parts stored in metadata against user part list
 * @param {array} storedParts - array of parts stored in metadata
 * @param {array} jsonList - array of parts sent by user for completion
 * @param {string} mpuOverviewKey - metadata mpu key
 * @param {string} splitter - mpu key divider
 * @param {object} log - Werelogs instance
 * @return {object} filtersPartsObj - contains 3 keys: partList, keysToDelete,
 * and extraPartLocations
 */
function validateAndFilterMpuParts(storedParts, jsonList, mpuOverviewKey,
splitter, log) {
    let storedPartsCopy = [];
    const filteredPartsObj = {};
    filteredPartsObj.partList = [];

    const keysToDelete = [];
    storedParts.forEach(item => {
        keysToDelete.push(item.key);
        storedPartsCopy.push({
            // In order to delete the part listing in the shadow
            // bucket, need the full key
            key: item.key,
            ETag: `"${item.value.ETag}"`,
            size: item.value.Size,
            locations: Array.isArray(item.value.partLocations) ?
                item.value.partLocations : [item.value.partLocations],
        });
    });
    keysToDelete.push(mpuOverviewKey);

    // Check list sent to make sure valid
    const partLength = jsonList.Part.length;
    // A user can put more parts than they end up including
    // in the completed MPU but there cannot be more
    // parts in the complete message than were already put
    if (partLength > storedPartsCopy.length) {
        filteredPartsObj.error = errors.InvalidPart;
        return filteredPartsObj;
    }

    let extraParts = [];
    const extraPartLocations = [];

    for (let i = 0; i < partLength; i++) {
        const part = jsonList.Part[i];
        const partNumber = Number.parseInt(part.PartNumber[0], 10);
        // If the complete list of parts sent with
        // the complete multipart upload request is not
        // in ascending order return an error
        if (i > 0) {
            const previousPartNumber =
                Number.parseInt(jsonList.Part[i - 1].PartNumber[0], 10);
            if (partNumber <= previousPartNumber) {
                filteredPartsObj.error = errors.InvalidPartOrder;
                return filteredPartsObj;
            }
        }

        let isPartUploaded = false;
        while (storedPartsCopy.length > 0 && !isPartUploaded) {
            const storedPart = storedPartsCopy[0];
            const storedPartNumber =
                Number.parseInt(storedPart.key.split(splitter)[1], 10);

            if (storedPartNumber === partNumber) {
                isPartUploaded = true;
                filteredPartsObj.partList.push(storedPart);

                let partETag = part.ETag[0].replace(/['"]/g, '');
                // some clients send base64, convert to hex
                // 32 chars = 16 bytes(2 chars-per-byte) = 128 bits of
                // MD5 hex
                if (partETag.length !== 32) {
                    const buffered = Buffer.from(part.ETag[0], 'base64')
                        .toString('hex');
                    partETag = `${buffered}`;
                }
                partETag = `"${partETag}"`;
                // If list of parts sent with complete mpu request contains
                // a part ETag that does not match the ETag for the part
                // stored in metadata, return an error
                if (partETag !== storedPart.ETag) {
                    filteredPartsObj.error = errors.InvalidPart;
                    return filteredPartsObj;
                }

                // If any part other than the last part is less than
                // 5MB, return an error
                const storedPartSize =
                    Number.parseInt(storedPart.size, 10);
                // allow smaller parts for testing
                if (process.env.MPU_TESTING) {
                    log.info('MPU_TESTING env variable setting',
                        { setting: process.env.MPU_TESTING });
                }
                if (process.env.MPU_TESTING !== 'yes' &&
                i < jsonList.Part.length - 1 &&
                storedPartSize < constants.minimumAllowedPartSize) {
                    log.debug('part too small on complete mpu');
                    filteredPartsObj.error = errors.EntityTooSmall;
                    return filteredPartsObj;
                }

                storedPartsCopy = storedPartsCopy.splice(1);
            } else {
                extraParts.push(storedPart);
                storedPartsCopy = storedPartsCopy.splice(1);
            }
        }
        if (!isPartUploaded) {
            filteredPartsObj.error = errors.InvalidPart;
            return filteredPartsObj;
        }
    }
    extraParts = extraParts.concat(storedPartsCopy);
    // if extra parts, need to delete the data when done with completing
    // mpu so extract the info to delete here
    if (extraParts.length > 0) {
        extraParts.forEach(part => {
            const locations = part.locations;
            locations.forEach(location => {
                if (!location || typeof location !== 'object') {
                    return;
                }
                extraPartLocations.push(location);
            });
        });
    }
    filteredPartsObj.keysToDelete = keysToDelete;
    filteredPartsObj.extraPartLocations = extraPartLocations;
    return filteredPartsObj;
}

module.exports = {
    generateMpuPartStorageInfo,
    validateAndFilterMpuParts,
    createAggregateETag,
};
