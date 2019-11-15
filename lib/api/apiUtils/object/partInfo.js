/**
 * Checks for partNumber in request query and returns the part number
 * @param {object} query - request query
 * @return {(Integer|undefined)} - part number, zero if part number is less than
 * 1 and undefined if no partNumber is found in the request query
 */
function getPartNumber(query) {
    if (query && query.partNumber !== undefined) {
        return Number.isNaN(query.partNumber) ?
            0 : Number.parseInt(query.partNumber, 10);
    }
    return undefined;
}

/**
 * Gets the size of the requested part of the object
 * @param {object} objMD - object metadata
 * @param {object} partNumber - part number
 * @return {(Integer|undefined)} - size of the part or undefined
 */
function getPartSize(objMD, partNumber) {
    let size;
    if (partNumber && objMD && objMD.location
        && objMD.location.length >= partNumber) {
        const locations = [];
        for (let i = 0; i < objMD.location.length; i++) {
            const { dataStoreETag } = objMD.location[i];
            const locationPartNumber =
                Number.parseInt(dataStoreETag.split(':')[0], 10);
            // Get all parts that belong to the requested part number
            if (partNumber === locationPartNumber) {
                locations.push(objMD.location[i]);
            } else if (locationPartNumber > partNumber) {
                break;
            }
        }
        if (locations.length > 0) {
            const { start } = locations[0];
            const endLocation = locations[locations.length - 1];
            const end = endLocation.start + endLocation.size - 1;
            size = end - start + 1;
        }
    }
    return size;
}

module.exports = {
    getPartNumber,
    getPartSize,
};
