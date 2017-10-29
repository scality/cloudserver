/**
 * @param {array} dataLocations - all data locations
 * @param {array} outerRange - range from request
 * @return {array} parsedLocations - dataLocations filtered for
 * what needed and ranges added for particular parts as needed
 */
function setPartRanges(dataLocations, outerRange) {
    const parsedLocations = [];

    if (!outerRange) {
        return dataLocations.slice();
    }

    const begin = outerRange[0];
    const end = outerRange[1];
    // If have single location, do not need to break up range among parts
    // and might not have a start and size property
    // on the dataLocation (because might be pre- md-model-version 2),
    // so just set range as property
    if (dataLocations.length === 1) {
        const soleLocation = dataLocations[0];
        soleLocation.range = [begin, end];
        // If missing size, does not impact get range.
        // We modify size here in case this function is used for
        // object put part copy where will need size.
        // If pre-md-model-version 2, object put part copy will not
        // be allowed, so not an issue that size not modified here.
        if (dataLocations[0].size) {
            const partSize = parseInt(dataLocations[0].size, 10);
            soleLocation.size =
                Math.min(partSize, end - begin + 1).toString();
        }
        parsedLocations.push(soleLocation);
        return parsedLocations;
    }
    // Range is inclusive of endpoint so need plus 1
    const max = end - begin + 1;
    let total = 0;
    for (let i = 0; i < dataLocations.length; i++) {
        if (total >= (max - 1)) {
            break;
        }
        const partStart = parseInt(dataLocations[i].start, 10);
        const partSize = parseInt(dataLocations[i].size, 10);
        if (partStart + partSize <= begin) {
            continue;
        }
        if (partStart >= begin) {
            // If the whole part is in the range, just include it
            if (partSize + total <= max) {
                const partWithoutRange = dataLocations[i];
                partWithoutRange.size = partSize.toString();
                parsedLocations.push(partWithoutRange);
                total += partSize;
                // Otherwise set a range limit on the part end
                // and we're done
            } else {
                const partWithRange = dataLocations[i];
                // Need to subtract one from endPart since range
                // includes endPart in byte count
                const endPart = Math.min(partSize - 1, max - total - 1);
                partWithRange.range = [0, endPart];
                // modify size to be stored for object put part copy
                partWithRange.size = (endPart + 1).toString();
                parsedLocations.push(dataLocations[i]);
                break;
            }
        } else {
            // Offset start (and end if necessary)
            const partWithRange = dataLocations[i];
            const startOffset = begin - partStart;
            // Use full remaining part if remaining partSize is less
            // than byte range we need to satisfy.  Or use byte range
            // we need to satisfy taking into account any startOffset
            const endPart = Math.min(partSize - 1,
                max - total + startOffset - 1);
            partWithRange.range = [startOffset, endPart];
            // modify size to be stored for object put part copy
            partWithRange.size = (endPart - startOffset + 1).toString();
            parsedLocations.push(partWithRange);
            // Need to add byte back since with total we are counting
            // number of bytes while the endPart and startOffset
            // are in terms of range which include the endpoint
            total += endPart - startOffset + 1;
        }
    }
    return parsedLocations;
}

module.exports = setPartRanges;
