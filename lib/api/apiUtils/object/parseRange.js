
/**
 * parseRange - Validate and parse range request header
 * @param {string} rangeHeader - range header from request
 * which should be in form bytes=0-9
 * @param {number} totalLength - totalLength of object
 * @return {array | undefined} range with range[0] as start and
 * range[1] as end or undefined if rangeHeader was invalid
 */
export function parseRange(rangeHeader, totalLength) {
    // If the range is invalid in any manner, AWS just returns the full object
    // (end is inclusive so minus 1)
    const maxEnd = totalLength - 1;
    let range = undefined;
    if (!rangeHeader.startsWith('bytes=')
        || rangeHeader.indexOf('-') < 0
        // Multiple ranges not supported
        || rangeHeader.indexOf(',') > 0) {
        return range;
    }
    const rangePortion = rangeHeader.replace('bytes=', '').split('-');
    if (rangePortion.length > 2) {
        return range;
    }
    let start;
    let end;
    // Handle incomplete specifier where just offset from end is given
    if (rangePortion[0] === '') {
        const offset = parseInt(rangePortion[1], 10);
        if (Number.isNaN(offset)) {
            return range;
        }
        start = totalLength - offset;
        end = maxEnd;
    // Handle incomplete specifier where just starting place is given
    // meaning range goes from start of range to end of object
    } else if (rangePortion[1] === '') {
        start = parseInt(rangePortion[0], 10);
        end = maxEnd;
    } else {
        start = parseInt(rangePortion[0], 10);
        end = Math.min(parseInt(rangePortion[1], 10), maxEnd);
    }

    if (Number.isNaN(start) || Number.isNaN(end) || start > end) {
        return range;
    }
    range = [start, end];
    return range;
}
