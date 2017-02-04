import { errors } from 'arsenal';

/**
 * parseRange - Validate and parse range request header
 * @param {string} rangeHeader - range header from request
 * which should be in form bytes=0-9
 * @param {number} totalLength - totalLength of object
 * @return {object} object containing range (array | undefined) and  error if
 * range is invalid
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
        return { range };
    }
    const rangePortion = rangeHeader.replace('bytes=', '').split('-');
    if (rangePortion.length > 2) {
        return { range };
    }
    let start;
    let end;
    // Handle incomplete specifier where just offset from end is given
    if (rangePortion[0] === '') {
        const offset = parseInt(rangePortion[1], 10);
        if (Number.isNaN(offset)) {
            return { range };
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
        end = parseInt(rangePortion[1], 10);
    }
    // InvalidRange when the resource being accessed does not cover
    // the byte range
    if (start >= totalLength && end >= totalLength) {
        return { range, error: errors.InvalidRange };
    }
    end = Math.min(end, maxEnd);

    if (Number.isNaN(start) || Number.isNaN(end) || start > end) {
        return { range };
    }
    if (start < 0) {
        start = 0;
    }
    range = [start, end];
    return { range };
}
