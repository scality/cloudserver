/**
 * validRangeForEmptyFile - Validate range request header on empty object
 * @param {string} rangeHeader - range header from request
 * which should be in form bytes=0-9
 * @return {bool} object true if valid range, false if not
 */
export function validRangeOnEmptyFile(rangeHeader) {
    if (rangeHeader.startsWith('bytes=') && rangeHeader.indexOf('-') > -1) {
        const rangePortion = rangeHeader.replace('bytes=', '').split('-');
        if (rangePortion.length === 2) {
            const start = parseInt(rangePortion[0], 10);
            const end = parseInt(rangePortion[1], 10);
            // if start value only, value has to be a positive integer
            const isValidStart = rangePortion[0] !== '' &&
              rangePortion[1] === '' && !Number.isNaN(start);
            if (isValidStart) {
                return true;
            }
            // if end value only, value has to be 0
            const isValidEnd = rangePortion[0] === '' && end === 0;
            if (isValidEnd) {
                return true;
            }
            // if start and end values, both values have to be positive
            // integer and end value superior or equal to start value
            const isValidStartAndEnd =
              rangePortion[0] !== '' && rangePortion[1] !== '' &&
              !Number.isNaN(start) && !Number.isNaN(end) && end >= start;
            if (isValidStartAndEnd) {
                return true;
            }
        }
    }
    return false;
}
