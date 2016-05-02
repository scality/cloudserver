
/**
 * validateRange - Validate range request header
 * @param {string} rangeHeader - range header from request
 * which should be in form bytes=0-9
 * @return {array} range with range[0] as start and range[1] as end
 * if range request is invalid, returns []
 */
export function validateRange(rangeHeader) {
    const range = [];
    if (rangeHeader.indexOf('=') < 0 || rangeHeader.indexOf('-') < 0) {
        return range;
    }
    const rangePortion = rangeHeader.split('=')[1].split('-');
    const start = parseInt(rangePortion[0], 10);
    const end = parseInt(rangePortion[1], 10);

    if (isNaN(start) || isNaN(end) || start > end || start < 0) {
        return range;
    }
    range[0] = start;
    range[1] = end;
    return range;
}
