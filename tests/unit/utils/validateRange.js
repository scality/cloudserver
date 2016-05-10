import assert from 'assert';

import { parseRange } from
    '../../../lib/api/apiUtils/object/parseRange';

describe('parseRange function', () => {
    it('should return an array with the start and end if range is '
        + 'valid', () => {
        const rangeHeader = 'bytes=0-9';
        const totalLength = 10;
        const actual =
            parseRange(rangeHeader, totalLength);
        assert.deepStrictEqual(actual, [0, 9]);
    });

    it('should set the end of the range at the total object length minus 1 ' +
        'if the provided end of range goes beyond the end of the object ' +
        'length', () => {
        const rangeHeader = 'bytes=0-9';
        const totalLength = 8;
        const actual =
            parseRange(rangeHeader, totalLength);
        assert.deepStrictEqual(actual, [0, 7]);
    });

    it('should handle incomplete range specifier where only end offset is ' +
    'provided', () => {
        const rangeHeader = 'bytes=-500';
        const totalLength = 10000;
        const actual = parseRange(rangeHeader, totalLength);
        assert.deepStrictEqual(actual, [9500, 9999]);
    });

    it('should handle incomplete range specifier where only start ' +
    'provided', () => {
        const rangeHeader = 'bytes=9500-';
        const totalLength = 10000;
        const actual = parseRange(rangeHeader, totalLength);
        assert.deepStrictEqual(actual, [9500, 9999]);
    });

    it('should return undefined for the range if the range header ' +
        'format is invalid', () => {
        const rangeHeaderMissingEquals = 'bytes0-9';
        const totalLength = 10;
        const actualForMissingEquals =
            parseRange(rangeHeaderMissingEquals, totalLength);
        assert.deepStrictEqual(actualForMissingEquals, undefined);
        const rangeHeaderMissingDash = 'bytes=09';
        const actualForMissingDash =
            parseRange(rangeHeaderMissingDash, totalLength);
        assert.deepStrictEqual(actualForMissingDash, undefined);
        const notNumberStart = 'bytes=%-4';
        const actualForNaNStart = parseRange(notNumberStart, totalLength);
        assert.deepStrictEqual(actualForNaNStart, undefined);
        const notNumberEnd = 'bytes=4-a';
        const actualForNaNEnd = parseRange(notNumberEnd, totalLength);
        assert.deepStrictEqual(actualForNaNEnd, undefined);
        const endGreaterThanStart = 'bytes=5-4';
        const actualForEndGreater =
            parseRange(endGreaterThanStart, totalLength);
        assert.deepStrictEqual(actualForEndGreater, undefined);
        const negativeStart = 'bytes=-2-5';
        const actualForNegativeStart =
            parseRange(negativeStart, totalLength);
        assert.deepStrictEqual(actualForNegativeStart, undefined);
    });
});
