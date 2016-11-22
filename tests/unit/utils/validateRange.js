import assert from 'assert';

import { parseRange } from
    '../../../lib/api/apiUtils/object/parseRange';

function checkRange(rangeHeader, totalLength, expectedRange) {
    const { range, error } =
        parseRange(rangeHeader, totalLength);
    assert.ifError(error);
    assert.deepStrictEqual(range, expectedRange);
}

describe('parseRange function', () => {
    it('should return an array with the start and end if range is '
        + 'valid', () => {
        checkRange('bytes=0-9', 10, [0, 9]);
    });

    it('should set the end of the range at the total object length minus 1 ' +
        'if the provided end of range goes beyond the end of the object ' +
        'length', () => {
        checkRange('bytes=0-9', 8, [0, 7]);
    });

    it('should handle incomplete range specifier where only end offset is ' +
    'provided', () => {
        checkRange('bytes=-500', 10000, [9500, 9999]);
    });

    it('should handle incomplete range specifier where only start ' +
    'provided', () => {
        checkRange('bytes=9500-', 10000, [9500, 9999]);
    });

    it('should return undefined for the range if the range header ' +
        'format is invalid: missing equal', () => {
        checkRange('bytes0-9', 10);
    });

    it('should return undefined for the range if the range header ' +
        'format is invalid: missing dash', () => {
        checkRange('bytes=09', 10);
    });

    it('should return undefined for the range if the range header ' +
        'format is invalid: value invalid character', () => {
        checkRange('bytes=%-4', 10);
    });

    it('should return undefined for the range if the range header ' +
    'format is invalid: value not int', () => {
        checkRange('bytes=4-a', 10);
    });

    it('should return undefined for the range if the range header ' +
        'format is invalid: start > end', () => {
        checkRange('bytes=5-4', 10);
    });

    it('should return undefined for the range if the range header ' +
        'format is invalid: start > end', () => {
        checkRange('bytes=-2-5', 10);
    });

    it('should return InvalidRange if the range of the resource ' +
    'does not cover the byte range', () => {
        const rangeHeader = 'bytes=10-30';
        const totalLength = 10;
        const { range, error } = parseRange(rangeHeader, totalLength);
        assert.strictEqual(error.code, 416);
        assert.strictEqual(range, undefined);
    });
});
