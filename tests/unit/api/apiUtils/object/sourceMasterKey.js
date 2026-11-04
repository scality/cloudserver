const assert = require('assert');

const { _partToCheck } = require('../../../../../lib/api/apiUtils/object/sourceMasterKey');

const md5A = 'a'.repeat(32);
const md5B = 'b'.repeat(32);
const md5C = 'c'.repeat(32);
const md5Empty = 'd41d8cd98f00b204e9800998ecf8427e';
const encrypted = (key, size, dataStoreETag) => ({ key, size, dataStoreETag, cipheredDataKey: 'ZGF0YWtleQ==' });

describe('_partToCheck', () => {
    it('should pick the smallest part of at least 16 bytes, with a part copied from several locations', () => {
        const dataLocator = [
            encrypted(1, 20, `1:${md5A}`),
            encrypted(2, 8, `2:${md5B}`),
            encrypted(3, 8, `2:${md5B}`),
            encrypted(4, 18, `3:${md5C}`),
        ];
        assert.deepStrictEqual(_partToCheck(dataLocator), {
            locations: [dataLocator[1], dataLocator[2]],
            md5: md5B,
            size: 16,
        });
    });

    it('should skip parts with an unencrypted location or no MD5', () => {
        const dataLocator = [
            { key: 1, size: 1, dataStoreETag: `1:${md5A}` },
            encrypted(2, 2, undefined),
            encrypted(3, 3, `${md5B}-2`),
            encrypted(4, 9, `4:${md5C}`),
        ];
        assert.deepStrictEqual(_partToCheck(dataLocator).locations, [dataLocator[3]]);
    });

    it('should pick a part of at least 16 bytes over a smaller one', () => {
        const dataLocator = [
            encrypted(1, 20, `1:${md5A}`),
            encrypted(2, 16, `2:${md5B}`),
            encrypted(3, 15, `3:${md5C}`),
        ];
        assert.deepStrictEqual(_partToCheck(dataLocator).locations, [dataLocator[1]]);
    });

    it('should pick the largest part when every part is under 16 bytes', () => {
        const dataLocator = [encrypted(1, 10, `1:${md5A}`), encrypted(2, 4, `2:${md5B}`), encrypted(3, 8, `3:${md5C}`)];
        assert.deepStrictEqual(_partToCheck(dataLocator).locations, [dataLocator[0]]);
    });

    it('should pick a part with data over a smaller empty part', () => {
        const dataLocator = [encrypted(1, 10, `1:${md5A}`), encrypted(2, 0, `2:${md5Empty}`)];
        assert.deepStrictEqual(_partToCheck(dataLocator).locations, [dataLocator[0]]);
    });

    it('should pick an empty part when every part is empty', () => {
        const dataLocator = [encrypted(1, 0, `1:${md5Empty}`)];
        assert.deepStrictEqual(_partToCheck(dataLocator).locations, [dataLocator[0]]);
    });
});
