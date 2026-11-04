const assert = require('assert');

const { _smallestPart } = require('../../../../../lib/api/apiUtils/object/sourceMasterKey');

const md5A = 'a'.repeat(32);
const md5B = 'b'.repeat(32);
const md5C = 'c'.repeat(32);
const md5Empty = 'd41d8cd98f00b204e9800998ecf8427e';
const encrypted = (key, size, dataStoreETag) => ({ key, size, dataStoreETag, cipheredDataKey: 'ZGF0YWtleQ==' });

describe('_smallestPart', () => {
    it('should pick the smallest part, with every location of a part copied from several locations', () => {
        const dataLocator = [
            encrypted(1, 10, `1:${md5A}`),
            encrypted(2, 4, `2:${md5B}`),
            encrypted(3, 2, `2:${md5B}`),
            encrypted(4, 8, `3:${md5C}`),
        ];
        assert.deepStrictEqual(_smallestPart(dataLocator), {
            locations: [dataLocator[1], dataLocator[2]],
            md5: md5B,
            size: 6,
        });
    });

    it('should skip parts with an unencrypted location or no MD5', () => {
        const dataLocator = [
            { key: 1, size: 1, dataStoreETag: `1:${md5A}` },
            encrypted(2, 2, undefined),
            encrypted(3, 3, `${md5B}-2`),
            encrypted(4, 9, `4:${md5C}`),
        ];
        assert.deepStrictEqual(_smallestPart(dataLocator).locations, [dataLocator[3]]);
    });

    it('should pick a part with data over a smaller empty part', () => {
        const dataLocator = [encrypted(1, 10, `1:${md5A}`), encrypted(2, 0, `2:${md5Empty}`)];
        assert.deepStrictEqual(_smallestPart(dataLocator).locations, [dataLocator[0]]);
    });

    it('should pick an empty part when every part is empty', () => {
        const dataLocator = [encrypted(1, 0, `1:${md5Empty}`)];
        assert.deepStrictEqual(_smallestPart(dataLocator).locations, [dataLocator[0]]);
    });
});
