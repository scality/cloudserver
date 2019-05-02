const assert = require('assert');
const locationKeysSanityCheck =
      require('../../../../lib/api/apiUtils/object/locationKeysSanityCheck');

describe('Sanity check for location keys', () => {
    it('should return true for no match ', () => {
        const prev = [{ key: 'aaa' }, { key: 'bbb' }, { key: 'ccc' }];
        const curr = [{ key: 'ddd' }, { key: 'eee' }, { key: 'fff' }];
        assert.strictEqual(locationKeysSanityCheck(prev, curr), true);
    });

    it('should return false if there is a match of 1 key', () => {
        const prev = [{ key: 'aaa' }, { key: 'bbb' }, { key: 'ccc' }];
        const curr = [{ key: 'ddd' }, { key: 'aaa' }, { key: 'fff' }];
        assert.strictEqual(locationKeysSanityCheck(prev, curr), false);
    });

    it('should return false if all keys match', () => {
        const prev = [{ key: 'aaa' }, { key: 'bbb' }, { key: 'ccc' }];
        const curr = [{ key: 'aaa' }, { key: 'bbb' }, { key: 'ccc' }];
        assert.strictEqual(locationKeysSanityCheck(prev, curr), false);
    });

    it('should return false if there is match (model version 2)', () => {
        const prev = 'ccc';
        const curr = [{ key: 'aaa' }, { key: 'bbb' }, { key: 'ccc' }];
        assert.strictEqual(locationKeysSanityCheck(prev, curr), false);
    });

    it('should return true if there is no match(model version 2)', () => {
        const prev = 'aaa';
        const curr = [{ key: 'ddd' }, { key: 'eee' }, { key: 'fff' }];
        assert.strictEqual(locationKeysSanityCheck(prev, curr), true);
    });
});
