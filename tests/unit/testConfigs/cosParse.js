const assert = require('assert');
const { cosParse } = require('../../../lib/Config');

const dummyChordCos = '20';

describe('cosParse', () => {
    it('should return the first digit of a string number as an integer', () => {
        const parsed = cosParse(dummyChordCos);
        assert.strictEqual(parsed, 2);
    });
});
