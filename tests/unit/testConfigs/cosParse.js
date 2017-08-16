const assert = require('assert');
const { cosParse } = require('../../../lib/Config');

const dummyChordCos = '2';

describe('cosParse', () => {
    it('should return the single digit of the string as an integer', () => {
        const parsed = cosParse(dummyChordCos);
        assert.strictEqual(parsed, 2);
    });
});
