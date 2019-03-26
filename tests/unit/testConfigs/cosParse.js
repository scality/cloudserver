const assert = require('assert');
const { cosParse } = require('../../../lib/Config');

const dummyChordCos = '2';

describe('cosParse', () => {
    test('should return the single digit of the string as an integer', () => {
        const parsed = cosParse(dummyChordCos);
        expect(parsed).toBe(2);
    });
});
