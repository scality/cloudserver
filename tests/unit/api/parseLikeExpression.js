const assert = require('assert');
const parseLikeExpression =
    require('../../../lib/api/apiUtils/bucket/parseLikeExpression');

describe('parseLikeExpression', () => {
    const tests = [
        {
            input: '',
            output: { $regex: '' },
        },
        {
            input: 'ice-cream-cone',
            output: { $regex: 'ice-cream-cone' },
        },
        {
            input: '/ice-cream-cone/',
            output: { $regex: /ice-cream-cone/, $options: '' },
        },
        {
            input: '/ice-cream-cone/i',
            output: { $regex: /ice-cream-cone/, $options: 'i' },
        },
        {
            input: 'an/ice-cream-cone/',
            output: { $regex: 'an/ice-cream-cone/' },
        },
        {
            input: '///',
            output: { $regex: /\//, $options: '' },
        },
    ];
    tests.forEach(test => it('should return correct MongoDB query object: ' +
        `"${test.input}" => ${JSON.stringify(test.output)}`, () => {
        const res = parseLikeExpression(test.input);
        assert.deepStrictEqual(res, test.output);
    }));
    const badInputTests = [
        {
            input: null,
            output: null,
        },
        {
            input: 1235,
            output: null,
        },
    ];
    badInputTests.forEach(test => it(
        'should return null if input is not a string ' +
        `"${test.input}" => ${JSON.stringify(test.output)}`, () => {
        const res = parseLikeExpression(test.input);
        assert.deepStrictEqual(res, test.output);
    }));
});
