const assert = require('assert');

const removeFromCommaSeparatedList = require('../../../lib/utilities/removeFromCommaSeparatedList');
const { awsChunkedContentEncoding } = require('../../../constants');

describe('removeFromCommaSeparatedList', () => {
    describe('with aws-chunked', () => {
        [
            { input: undefined, expected: undefined },
            { input: null, expected: null },
            { input: '', expected: '' },
            { input: 'gzip', expected: 'gzip' },
            { input: 'gzip, br', expected: 'gzip, br' },
            { input: 'gzip,', expected: 'gzip,' },
            { input: 'AWS-Chunked,gzip', expected: 'AWS-Chunked,gzip' },
            { input: 'aws-chunked', expected: '' },
            { input: 'aws-chunked,gzip', expected: 'gzip' },
            { input: 'gzip,aws-chunked', expected: 'gzip' },
            { input: 'gzip,aws-chunked,br', expected: 'gzip,br' },
            { input: 'gzip, aws-chunked', expected: 'gzip' },
            { input: 'aws-chunked, gzip', expected: 'gzip' },
            { input: 'gzip, aws-chunked, br', expected: 'gzip, br' },
            { input: 'aws-chunked,gzip,aws-chunked', expected: 'gzip' },
        ].forEach(({ input, expected }) => {
            it(`should turn ${JSON.stringify(input)} into ${JSON.stringify(expected)}`, () => {
                assert.strictEqual(removeFromCommaSeparatedList(input, awsChunkedContentEncoding), expected);
            });
        });
    });

    describe('with empty elements', () => {
        [
            { input: undefined, expected: undefined },
            { input: null, expected: null },
            { input: '', expected: '' },
            { input: 'gzip', expected: 'gzip' },
            { input: 'gzip, br', expected: 'gzip, br' },
            { input: 'gzip,', expected: 'gzip' },
            { input: 'gzip, ', expected: 'gzip' },
            { input: ' gzip', expected: 'gzip' },
            { input: ',gzip', expected: 'gzip' },
            { input: 'gzip,,br', expected: 'gzip,br' },
            { input: ',', expected: '' },
        ].forEach(({ input, expected }) => {
            it(`should turn ${JSON.stringify(input)} into ${JSON.stringify(expected)}`, () => {
                assert.strictEqual(removeFromCommaSeparatedList(input, ''), expected);
            });
        });
    });

    describe('with aws-chunked and empty elements', () => {
        [
            { input: 'gzip,', expected: 'gzip' },
            { input: 'gzip,aws-chunked,', expected: 'gzip' },
            { input: 'gzip,,aws-chunked', expected: 'gzip' },
            { input: 'gzip, br', expected: 'gzip, br' },
        ].forEach(({ input, expected }) => {
            it(`should turn ${JSON.stringify(input)} into ${JSON.stringify(expected)}`, () => {
                assert.strictEqual(removeFromCommaSeparatedList(input, awsChunkedContentEncoding, ''), expected);
            });
        });
    });
});
