const assert = require('assert');
const { errors } = require('arsenal');
const { gcpTaggingPrefix } = require('../../../constants');
const { genPutTagObj } =
    require('../../../tests/functional/raw-node/utils/gcpUtils');
const { processTagSet, stripTags, retrieveTags, getPutTagsMetadata } =
    require('../../../lib/data/external/GCP').GcpUtils;

const maxTagSize = 10;
const validTagSet = genPutTagObj(2);
const validTagObj = {};
validTagObj[`${gcpTaggingPrefix}key0`] = 'Value0';
validTagObj[`${gcpTaggingPrefix}key1`] = 'Value1';
const tagQuery = 'key0=Value0&key1=Value1';
const invalidSizeTagSet = genPutTagObj(maxTagSize + 1);
const invalidDuplicateTagSet = genPutTagObj(maxTagSize, true);
const invalidKeyTagSet = [{ Key: Buffer.alloc(129, 'a'), Value: 'value' }];
const invalidValueTagSet = [{ Key: 'key', Value: Buffer.alloc(257, 'a') }];
const onlyMetadata = {
    metadata1: 'metadatavalue1',
    metadata2: 'metadatavalue2',
};
const tagMetadata = Object.assign({}, validTagObj, onlyMetadata);
const oldTagMetadata = {};
oldTagMetadata[`${gcpTaggingPrefix}Old`] = 'OldValue0';
const withPriorTags = Object.assign({}, onlyMetadata, oldTagMetadata);

describe('GcpUtils Tagging Helper Functions:', () => {
    describe('processTagSet', () => {
        const tests = [
            {
                it: 'should return tag object as metadata for valid tag set',
                input: validTagSet,
                output: validTagObj,
            },
            {
                it: 'should return error for invalid tag set size',
                input: invalidSizeTagSet,
                output: errors.BadRequest.customizeDescription(
                    'Object tags cannot be greater than 10'),
            },
            {
                it: 'should return error for duplicate tag keys',
                input: invalidDuplicateTagSet,
                output: errors.InvalidTag.customizeDescription(
                    'Cannot provide multiple Tags with the same key'),
            },
            {
                it: 'should return error for invalid "key" value',
                input: invalidKeyTagSet,
                output: errors.InvalidTag.customizeDescription(
                    'The TagKey you have provided is invalid'),
            },
            {
                it: 'should return error for invalid "value" value',
                input: invalidValueTagSet,
                output: errors.InvalidTag.customizeDescription(
                    'The TagValue you have provided is invalid'),
            },
            {
                it: 'should return empty tag object when input is undefined',
                input: undefined,
                output: {},
            },
        ];
        tests.forEach(test => {
            it(test.it, () => {
                assert.deepStrictEqual(processTagSet(test.input), test.output);
            });
        });
    });

    describe('stripTags', () => {
        const tests = [
            {
                it: 'should return metadata without tag',
                input: tagMetadata,
                output: onlyMetadata,
            },
            {
                it: 'should return empty object if metadata only has tags',
                input: validTagObj,
                output: {},
            },
            {
                it: 'should return empty object if input is undefined',
                input: undefined,
                output: {},
            },
        ];
        tests.forEach(test => {
            it(test.it, () => {
                assert.deepStrictEqual(stripTags(test.input), test.output);
            });
        });
    });

    describe('retrieveTags', () => {
        const tests = [
            {
                it: 'should return tagSet from given input metadata',
                input: tagMetadata,
                output: validTagSet,
            },
            {
                it: 'should return empty when metadata does not have tags',
                input: onlyMetadata,
                output: [],
            },
            {
                it: 'should return empty if input is undefined',
                input: undefined,
                output: [],
            },
        ];
        tests.forEach(test => {
            it(test.it, () => {
                assert.deepStrictEqual(retrieveTags(test.input), test.output);
            });
        });
    });

    describe('getPutTagsMetadata', () => {
        const tests = [
            {
                it: 'should return correct object when' +
                    ' given a tag query string and a metadata obj',
                input: { metadata: Object.assign({}, onlyMetadata), tagQuery },
                output: tagMetadata,
            },
            {
                it: 'should return correct object when given only query string',
                input: { tagQuery },
                output: validTagObj,
            },
            {
                it: 'should return correct object when only metadata is given',
                input: { metadata: onlyMetadata },
                output: onlyMetadata,
            },
            {
                it: 'should return metadata with correct tag properties ' +
                    'if given a metdata with prior tags and query string',
                input: { metadata: Object.assign({}, withPriorTags), tagQuery },
                output: tagMetadata,
            },
        ];
        tests.forEach(test => {
            it(test.it, () => {
                const { metadata, tagQuery } = test.input;
                assert.deepStrictEqual(
                    getPutTagsMetadata(metadata, tagQuery), test.output);
            });
        });
    });
});
