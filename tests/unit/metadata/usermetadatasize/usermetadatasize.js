const assert = require('assert');
const { invalidObjectUserMetadataHeader } = require('../../../../constants');
const genMaxSizeMetaHeaders = require(
    '../../../functional/aws-node-sdk/lib/utility/genMaxSizeMetaHeaders');
const checkUserMetadataSize
    = require('../../../../lib/api/apiUtils/object/checkUserMetadataSize');

const userMetadataKey = 'x-amz-meta-';
const metadata = {};

let userMetadataKeys = 0;

describe('Check user metadata size', () => {
    before('Set up metadata', () => {
        const md = genMaxSizeMetaHeaders();
        Object.keys(md).forEach(key => {
            metadata[`${userMetadataKey}${key}`] = md[key];
        });
        userMetadataKeys = Object.keys(metadata)
            .filter(key => key.startsWith(userMetadataKey)).length;
    });

    it('Should return user metadata when the size is within limits', () => {
        const responseMetadata = checkUserMetadataSize(metadata);
        const invalidHeader
            = responseMetadata[invalidObjectUserMetadataHeader];
        assert.strictEqual(userMetadataKeys > 0, true);
        assert.strictEqual(invalidHeader, undefined);
        assert.deepStrictEqual(metadata, responseMetadata);
    });

    it('Should not return user metadata when the size exceeds limit', () => {
        const firstMetadatKey = `${userMetadataKey}header0`;
        // add one more byte to be over the limit
        metadata[firstMetadatKey] = `${metadata[firstMetadatKey]}${'0'}`;
        const responseMetadata = checkUserMetadataSize(metadata);
        const invalidHeader
            = responseMetadata[invalidObjectUserMetadataHeader];
        const responseMetadataKeys = Object.keys(responseMetadata)
            .filter(key => key.startsWith(userMetadataKey));
        assert.notEqual(responseMetadata, undefined);
        assert.strictEqual(responseMetadataKeys.length > 0, false);
        assert.strictEqual(invalidHeader, userMetadataKeys);
    });
});
