const assert = require('assert');
const { versioning } = require('@scality/arsenal');

const parseCopySource = require('../../../../../lib/api/apiUtils/object/parseCopySource');

const { VersionID } = versioning;

describe('parseCopySource', () => {
    const encodedVersionId = VersionID.encode(VersionID.generateVersionId('0', ''));

    it('should return the source bucket and key', () => {
        const result = parseCopySource('objectCopy', '/srcbucket/some/key');
        assert.deepStrictEqual(result, {
            sourceBucket: 'srcbucket',
            sourceObject: 'some/key',
            sourceVersionId: undefined,
        });
    });

    it('should return the version ID as sent', () => {
        const result = parseCopySource('objectPutCopyPart', `srcbucket/key?versionId=${encodedVersionId}`);
        assert.strictEqual(result.sourceVersionId, encodedVersionId);
    });

    it('should return the null version ID as sent', () => {
        const result = parseCopySource('objectCopy', 'srcbucket/key?versionId=null');
        assert.strictEqual(result.sourceVersionId, 'null');
    });

    it('should accept an invalid version ID', () => {
        const result = parseCopySource('objectCopy', 'srcbucket/key?versionId=invalid');
        assert.strictEqual(result.parsingError, undefined);
        assert.strictEqual(result.sourceVersionId, 'invalid');
    });

    it('should return a parsing error for a repeated version ID', () => {
        // From 33 values on, the version ID decoder does not reject them
        const query = Array.from({ length: 33 }, (_, i) => `versionId=${10 + i}`).join('&');
        const result = parseCopySource('objectCopy', `srcbucket/key?${query}`);
        assert(result.parsingError, 'expected a parsing error');
        assert.strictEqual(result.parsingError.is.InvalidArgument, true);
    });
});
