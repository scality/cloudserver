const assert = require('assert');
const {
    parseAttributesHeaders,
    buildAttributesXml
} = require('../../../../../lib/api/apiUtils/object/objectAttributes');

const headerName = 'x-amz-object-attributes';
const allowedAttributes = new Set(['ETag', 'StorageClass', 'ObjectSize']);

describe('parseAttributesHeaders', () => {
    it('should throw InvalidArgument when attribute is invalid', () => {
        const headers = { [headerName]: 'InvalidAttribute' };

        assert.throws(
            () => parseAttributesHeaders(headers, headerName, allowedAttributes),
            err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                return true;
            },
        );
    });

    it('should return empty array when header is missing', () => {
        const result = parseAttributesHeaders({}, headerName, allowedAttributes);

        assert.deepStrictEqual(result, new Set([]));
    });

    it('should parse valid attributes', () => {
        const headers = { [headerName]: 'ETag,ObjectSize,x-amz-meta-custom,x-amz-meta-*' };
        const result = parseAttributesHeaders(headers, headerName, allowedAttributes);

        assert.deepStrictEqual(result, new Set(['ETag', 'ObjectSize', 'x-amz-meta-custom', 'x-amz-meta-*']));
    });

    it('should lowercase attributes not in allowedAttributes', () => {
        const headers = { [headerName]: 'ETag,X-AMZ-META-CUSTOM' };
        const result = parseAttributesHeaders(headers, headerName, allowedAttributes);

        assert.deepStrictEqual(result, new Set(['ETag', 'x-amz-meta-custom']));
    });

    it('should trim whitespace around attribute names', () => {
        const headers = { [headerName]: '  ETag  ,  ObjectSize  ' };
        const result = parseAttributesHeaders(headers, headerName, allowedAttributes);

        assert.deepStrictEqual(result, new Set(['ETag', 'ObjectSize']));
    });
});


describe('buildXmlAttributes', () => {
    const objectMD = {
        'content-md5': '16e37e19194511993498801d4692795f',
        'content-length': 5000,
        'x-amz-storage-class': 'STANDARD',
        'restoreStatus': {
            inProgress: false,
            expiryDate: 'Fri, 20 Feb 2026 12:00:00 GMT'
        }
    };

    const userMetadata = {
        'x-amz-meta-foo': 'foo',
        'x-amz-meta-bar': 'bar',
    };

    describe('with object attributes', () => {
        it('should generate empty XML when attributes is empty', () => {
            const result = [];
            buildAttributesXml(objectMD, userMetadata, [], result);

            assert.strictEqual(result.length, 0);
        });

        it('should generate ETag XML', () => {
            const result = [];
            buildAttributesXml(objectMD, userMetadata, ['ETag'], result);

            assert.strictEqual(result.length, 1);
            assert.strictEqual(result[0], '<ETag>16e37e19194511993498801d4692795f</ETag>');
        });

        it('should generate StorageClass XML', () => {
            const result = [];
            buildAttributesXml(objectMD, userMetadata, ['StorageClass'], result);

            assert.strictEqual(result.length, 1);
            assert.strictEqual(result[0], '<StorageClass>STANDARD</StorageClass>');
        });

        it('should generate ObjectSize XML', () => {
            const result = [];
            buildAttributesXml(objectMD, userMetadata, ['ObjectSize'], result);

            assert.strictEqual(result.length, 1);
            assert.strictEqual(result[0], '<ObjectSize>5000</ObjectSize>');
        });

        it('should generate ObjectParts XML when parts exist', () => {
            const result = [];
            const objectMDWithParts = { ...objectMD, 'content-md5': `${objectMD['content-md5']}-10` };
            buildAttributesXml(objectMDWithParts, {}, ['ObjectParts'], result);

            assert.strictEqual(result.length, 3);
            assert.strictEqual(result[0], '<ObjectParts>');
            assert.strictEqual(result[1], '<PartsCount>10</PartsCount>');
            assert.strictEqual(result[2], '</ObjectParts>');
        });

        it('should generate RestoreStatus XML with expiry date', () => {
            const result = [];
            buildAttributesXml(objectMD, userMetadata, ['RestoreStatus'], result);

            assert.strictEqual(result.length, 4);
            assert.strictEqual(result[0], '<RestoreStatus>');
            assert.strictEqual(result[1], '<IsRestoreInProgress>false</IsRestoreInProgress>');
            assert.strictEqual(result[2], '<RestoreExpiryDate>Fri, 20 Feb 2026 12:00:00 GMT</RestoreExpiryDate>');
            assert.strictEqual(result[3], '</RestoreStatus>');
        });

        it('should ignore unknown attributes', () => {
            const result = [];
            buildAttributesXml(objectMD, userMetadata, ['UnknownAttribute', 'ETag'], result);

            assert.strictEqual(result.length, 1);
            assert.strictEqual(result[0], '<ETag>16e37e19194511993498801d4692795f</ETag>');
        });
    });

    describe('with user metadata', () => {
        it('should include all user metadata when x-amz-meta-* is used', () => {
            const result = [];
            buildAttributesXml({}, userMetadata, ['x-amz-meta-*'], result);

            assert.strictEqual(result.length, 2);
            assert.strictEqual(result[0], '<x-amz-meta-foo>foo</x-amz-meta-foo>');
            assert.strictEqual(result[1], '<x-amz-meta-bar>bar</x-amz-meta-bar>');
        });

        it('should include specific user metadata keys', () => {
            const result = [];
            buildAttributesXml({}, userMetadata, ['x-amz-meta-foo'], result);

            assert.strictEqual(result.length, 1);
            assert.strictEqual(result[0], '<x-amz-meta-foo>foo</x-amz-meta-foo>');
        });

        it('should de-duplicate keys when both specific key and wildcard are requested', () => {
            const result = [];
            buildAttributesXml({}, userMetadata, ['x-amz-meta-foo', 'x-amz-meta-*'], result);

            assert.strictEqual(result.length, 2);
            assert.strictEqual(result[0], '<x-amz-meta-foo>foo</x-amz-meta-foo>');
            assert.strictEqual(result[1], '<x-amz-meta-bar>bar</x-amz-meta-bar>');
        });
    });

    describe('with object attributes and user metadata', () => {
        it('should build a comprehensive XML array with all supported features', () => {
            const result = [];
            const objectMDWithParts = { ...objectMD, 'content-md5': `${objectMD['content-md5']}-10` };
            const requested = ['ETag', 'ObjectSize', 'ObjectParts', 'RestoreStatus', 'x-amz-meta-*', 'x-amz-meta-foo'];
            buildAttributesXml(objectMDWithParts, userMetadata, requested, result);

            const expected = [
                '<ETag>16e37e19194511993498801d4692795f-10</ETag>',
                '<ObjectSize>5000</ObjectSize>',
                '<ObjectParts>',
                '<PartsCount>10</PartsCount>',
                '</ObjectParts>',
                '<RestoreStatus>',
                '<IsRestoreInProgress>false</IsRestoreInProgress>',
                '<RestoreExpiryDate>Fri, 20 Feb 2026 12:00:00 GMT</RestoreExpiryDate>',
                '</RestoreStatus>',
                '<x-amz-meta-foo>foo</x-amz-meta-foo>',
                '<x-amz-meta-bar>bar</x-amz-meta-bar>',
            ];

            assert.strictEqual(result.length, 11);
            for (const [i, elem] of expected.entries()) {
                assert.strictEqual(result[i], elem);
            }
        });
    });
});
