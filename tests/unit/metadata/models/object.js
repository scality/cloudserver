const assert = require('assert');
const ObjectMD = require('../../../../lib/metadata/models/object/ObjectMD');

describe('ObjectMD class setters/getters', () => {
    let md = null;

    beforeEach(() => {
        md = new ObjectMD();
    });

    [
        // In order: data property, value to set/get, default value
        ['ModelVersion', null, 2],
        ['OwnerDisplayName', null, ''],
        ['OwnerDisplayName', 'owner-display-name'],
        ['OwnerId', null, ''],
        ['OwnerId', 'owner-id'],
        ['CacheControl', null, ''],
        ['CacheControl', 'cache-control'],
        ['ContentDisposition', null, ''],
        ['ContentDisposition', 'content-disposition'],
        ['ContentEncoding', null, ''],
        ['ContentEncoding', 'content-encoding'],
        ['Expires', null, ''],
        ['Expires', 'expire-date'],
        ['ContentLength', null, 0],
        ['ContentLength', 15000],
        ['ContentType', null, ''],
        ['ContentType', 'content-type'],
        ['LastModified', new Date().toJSON()],
        ['ContentMd5', null, ''],
        ['ContentMd5', 'content-md5'],
        ['AmzVersionId', null, 'null'],
        ['AmzVersionId', 'version-id'],
        ['AmzServerVersionId', null, ''],
        ['AmzServerVersionId', 'server-version-id'],
        ['AmzStorageClass', null, 'STANDARD'],
        ['AmzStorageClass', 'storage-class'],
        ['AmzServerSideEncryption', null, ''],
        ['AmzServerSideEncryption', 'server-side-encryption'],
        ['AmzEncryptionKeyId', null, ''],
        ['AmzEncryptionKeyId', 'encryption-key-id'],
        ['AmzEncryptionCustomerAlgorithm', null, ''],
        ['AmzEncryptionCustomerAlgorithm', 'customer-algorithm'],
        ['Acl', null, {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        }],
        ['Acl', {
            Canned: 'public',
            FULL_CONTROL: ['id'],
            WRITE_ACP: ['id'],
            READ: ['id'],
            READ_ACP: ['id'],
        }],
        ['Key', null, ''],
        ['Key', 'key'],
        ['Location', null, []],
        ['Location', ['location1']],
        ['IsNull', null, ''],
        ['IsNull', true],
        ['NullVersionId', null, ''],
        ['NullVersionId', '111111'],
        ['IsDeleteMarker', null, ''],
        ['IsDeleteMarker', true],
        ['VersionId', null, undefined],
        ['VersionId', '111111'],
        ['Tags', null, {}],
        ['Tags', {
            key: 'value',
        }],
        ['Tags', null, {}],
        ['ReplicationInfo', null, {
            status: '',
            content: [],
            destination: '',
            storageClass: '',
        }],
        ['ReplicationInfo', {
            status: 'PENDING',
            content: ['DATA', 'METADATA'],
            destination: 'destination-bucket',
            storageClass: 'STANDARD',
        }],
    ].forEach(test => {
        const property = test[0];
        const testValue = test[1];
        const defaultValue = test[2];
        const testName = testValue === null ? 'get default' : 'get/set';
        it(`${testName}: ${property}`, () => {
            if (testValue !== null) {
                md[`set${property}`](testValue);
            }
            const value = md[`get${property}`]();
            if ((testValue !== null && typeof testValue === 'object') ||
                typeof defaultValue === 'object') {
                assert.deepStrictEqual(value, testValue || defaultValue);
            } else if (testValue !== null) {
                assert.strictEqual(value, testValue);
            } else {
                assert.strictEqual(value, defaultValue);
            }
        });
    });
});
