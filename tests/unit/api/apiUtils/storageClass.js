const assert = require('assert');

const { isColdStorageClass, validateStorageClass } = require('../../../../lib/api/apiUtils/object/storageClass');
const { config } = require('../../../../lib/Config');

const coldLocation = 'location-dmf-v1';
const hotLocation = 'us-east-1';

describe('storage class helpers', () => {
    let originalEnableDirectToCold;

    beforeEach(() => {
        originalEnableDirectToCold = config.enableDirectToCold;
    });

    afterEach(() => {
        config.enableDirectToCold = originalEnableDirectToCold;
    });

    describe('isColdStorageClass', () => {
        it('should return true for a cold location', () => {
            assert.strictEqual(isColdStorageClass(coldLocation), true);
        });

        it('should not return true for a hot location', () => {
            assert.ok(!isColdStorageClass(hotLocation));
        });

        it('should not return true for a regular storage class', () => {
            assert.ok(!isColdStorageClass('STANDARD'));
        });

        it('should not return true for an unknown location', () => {
            assert.ok(!isColdStorageClass('does-not-exist'));
        });

        it('should not return true when no storage class is given', () => {
            assert.ok(!isColdStorageClass(undefined));
        });
    });

    describe('validateStorageClass', () => {
        it('should accept a request without a storage class header', () => {
            assert.strictEqual(validateStorageClass({}), null);
        });

        it('should accept a regular storage class', () => {
            assert.strictEqual(validateStorageClass({ 'x-amz-storage-class': 'STANDARD' }), null);
        });

        it('should reject a cold location when the option is disabled', () => {
            config.enableDirectToCold = false;
            const err = validateStorageClass({ 'x-amz-storage-class': coldLocation });
            assert.strictEqual(err.message, 'InvalidStorageClass');
        });

        it('should accept a cold location when the option is enabled', () => {
            config.enableDirectToCold = true;
            assert.strictEqual(validateStorageClass({ 'x-amz-storage-class': coldLocation }), null);
        });

        it('should reject a hot location even when the option is enabled', () => {
            config.enableDirectToCold = true;
            const err = validateStorageClass({ 'x-amz-storage-class': hotLocation });
            assert.strictEqual(err.message, 'InvalidStorageClass');
        });

        it('should reject an unknown storage class', () => {
            config.enableDirectToCold = true;
            const err = validateStorageClass({ 'x-amz-storage-class': 'GLACIER' });
            assert.strictEqual(err.message, 'InvalidStorageClass');
        });

        it('should reject a cold location on a restore', () => {
            config.enableDirectToCold = true;
            const err = validateStorageClass({
                'x-amz-storage-class': coldLocation,
                'x-scal-s3-version-id': 'some-version-id',
            });
            assert.strictEqual(err.message, 'InvalidStorageClass');
        });

        it('should reject a cold location on a restore of a non-versioned object', () => {
            config.enableDirectToCold = true;
            const err = validateStorageClass({
                'x-amz-storage-class': coldLocation,
                'x-scal-s3-version-id': '',
            });
            assert.strictEqual(err.message, 'InvalidStorageClass');
        });

        it('should accept a regular storage class on a restore', () => {
            config.enableDirectToCold = true;
            const headers = { 'x-amz-storage-class': 'STANDARD', 'x-scal-s3-version-id': '' };
            assert.strictEqual(validateStorageClass(headers), null);
        });
    });
});
