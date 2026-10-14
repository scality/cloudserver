const assert = require('assert');

const {
    normalizeStorageClass,
    formatStorageClass,
    isColdStorageClass,
    validateStorageClass,
} = require('../../../../lib/api/apiUtils/object/storageClass');
const { config } = require('../../../../lib/Config');

const coldLocation = 'location-dmf-v1';
const hotLocation = 'us-east-1';
const archiveLocations = ['glacier', 'deep_archive'];

describe('storage class helpers', () => {
    let originalEnableDirectToCold;

    beforeEach(() => {
        originalEnableDirectToCold = config.enableDirectToCold;
    });

    afterEach(() => {
        config.enableDirectToCold = originalEnableDirectToCold;
    });

    describe('normalizeStorageClass', () => {
        describe('with cold locations named after the AWS archive storage classes', () => {
            beforeEach(() => {
                archiveLocations.forEach(location => {
                    config.locationConstraints[location] = { ...config.locationConstraints[coldLocation] };
                });
            });

            afterEach(() => {
                archiveLocations.forEach(location => {
                    delete config.locationConstraints[location];
                });
            });

            [
                ['GLACIER', 'glacier'],
                ['DEEP_ARCHIVE', 'deep_archive'],
            ].forEach(([awsName, location]) => {
                it(`should map ${awsName} to the ${location} location`, () => {
                    assert.strictEqual(normalizeStorageClass(awsName), location);
                });
            });

            ['STANDARD', 'GLACIER_IR', 'Glacier', coldLocation, 'Some-Location'].forEach(storageClass => {
                it(`should keep ${storageClass} as is`, () => {
                    assert.strictEqual(normalizeStorageClass(storageClass), storageClass);
                });
            });
        });

        describe('without a cold glacier location', () => {
            afterEach(() => {
                delete config.locationConstraints.glacier;
            });

            it('should not map GLACIER when there is no glacier location', () => {
                assert.strictEqual(normalizeStorageClass('GLACIER'), 'GLACIER');
            });

            it('should not map GLACIER when the glacier location is not cold', () => {
                config.locationConstraints.glacier = { ...config.locationConstraints[hotLocation] };
                assert.strictEqual(normalizeStorageClass('GLACIER'), 'GLACIER');
            });
        });
    });

    describe('formatStorageClass', () => {
        describe('with cold locations named after the AWS archive storage classes', () => {
            beforeEach(() => {
                archiveLocations.forEach(location => {
                    config.locationConstraints[location] = { ...config.locationConstraints[coldLocation] };
                });
            });

            afterEach(() => {
                archiveLocations.forEach(location => {
                    delete config.locationConstraints[location];
                });
            });

            [
                ['glacier', 'GLACIER'],
                ['deep_archive', 'DEEP_ARCHIVE'],
            ].forEach(([location, awsName]) => {
                it(`should report the ${location} location as ${awsName}`, () => {
                    assert.strictEqual(formatStorageClass(location), awsName);
                });
            });

            ['STANDARD', 'GLACIER', coldLocation, 'Some-Location'].forEach(storageClass => {
                it(`should keep ${storageClass} as is`, () => {
                    assert.strictEqual(formatStorageClass(storageClass), storageClass);
                });
            });

            it('should keep an undefined storage class', () => {
                assert.strictEqual(formatStorageClass(undefined), undefined);
            });
        });

        describe('without a cold glacier location', () => {
            afterEach(() => {
                delete config.locationConstraints.glacier;
            });

            it('should not report glacier as GLACIER when there is no glacier location', () => {
                assert.strictEqual(formatStorageClass('glacier'), 'glacier');
            });

            it('should not report glacier as GLACIER when the glacier location is not cold', () => {
                config.locationConstraints.glacier = { ...config.locationConstraints[hotLocation] };
                assert.strictEqual(formatStorageClass('glacier'), 'glacier');
            });
        });
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

        describe('with a location named after an AWS archive storage class', () => {
            beforeEach(() => {
                config.locationConstraints.glacier = { ...config.locationConstraints[coldLocation] };
            });

            afterEach(() => {
                delete config.locationConstraints.glacier;
            });

            it('should rewrite the header to the location name', () => {
                config.enableDirectToCold = true;
                const headers = { 'x-amz-storage-class': 'GLACIER' };
                assert.strictEqual(validateStorageClass(headers), null);
                assert.strictEqual(headers['x-amz-storage-class'], 'glacier');
            });

            it('should reject the AWS storage class when the option is disabled', () => {
                config.enableDirectToCold = false;
                const err = validateStorageClass({ 'x-amz-storage-class': 'GLACIER' });
                assert.strictEqual(err.message, 'InvalidStorageClass');
            });

            it('should reject the AWS storage class on a restore', () => {
                config.enableDirectToCold = true;
                const headers = { 'x-amz-storage-class': 'GLACIER', 'x-scal-s3-version-id': '' };
                const err = validateStorageClass(headers);
                assert.strictEqual(err.message, 'InvalidStorageClass');
            });

            it('should not rewrite a regular storage class', () => {
                const headers = { 'x-amz-storage-class': 'STANDARD' };
                assert.strictEqual(validateStorageClass(headers), null);
                assert.strictEqual(headers['x-amz-storage-class'], 'STANDARD');
            });

            it('should not rewrite a location name', () => {
                config.enableDirectToCold = true;
                const headers = { 'x-amz-storage-class': coldLocation };
                assert.strictEqual(validateStorageClass(headers), null);
                assert.strictEqual(headers['x-amz-storage-class'], coldLocation);
            });
        });
    });
});
