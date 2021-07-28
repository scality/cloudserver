const assert = require('assert');
const moment = require('moment');
const { errors } = require('arsenal');
const BucketInfo = require('arsenal').models.BucketInfo;
const { DummyRequestLogger } = require('../../helpers');
const {
    calculateRetainUntilDate,
    validateHeaders,
    validateObjectLockUpdate,
} = require('../../../../lib/api/apiUtils/object/objectLockHelpers');

const mockName = 'testbucket';
const mockOwner = 'someCanonicalId';
const mockOwnerDisplayName = 'accountDisplayName';
const mockCreationDate = new Date().toJSON();

const bucketInfo = new BucketInfo(
    mockName, mockOwner, mockOwnerDisplayName, mockCreationDate,
    null, null, null, null, null, null, null, null, null, null, null,
    null, null, true);

const objLockDisabledBucketInfo = new BucketInfo(
    mockName, mockOwner, mockOwnerDisplayName, mockCreationDate,
    null, null, null, null, null, null, null, null, null, null, null,
    null, null, false);

const log = new DummyRequestLogger();

describe('objectLockHelpers: validateHeaders', () => {
    it('should fail if object lock is not enabled on the bucket', () => {
        const headers = {
            'x-amz-object-lock-retain-until-date': '2050-10-12',
            'x-amz-object-lock-mode': 'COMPLIANCE',
        };
        const objectLockValidationError
            = validateHeaders(objLockDisabledBucketInfo, headers, log);
        const expectedError = errors.InvalidRequest.customizeDescription(
            'Bucket is missing ObjectLockConfiguration');
        assert.strictEqual(objectLockValidationError.InvalidRequest, true);
        assert.strictEqual(objectLockValidationError.description,
            expectedError.description);
    });

    it('should pass with valid retention headers', () => {
        const headers = {
            'x-amz-object-lock-retain-until-date': '2050-10-12',
            'x-amz-object-lock-mode': 'COMPLIANCE',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        assert.strictEqual(objectLockValidationError, null);
    });

    it('should pass with valid legal hold header', () => {
        const headers = {
            'x-amz-object-lock-legal-hold': 'ON',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        assert.strictEqual(objectLockValidationError, null);
    });

    it('should pass with valid legal hold header', () => {
        const headers = {
            'x-amz-object-lock-legal-hold': 'OFF',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        assert.strictEqual(objectLockValidationError, null);
    });

    it('should pass with both legal hold and retention headers', () => {
        const headers = {
            'x-amz-object-lock-retain-until-date': '2050-10-12',
            'x-amz-object-lock-mode': 'GOVERNANCE',
            'x-amz-object-lock-legal-hold': 'ON',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        assert.strictEqual(objectLockValidationError, null);
    });

    it('should fail with missing object-lock-mode header', () => {
        const headers = {
            'x-amz-object-lock-retain-until-date': '2005-10-12',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        const expectedError = errors.InvalidArgument.customizeDescription(
            'x-amz-object-lock-retain-until-date and x-amz-object-lock-mode ' +
            'must both be supplied');
        assert.strictEqual(objectLockValidationError.InvalidArgument, true);
        assert.strictEqual(objectLockValidationError.description,
            expectedError.description);
    });

    it('should fail with missing object-lock-retain-until-date header', () => {
        const headers = {
            'x-amz-object-lock-mode': 'GOVERNANCE',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        const expectedError = errors.InvalidArgument.customizeDescription(
            'x-amz-object-lock-retain-until-date and x-amz-object-lock-mode ' +
            'must both be supplied');
        assert.strictEqual(objectLockValidationError.InvalidArgument, true);
        assert.strictEqual(objectLockValidationError.description,
            expectedError.description);
    });

    it('should fail with past retention date header', () => {
        const headers = {
            'x-amz-object-lock-retain-until-date': '2005-10-12',
            'x-amz-object-lock-mode': 'COMPLIANCE',
        };
        const expectedError = errors.InvalidArgument.customizeDescription(
            'The retain until date must be in the future!');
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        assert.strictEqual(objectLockValidationError.InvalidArgument, true);
        assert.strictEqual(objectLockValidationError.description,
            expectedError.description);
    });

    it('should fail with invalid legal hold header', () => {
        const headers = {
            'x-amz-object-lock-legal-hold': 'on',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        const expectedError = errors.InvalidArgument.customizeDescription(
            'Legal hold status must be one of "ON", "OFF"');
        assert.strictEqual(objectLockValidationError.InvalidArgument, true);
        assert.strictEqual(objectLockValidationError.description,
            expectedError.description);
    });

    it('should fail with invalid retention period header', () => {
        const headers = {
            'x-amz-object-lock-retain-until-date': '2050-10-12',
            'x-amz-object-lock-mode': 'Governance',
        };
        const objectLockValidationError
            = validateHeaders(bucketInfo, headers, log);
        const expectedError = errors.InvalidArgument.customizeDescription(
            'Unknown wormMode directive');
        assert.strictEqual(objectLockValidationError.InvalidArgument, true);
        assert.strictEqual(objectLockValidationError.description,
            expectedError.description);
    });
});

describe('objectLockHelpers: calculateRetainUntilDate', () => {
    it('should calculate retainUntilDate for config with days', () => {
        const mockConfigWithDays = {
            mode: 'GOVERNANCE',
            days: 90,
        };
        const date = moment();
        const expectedRetainUntilDate
            = date.add(mockConfigWithDays.days, 'days');
        const retainUntilDate = calculateRetainUntilDate(mockConfigWithDays);
        assert.strictEqual(retainUntilDate.slice(0, 16),
            expectedRetainUntilDate.toISOString().slice(0, 16));
    });

    it('should calculate retainUntilDate for config with years', () => {
        const mockConfigWithYears = {
            mode: 'GOVERNANCE',
            years: 3,
        };
        const date = moment();
        const expectedRetainUntilDate
            = date.add(mockConfigWithYears.years * 365, 'days');
        const retainUntilDate = calculateRetainUntilDate(mockConfigWithYears);
        assert.strictEqual(retainUntilDate.slice(0, 16),
            expectedRetainUntilDate.toISOString().slice(0, 16));
    });
});

describe('objectLockHelpers: validateObjectLockUpdate', () => {
    it('should allow GOVERNANCE => COMPLIANCE if bypassGovernanceRetention is true', () => {
        const objMD = {
            retentionMode: 'GOVERNANCE',
            retentionDate: moment().add(1, 'days').toISOString(),
        };

        const retentionInfo = {
            mode: 'COMPLIANCE',
            date: moment().add(1, 'days').toISOString(),
        };

        const error = validateObjectLockUpdate(objMD, retentionInfo, true);
        assert.strictEqual(error, null);
    });

    it('should disallow GOVERNANCE => COMPLIANCE if bypassGovernanceRetention is false', () => {
        const objMD = {
            retentionMode: 'GOVERNANCE',
            retentionDate: moment().add(1, 'days').toISOString(),
        };

        const retentionInfo = {
            mode: 'COMPLIANCE',
            date: moment().add(1, 'days').toISOString(),
        };

        const error = validateObjectLockUpdate(objMD, retentionInfo, false);
        assert.deepStrictEqual(error, errors.AccessDenied);
    });

    it('should disallow COMPLIANCE => GOVERNANCE if retention is not expired', () => {
        const objMD = {
            retentionMode: 'COMPLIANCE',
            retentionDate: moment().add(1, 'days').toISOString(),
        };

        const retentionInfo = {
            mode: 'GOVERNANCE',
            date: moment().add(1, 'days').toISOString(),
        };

        const error = validateObjectLockUpdate(objMD, retentionInfo);
        assert.deepStrictEqual(error, errors.AccessDenied);
    });

    it('should allow COMPLIANCE => GOVERNANCE if retention is expired', () => {
        const objMD = {
            retentionMode: 'COMPLIANCE',
            retentionDate: moment().subtract(1, 'days').toISOString(),
        };

        const retentionInfo = {
            mode: 'GOVERNANCE',
            date: moment().add(1, 'days').toISOString(),
        };

        const error = validateObjectLockUpdate(objMD, retentionInfo);
        assert.strictEqual(error, null);
    });

    it('should allow extending retention period if in COMPLIANCE', () => {
        const objMD = {
            retentionMode: 'COMPLIANCE',
            retentionDate: moment().add(1, 'days').toISOString(),
        };

        const retentionInfo = {
            mode: 'COMPLIANCE',
            date: moment().add(2, 'days').toISOString(),
        };

        const error = validateObjectLockUpdate(objMD, retentionInfo);
        assert.strictEqual(error, null);
    });

    it('should disallow shortening retention period if in COMPLIANCE', () => {
        const objMD = {
            retentionMode: 'COMPLIANCE',
            retentionDate: moment().add(2, 'days').toISOString(),
        };

        const retentionInfo = {
            mode: 'COMPLIANCE',
            date: moment().add(1, 'days').toISOString(),
        };

        const error = validateObjectLockUpdate(objMD, retentionInfo);
        assert.deepStrictEqual(error, errors.AccessDenied);
    });

    it('should allow shortening retention period if in GOVERNANCE', () => {
        const objMD = {
            retentionMode: 'GOVERNANCE',
            retentionDate: moment().add(2, 'days').toISOString(),
        };

        const retentionInfo = {
            mode: 'GOVERNANCE',
            date: moment().add(1, 'days').toISOString(),
        };

        const error = validateObjectLockUpdate(objMD, retentionInfo, true);
        assert.strictEqual(error, null);
    });
});
