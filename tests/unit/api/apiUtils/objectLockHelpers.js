const assert = require('assert');
const moment = require('moment');
const { errors } = require('arsenal');
const BucketInfo = require('arsenal').models.BucketInfo;
const { DummyRequestLogger } = require('../../helpers');
const {
    calculateRetainUntilDate,
    validateHeaders,
    compareObjectLockInformation,
    ObjectLockInfo,
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
        assert.strictEqual(objectLockValidationError.is.InvalidRequest, true);
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
        assert.strictEqual(objectLockValidationError.is.InvalidArgument, true);
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
        assert.strictEqual(objectLockValidationError.is.InvalidArgument, true);
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
        assert.strictEqual(objectLockValidationError.is.InvalidArgument, true);
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
        assert.strictEqual(objectLockValidationError.is.InvalidArgument, true);
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
        assert.strictEqual(objectLockValidationError.is.InvalidArgument, true);
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

describe('objectLockHelpers: compareObjectLockInformation', () => {
    const mockDate = new Date();
    let origNow = null;
    let someDateInFuture = null;

    before(() => {
        origNow = moment.now;
        moment.now = () => mockDate;
        someDateInFuture = moment().add(100, 'days').toISOString();
    });

    after(() => {
        moment.now = origNow;
        origNow = null;
    });

    it('should return empty object when both headers and default lock config are not set', () => {
        const headers = {
            'x-amz-object-lock-mode': '',
            'x-amz-object-lock-retain-until-date': '',
        };
        const defaultRetention = {};
        const res = compareObjectLockInformation(headers, defaultRetention);
        assert.deepStrictEqual(res, {});
    });

    it('should not use default retention if mode property is missing', () => {
        const headers = {};
        const defaultRetention = { rule: { days: 1 } };
        const res = compareObjectLockInformation(headers, defaultRetention);
        assert.deepStrictEqual(res, {});
    });

    it('should not use default retention if both days/years properties are missing', () => {
        const headers = {};
        const defaultRetention = { rule: { mode: 'GOVERNANCE' } };
        const res = compareObjectLockInformation(headers, defaultRetention);
        assert.deepStrictEqual(res, {});
    });

    it('should use default retention config (days)', () => {
        const headers = {
            'x-amz-object-lock-mode': '',
            'x-amz-object-lock-retain-until-date': '',
        };
        const defaultRetention = { rule: { mode: 'GOVERNANCE', days: 1 } };
        const res = compareObjectLockInformation(headers, defaultRetention);
        assert.deepStrictEqual(res, {
            retentionInfo: {
                mode: 'GOVERNANCE',
                date: moment().add(1, 'days').toISOString(),
            },
        });
    });

    it('should use default retention config (years)', () => {
        const headers = {
            'x-amz-object-lock-mode': '',
            'x-amz-object-lock-retain-until-date': '',
        };
        const defaultRetention = { rule: { mode: 'GOVERNANCE', years: 1 } };
        const res = compareObjectLockInformation(headers, defaultRetention);
        assert.deepStrictEqual(res, {
            retentionInfo: {
                mode: 'GOVERNANCE',
                date: moment().add(365, 'days').toISOString(),
            },
        });
    });

    it('should use header-defined lock config', () => {
        const headers = {
            'x-amz-object-lock-mode': 'COMPLIANCE',
            'x-amz-object-lock-retain-until-date': someDateInFuture,
        };
        const defaultRetention = { rule: { mode: 'GOVERNANCE', years: 1 } };
        const res = compareObjectLockInformation(headers, defaultRetention);
        assert.deepStrictEqual(res, {
            retentionInfo: { mode: 'COMPLIANCE', date: someDateInFuture },
        });
    });

    it('should use legal-hold config', () => {
        const headers = { 'x-amz-object-lock-legal-hold': 'ON' };
        const defaultRetention = {};
        const res = compareObjectLockInformation(headers, defaultRetention);
        assert.deepStrictEqual(res, { legalHold: true });
    });
});


const pastDate = moment().subtract(1, 'days');
const futureDate = moment().add(100, 'days');

const isLockedTestCases = [
    {
        desc: 'no mode and no date',
        policy: {},
        expected: false,
    },
    {
        desc: 'mode and no date',
        policy: {
            mode: 'GOVERNANCE',
        },
        expected: false,
    },
    {
        desc: 'mode and past date',
        policy: {
            mode: 'GOVERNANCE',
            date: pastDate.toISOString(),
        },
        expected: false,
    },
    {
        desc: 'mode and future date',
        policy: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        expected: true,
    },
];

const isExpiredTestCases = [
    {
        desc: 'should return true, no date is the same as expired',
        expected: true,
    },
    {
        desc: 'should return true, past date.',
        date: pastDate.toISOString(),
        expected: true,
    },
    {
        desc: 'should return false, future date.',
        date: futureDate.toISOString(),
        expected: false,
    },
];

const policyChangeTestCases = [
    {
        desc: 'enable governance policy',
        from: {},
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'modifying expired governance policy',
        from: {
            mode: 'GOVERNANCE',
            date: pastDate.toISOString(),
        },
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'extending governance policy',
        from: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.add(1, 'days').toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'shortening governance policy',
        from: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.subtract(1, 'days').toISOString(),
        },
        allowed: false,
        allowedWithBypass: true,
    },
    {
        desc: 'extending governance policy using same date',
        from: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'removing governance policy',
        from: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        to: {},
        allowed: false,
        allowedWithBypass: true,
    },
    {
        desc: 'changing governance policy to compliance',
        from: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        allowed: false,
        allowedWithBypass: true,
    },
    {
        desc: 'enable compliance policy',
        from: {},
        to: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'modifying expired compliance policy',
        from: {
            mode: 'COMPLIANCE',
            date: pastDate.toISOString(),
        },
        to: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'extending compliance policy',
        from: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'COMPLIANCE',
            date: futureDate.add(1, 'days').toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'shortening compliance policy',
        from: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'COMPLIANCE',
            date: futureDate.subtract(1, 'days').toISOString(),
        },
        allowed: false,
        allowedWithBypass: false,
    },
    {
        desc: 'extending compliance policy with the same date',
        from: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'removing compliance policy',
        from: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        to: {},
        allowed: false,
        allowedWithBypass: false,
    },
    {
        desc: 'changing compliance to governance policy',
        from: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: false,
        allowedWithBypass: false,
    },
    {
        desc: 'invalid starting mode',
        from: {
            mode: 'IM_AN_INVALID_MODE',
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: false,
        allowedWithBypass: false,
    },
    {
        desc: 'date with no mode',
        from: {
            date: futureDate.toISOString(),
        },
        to: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
];

const canModifyObjectTestCases = [
    {
        desc: 'No object lock config',
        policy: {},
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'active governance mode',
        policy: {
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: false,
        allowedWithBypass: true,
    },
    {
        desc: 'expired governance mode',
        policy: {
            mode: 'GOVERNANCE',
            date: pastDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'active compliance mode',
        policy: {
            mode: 'COMPLIANCE',
            date: futureDate.toISOString(),
        },
        allowed: false,
        allowedWithBypass: false,
    },
    {
        desc: 'expired compliance mode',
        policy: {
            mode: 'COMPLIANCE',
            date: pastDate.toISOString(),
        },
        allowed: true,
        allowedWithBypass: true,
    },
    {
        desc: 'invalid mode',
        policy: {
            mode: 'IM_AN_INVALID_MODE',
            date: futureDate.toISOString(),
        },
        allowed: false,
        allowedWithBypass: false,
    },
    {
        desc: 'legal hold enabled',
        policy: {
            legalHold: true,
        },
        allowed: false,
        allowedWithBypass: false,
    },
    {
        desc: 'legal hold enabled with governance mode',
        policy: {
            legalHold: true,
            mode: 'GOVERNANCE',
            date: futureDate.toISOString(),
        },
        allowed: false,
        allowedWithBypass: false,
    },
];

describe('objectLockHelpers: ObjectLockInfo', () => {
    ['GOVERNANCE', 'COMPLIANCE'].forEach(mode => {
        it(`should return ${mode === 'GOVERNANCE'} for isGovernance`, () => {
            const info = new ObjectLockInfo({
                mode,
            });
            assert.strictEqual(info.isGovernanceMode(), mode === 'GOVERNANCE');
        });

        it(`should return ${mode === 'COMPLIANCE'} for isCompliance`, () => {
            const info = new ObjectLockInfo({
                mode,
            });
            assert.strictEqual(info.isComplianceMode(), mode === 'COMPLIANCE');
        });
    });

    describe('isExpired: ', () => isExpiredTestCases.forEach(testCase => {
        const objLockInfo = new ObjectLockInfo({ date: testCase.date });
        it(testCase.desc, () => assert.strictEqual(objLockInfo.isExpired(), testCase.expected));
    }));

    describe('isLocked: ', () => isLockedTestCases.forEach(testCase => {
        describe(`${testCase.desc}`, () => {
            it(`should show policy as ${testCase.expected ? '' : 'not'} locked without legal hold`, () => {
                const objLockInfo = new ObjectLockInfo(testCase.policy);
                assert.strictEqual(objLockInfo.isLocked(), testCase.expected);
            });

            // legal hold should show as locked regardless of policy
            it('should show policy as locked with legal hold', () => {
                const policy = Object.assign({}, testCase.policy, { legalHold: true });
                const objLockInfo = new ObjectLockInfo(policy);
                assert.strictEqual(objLockInfo.isLocked(), true);
            });
        });
    }));

    describe('canModifyPolicy: ', () => policyChangeTestCases.forEach(testCase => {
        describe(testCase.desc, () => {
            const objLockInfo = new ObjectLockInfo(testCase.from);
            it(`should ${testCase.allowed ? 'allow' : 'deny'} modifying the policy without bypass`,
                () => assert.strictEqual(objLockInfo.canModifyPolicy(testCase.to), testCase.allowed));

            it(`should ${testCase.allowedWithBypass ? 'allow' : 'deny'} modifying the policy with bypass`,
                () => assert.strictEqual(objLockInfo.canModifyPolicy(testCase.to, true), testCase.allowedWithBypass));
        });
    }));

    describe('canModifyObject: ', () => canModifyObjectTestCases.forEach(testCase => {
        describe(testCase.desc, () => {
            const objLockInfo = new ObjectLockInfo(testCase.policy);
            it(`should ${testCase.allowed ? 'allow' : 'deny'} modifying object without bypass`,
                () => assert.strictEqual(objLockInfo.canModifyObject(), testCase.allowed));

            it(`should ${testCase.allowedWithBypass ? 'allow' : 'deny'} modifying object with bypass`,
                () => assert.strictEqual(objLockInfo.canModifyObject(true), testCase.allowedWithBypass));
        });
    }));
});
