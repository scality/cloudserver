const assert = require('assert');
const { errors } = require('arsenal');
const sinon = require('sinon');
const inMemory = require('../../../lib/kms/in_memory/backend').backend;
const vault = require('../../../lib/auth/vault');

const { checkLocationConstraint, _handleAuthResults } = require('../../../lib/api/bucketPut');
const { bucketPut } = require('../../../lib/api/bucketPut');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const metadata = require('../metadataswitch');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const originalLCs = Object.assign({}, config.locationConstraints);

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const namespace = 'default';
const splitter = constants.splitter;
const usersBucket = constants.usersBucket;
const bucketName = 'bucketname';
const testRequest = {
    bucketName,
    namespace,
    url: '/',
    post: '',
    headers: { host: `${bucketName}.s3.amazonaws.com` },
};

const testChecks = [
    {
        data: 'scality-internal-file',
        locationSent: 'scality-internal-file',
        parsedHost: '127.1.2.3',
        locationReturn: 'scality-internal-file',
        isError: false,
    },
    {
        data: 'scality-internal-file',
        locationSent: 'wronglocation',
        parsedHost: '127.1.0.0',
        locationReturn: undefined,
        isError: true,
    },
    {
        data: 'scality-internal-file',
        locationSent: '',
        parsedHost: '127.0.0.1',
        locationReturn: config.restEndpoints['127.0.0.1'],
        isError: false,
    },
    {
        data: 'scality-internal-file',
        locationSent: '',
        parsedHost: '127.3.2.1',
        locationReturn: 'us-east-1',
        isError: false,
    },
    {
        data: 'multiple',
        locationSent: '',
        parsedHost: '127.3.2.1',
        locationReturn: 'us-east-1',
        isError: false,
    },
];

describe('checkLocationConstraint function', () => {
    const request = {};
    const initialConfigData = config.backends.data;
    afterEach(() => {
        config.backends.data = initialConfigData;
    });
    testChecks.forEach(testCheck => {
        const returnText = testCheck.isError ? 'InvalidLocationConstraint error'
        : 'the appropriate location constraint';
        it(`with data backend: "${testCheck.data}", ` +
        `location: "${testCheck.locationSent}",` +
        ` and host: "${testCheck.parsedHost}", should return ${returnText} `,
        done => {
            config.backends.data = testCheck.data;
            request.parsedHost = testCheck.parsedHost;
            const checkLocation = checkLocationConstraint(request,
              testCheck.locationSent, log);
            if (testCheck.isError) {
                assert.notEqual(checkLocation.error, null,
                  'Expected failure but got success');
                assert.strictEqual(checkLocation.error.is.InvalidLocationConstraint, true);
            } else {
                assert.ifError(checkLocation.error);
                assert.strictEqual(checkLocation.locationConstraint,
                  testCheck.locationReturn);
            }
            done();
        });
    });
});

describe('bucketPut API', () => {
    beforeEach(() => {
        cleanup();
    });

    it('should return an error if bucket already exists', done => {
        const otherAuthInfo = makeAuthInfo('accessKey2');
        bucketPut(authInfo, testRequest, log, () => {
            bucketPut(otherAuthInfo, testRequest,
                log, err => {
                    assert.deepStrictEqual(err, errors.BucketAlreadyExists);
                    done();
                });
        });
    });

    it('should create a bucket', done => {
        bucketPut(authInfo, testRequest, log, err => {
            if (err) {
                return done(new Error(err));
            }
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(md.getName(), bucketName);
                assert.strictEqual(md.getOwner(), canonicalID);
                const prefix = `${canonicalID}${splitter}`;
                metadata.listObject(usersBucket, { prefix },
                    log, (err, listResponse) => {
                        assert.strictEqual(listResponse.Contents[0].key,
                            `${canonicalID}${splitter}${bucketName}`);
                        done();
                    });
            });
        });
    });

    const createTestRequestWithLock = status => ({
        bucketName,
        namespace,
        url: '/',
        post: '',
        headers: {
            'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-bucket-object-lock-enabled': `${status}`,
        },
    });

    const validObjLockVals = ['True', 'true', 'False', 'false'];

    validObjLockVals.forEach(val => {
        it('when valid object lock enabled header passed in', done => {
            const params = createTestRequestWithLock(val);
            const expectedVal = ['True', 'true'].includes(val);
            bucketPut(authInfo, params, log, err => {
                if (err) {
                    return done(new Error(err));
                }
                return metadata.getBucket(bucketName, log, (err, md) => {
                    assert.ifError(err);
                    assert.strictEqual(md.isObjectLockEnabled(), expectedVal);
                    done();
                });
            });
        });
    });

    it('without object lock if no header passed in', done => {
        bucketPut(authInfo, testRequest, log, err => {
            if (err) {
                return done(new Error(err));
            }
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.ifError(err);
                assert.strictEqual(md.isObjectLockEnabled(), false);
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid group URI', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid canned ACL', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'not-valid-option',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid email address', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-read':
                    'emailaddress="fake@faking.com"',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, log, err => {
            assert.deepStrictEqual(err, errors.UnresolvableGrantByEmailAddress);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    it('should set a canned ACL while creating bucket' +
        ' if option set out in header', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl':
                    'public-read',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, log, err => {
            assert.strictEqual(err, null);
            metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(err, null);
                assert.strictEqual(md.getAcl().Canned, 'public-read');
                done();
            });
        });
    });

    it('should set specific ACL grants while creating bucket' +
        ' if options set out in header', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.logId}`,
                'x-amz-grant-write': `uri=${constants.publicId}`,
                'x-amz-grant-read-acp':
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be',
                'x-amz-grant-write-acp':
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf',
            },
            url: '/',
            post: '',
        };
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';
        bucketPut(authInfo, testRequest, log, err => {
            assert.strictEqual(err, null, 'Error creating bucket');
            metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(md.getAcl().READ[0], constants.logId);
                assert.strictEqual(md.getAcl().WRITE[0], constants.publicId);
                assert(md.getAcl()
                       .FULL_CONTROL.indexOf(canonicalIDforSample1) > -1);
                assert(md.getAcl()
                       .FULL_CONTROL.indexOf(canonicalIDforSample2) > -1);
                assert(md.getAcl()
                       .READ_ACP.indexOf(canonicalIDforSample1) > -1);
                assert(md.getAcl()
                       .WRITE_ACP.indexOf(canonicalIDforSample2) > -1);
                done();
            });
        });
    });

    it('should prevent anonymous user from accessing putBucket API', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        bucketPut(publicAuthInfo, testRequest, log, err => {
            assert.deepStrictEqual(err, errors.AccessDenied);
        });
        done();
    });

    it('should pick up updated rest endpoint config', done => {
        const bucketName = 'new-loc-bucket-name';
        const newRestEndpoint = 'newly.defined.rest.endpoint';
        const newLocation = 'scality-us-west-1';

        const req = Object.assign({}, testRequest, {
            parsedHost: newRestEndpoint,
            bucketName,
        });

        const newRestEndpoints = Object.assign({}, config.restEndpoints);
        newRestEndpoints[newRestEndpoint] = newLocation;
        config.setRestEndpoints(newRestEndpoints);

        bucketPut(authInfo, req, log, err => {
            assert.deepStrictEqual(err, null);
            metadata.getBucket(bucketName, log, (err, bucketInfo) => {
                assert.deepStrictEqual(err, null);
                assert.deepStrictEqual(newLocation,
                    bucketInfo.getLocationConstraint());
                done();
            });
        });
    });

    describe('Config::setLocationConstraints', () => {
        const bucketName = `test-bucket-${Date.now()}`;
        const newLC = {};
        const newLCKey = `test_location_constraint_${Date.now()}`;
        newLC[newLCKey] = {
            type: 'aws_s3',
            legacyAwsBehavior: true,
            details: {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: `test-detail-bucket-${Date.now()}`,
                bucketMatch: true,
                credentialsProfile: 'default',
            },
        };
        const newLCs = Object.assign({}, config.locationConstraints, newLC);
        const req = Object.assign({}, testRequest, {
            bucketName,
            post: '<?xml version="1.0" encoding="UTF-8"?>' +
                '<CreateBucketConfiguration ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
                    `<LocationConstraint>${newLCKey}</LocationConstraint>` +
                '</CreateBucketConfiguration>',
        });

        afterEach(() => config.setLocationConstraints(originalLCs));

        it('should return error if location constraint config is not updated',
            done => bucketPut(authInfo, req, log, err => {
                const expectedError = errors.InvalidLocationConstraint;
                expectedError.description = 'value of the location you are ' +
                    `attempting to set - ${newLCKey} - is not listed in the ` +
                    'locationConstraint config';
                assert.deepStrictEqual(err, expectedError);
                done();
            }));

        it('should accept updated location constraint config', done => {
            config.setLocationConstraints(newLCs);
            bucketPut(authInfo, req, log, err => {
                assert.strictEqual(err, null);
                done();
            });
        });
    });

    describe('_handleAuthResults handles', () => {
        const constraint = 'location-constraint';
        [
            {
                description: 'errors',
                error: 'our error',
                results: undefined,
                calledWith: ['our error'],
            },
            {
                description: 'single allowed auth',
                error: undefined,
                results: [{ isAllowed: true }],
                calledWith: [null, constraint],
            },
            {
                description: 'many allowed auth',
                error: undefined,
                results: [
                    { isAllowed: true },
                    { isAllowed: true },
                    { isAllowed: true },
                    { isAllowed: true },
                ],
                calledWith: [null, constraint],
            },
            {
                description: 'array of arrays allowed auth',
                error: undefined,
                results: [
                    { isAllowed: true },
                    { isAllowed: true },
                    [{ isAllowed: true }, { isAllowed: true }],
                    { isAllowed: true },
                ],
                calledWith: [null, constraint],
            },
            {
                description: 'array of arrays not allowed auth',
                error: undefined,
                results: [
                    { isAllowed: true },
                    { isAllowed: true },
                    [{ isAllowed: true }, { isAllowed: false }],
                    { isAllowed: true },
                ],
                calledWith: [errors.AccessDenied],
            },
            {
                description: 'single not allowed auth',
                error: undefined,
                results: [{ isAllowed: false }],
                calledWith: [errors.AccessDenied],
            },
            {
                description: 'one not allowed auth of many',
                error: undefined,
                results: [
                    { isAllowed: true },
                    { isAllowed: true },
                    { isAllowed: false },
                    { isAllowed: true },
                ],
                calledWith: [errors.AccessDenied],
            },
        ].forEach(tc => it(tc.description, () => {
            const cb = sinon.fake();
            const handler = _handleAuthResults(constraint, log, cb);
            handler(tc.error, tc.results);
            assert.deepStrictEqual(cb.getCalls()[0].args, tc.calledWith);
        }));
    });
});

describe('bucketPut API with bucket-level encryption', () => {
    let createBucketKeySpy;

    beforeEach(() => {
        createBucketKeySpy = sinon.spy(inMemory, 'createBucketKey');
    });

    afterEach(() => {
        cleanup();
        sinon.restore();
    });

    it('should create a bucket with AES256 algorithm', done => {
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'AES256',
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(createBucketKeySpy);
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.ifError(err);
                const serverSideEncryption = md.getServerSideEncryption();
                assert.strictEqual(serverSideEncryption.algorithm, 'AES256');
                assert.strictEqual(serverSideEncryption.mandatory, true);
                assert(serverSideEncryption.masterKeyId);
                assert(!serverSideEncryption.isAccountEncryptionEnabled);
                done();
            });
        });
    });

    it('should create a bucket with aws:kms algorithm', done => {
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'aws:kms',
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(createBucketKeySpy);
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.ifError(err);
                const serverSideEncryption = md.getServerSideEncryption();
                assert.strictEqual(serverSideEncryption.algorithm, 'aws:kms');
                assert.strictEqual(serverSideEncryption.mandatory, true);
                assert(serverSideEncryption.masterKeyId);
                assert(!serverSideEncryption.isAccountEncryptionEnabled);
                done();
            });
        });
    });

    it('should create a bucket with aws:kms algorithm and configured key id', done => {
        const keyId = '12345';
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'aws:kms',
                'x-amz-scal-server-side-encryption-aws-kms-key-id': keyId,
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert.ifError(err);
            sinon.assert.notCalled(createBucketKeySpy);
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.ifError(err);
                assert.deepStrictEqual(md.getServerSideEncryption(), {
                    cryptoScheme: 1,
                    algorithm: 'aws:kms',
                    mandatory: true,
                    configuredMasterKeyId: keyId,
                });
                done();
            });
        });
    });

    // TODO: Currently, the operation does not fail when both the AES256 algorithm
    // and a KMS key ID are specified. Modify the behavior to ensure that bucket
    // creation fails in this case.
    it.skip('should fail creating a bucket with AES256 algorithm and configured key id', done => {
        const keyId = '12345';
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'AES256',
                'x-amz-scal-server-side-encryption-aws-kms-key-id': keyId,
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert(err);
            done();
        });
    });
});

describe('bucketPut API with account level encryption', () => {
    let getOrCreateEncryptionKeyIdSpy;
    const accountLevelMasterKeyId = 'account-level-master-encryption-key';

    beforeEach(() => {
        sinon.stub(inMemory, 'supportsDefaultKeyPerAccount').value(true);
        getOrCreateEncryptionKeyIdSpy = sinon.spy(vault, 'getOrCreateEncryptionKeyId');
    });

    afterEach(() => {
        cleanup();
        sinon.restore();
    });

    it('should create a bucket with AES256 algorithm', done => {
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'AES256',
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(getOrCreateEncryptionKeyIdSpy);
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.ifError(err);
                assert.deepStrictEqual(md.getServerSideEncryption(), {
                    cryptoScheme: 1,
                    algorithm: 'AES256',
                    mandatory: true,
                    masterKeyId: accountLevelMasterKeyId,
                    isAccountEncryptionEnabled: true,
                });
                done();
            });
        });
    });

    it('should create a bucket with aws:kms algorithm', done => {
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'aws:kms',
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(getOrCreateEncryptionKeyIdSpy);
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.ifError(err);
                assert.deepStrictEqual(md.getServerSideEncryption(), {
                    cryptoScheme: 1,
                    algorithm: 'aws:kms',
                    mandatory: true,
                    masterKeyId: accountLevelMasterKeyId,
                    isAccountEncryptionEnabled: true,
                });
                done();
            });
        });
    });

    it('should create a bucket with aws:kms algorithm and configured key id', done => {
        const keyId = '12345';
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'aws:kms',
                'x-amz-scal-server-side-encryption-aws-kms-key-id': keyId,
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert.ifError(err);
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.ifError(err);
                sinon.assert.notCalled(getOrCreateEncryptionKeyIdSpy);
                assert.deepStrictEqual(md.getServerSideEncryption(), {
                    cryptoScheme: 1,
                    algorithm: 'aws:kms',
                    mandatory: true,
                    configuredMasterKeyId: keyId,
                });
                done();
            });
        });
    });
});

describe('bucketPut API with failed encryption service', () => {
    beforeEach(() => {
        sinon.stub(inMemory, 'createBucketKey').callsFake((bucketName, log, cb) => cb(errors.InternalError));
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should fail creating bucket', done => {
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'AES256',
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert(err && err.InternalError);
            done();
        });
    });
});

describe('bucketPut API with failed vault service', () => {
    beforeEach(() => {
        sinon.stub(inMemory, 'supportsDefaultKeyPerAccount').value(true);
        sinon.stub(vault, 'getOrCreateEncryptionKeyId').callsFake((accountCanonicalId, log, cb) =>
            cb(errors.ServiceFailure));
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should fail putting bucket encryption', done => {
        const testRequestWithEncryption = {
            ...testRequest,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-scal-server-side-encryption': 'AES256',
            },
        };
        bucketPut(authInfo, testRequestWithEncryption, log, err => {
            assert(err && err.ServiceFailure);
            done();
        });
    });
});

