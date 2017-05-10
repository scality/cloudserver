const assert = require('assert');
const { errors } = require('arsenal');

const { checkLocationConstraint } = require('../../../lib/api/bucketPut');
const { bucketPut } = require('../../../lib/api/bucketPut');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const metadata = require('../metadataswitch');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

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
        data: 'file',
        locationSent: 'file',
        parsedHost: '127.1.2.3',
        locationReturn: 'file',
        isError: false,
    },
    {
        data: 'file',
        locationSent: 'wronglocation',
        parsedHost: '127.1.0.0',
        locationReturn: undefined,
        isError: true,
    },
    {
        data: 'file',
        locationSent: '',
        parsedHost: '127.0.0.1',
        locationReturn: config.restEndpoints['127.0.0.1'],
        isError: false,
    },
    {
        data: 'file',
        locationSent: '',
        parsedHost: '127.3.2.1',
        locationReturn: '',
        isError: false,
    },
    {
        data: 'multiple',
        locationSent: '',
        parsedHost: '127.3.2.1',
        locationReturn: undefined,
        isError: true,
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
                assert.strictEqual(checkLocation.error.
                  InvalidLocationConstraint, true);
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
});
