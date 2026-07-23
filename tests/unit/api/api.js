const sinon = require('sinon');
const { errors, auth } = require('arsenal');
const api = require('../../../lib/api/api');
const rateLimitCache = require('../../../lib/api/apiUtils/rateLimit/cache');
const DummyRequest = require('../DummyRequest');
const { default: AuthInfo } = require('arsenal/build/lib/auth/AuthInfo');
const assert = require('assert');
const crypto = require('crypto');

describe('api.callApiMethod', () => {
    let sandbox;
    let request;
    let response;
    let log;
    let authServer;

    beforeEach(() => {
        sandbox = sinon.createSandbox();

        request = new DummyRequest('my-obj');
        request.query = {};
        request.socket = new ReadableStream();
        request.socket.remoteAddress = '127.0.0.1';
        request.socket.destroy = sandbox.stub();

        response = {
            write: sandbox.stub(),
            end: sandbox.stub(),
        };

        log = {
            addDefaultFields: sandbox.stub(),
            trace: sandbox.stub(),
            error: sandbox.stub(),
            debug: sandbox.stub(),
        };

        authServer = {
            doAuth: sandbox.stub().callsArgWith(
                2,
                null,
                new AuthInfo({}),
                [
                    {
                        isAllowed: true,
                        isImplicit: false,
                    },
                ],
                null,
                {
                    accountQuota: 5000,
                },
            ),
        };

        sandbox.stub(auth, 'server').value(authServer);
    });

    afterEach(() => {
        sandbox.restore();
    });

    it('should attach apiMethod to request', done => {
        const testMethod = 'bucketGet';
        api.callApiMethod(testMethod, request, response, log, () => {
            if (request.apiMethod !== testMethod) {
                return done(new Error('apiMethod not attached to request'));
            }
            return done();
        });
    });

    it('should initialize finalizerHooks array', done => {
        api.callApiMethod('bucketGet', request, response, log, () => {
            assert.strictEqual(Array.isArray(request.finalizerHooks), true);
            assert.strictEqual(request.finalizerHooks.length, 0);
            done();
        });
    });

    it('should handle auth server errors', done => {
        authServer.doAuth.callsArgWith(2, errors.AccessDenied);

        api.callApiMethod('bucketGet', request, response, log, err => {
            assert(err.is.AccessDenied);
            done();
        });
    });

    it('should execute finalizer hooks after api method completion', done => {
        let called = false;

        sandbox.stub(api, 'objectPut').callsFake((userInfo, _request, streamingV4Params, log, cb) => {
            request.finalizerHooks.push((err, _done) => {
                called = true;
                _done();
            });
            cb();
        });
        request.objectKey = 'testobject';
        api.callApiMethod('objectPut', request, response, log, () => {
            assert.strictEqual(called, true);
            done();
        });
    });

    it('should set _needQuota to true for completeMultipartUpload', done => {
        authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts) => {
            assert.strictEqual(requestContexts[0]._needQuota, true);
            done();
        });
        sandbox.stub(api, 'completeMultipartUpload').callsFake((userInfo, _request, streamingV4Params, log, cb) => cb);
        api.callApiMethod('completeMultipartUpload', request, response, log);
    });

    it('should set _needQuota to true for multipartDelete', done => {
        authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts) => {
            assert.strictEqual(requestContexts[0]._needQuota, true);
            done();
        });
        sandbox.stub(api, 'multipartDelete').callsFake((userInfo, _request, streamingV4Params, log, cb) => cb);
        api.callApiMethod('multipartDelete', request, response, log);
    });

    ['objectPut', 'objectPutPart'].forEach(method => {
        it(`should set startTurnAroundTime on request end for ${method}`, done => {
            sandbox.stub(api, method).callsFake((userInfo, _request, streamingV4Params, _log, cb) => {
                request.on('end', () => {
                    assert.strictEqual(typeof request.serverAccessLog.startTurnAroundTime, 'bigint');
                    cb();
                });
                request.resume();
            });
            request.objectKey = 'testobject';
            request.serverAccessLog = {};
            api.callApiMethod(method, request, response, log, err => {
                assert.ifError(err);
                done();
            });
        });

        it(`should set startTurnAroundTime synchronously for 0-byte ${method}`, done => {
            sandbox.stub(api, method).callsFake((userInfo, _request, streamingV4Params, _log, cb) => {
                assert.strictEqual(typeof request.serverAccessLog.startTurnAroundTime, 'bigint');
                cb();
            });
            request.objectKey = 'testobject';
            request.serverAccessLog = {};
            request.headers = Object.assign({}, request.headers, { 'content-length': '0' });
            api.callApiMethod(method, request, response, log, err => {
                assert.ifError(err);
                done();
            });
        });
    });

    describe('cross-account rate limiting target account', () => {
        afterEach(() => {
            rateLimitCache.bucketOwnerCache.clear();
        });

        it('should pass the cached bucket owner to doAuth as targetAccount', done => {
            request.bucketName = 'rl-bucket';
            rateLimitCache.setCachedBucketOwner('rl-bucket', 'owner-canonical-id', 30000);
            // Reset the default callsArgWith behavior so only the fake runs
            authServer.doAuth.resetBehavior();
            authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts, authOptions) => {
                assert.strictEqual(authOptions.targetAccount, 'owner-canonical-id');
                assert.strictEqual(request.rateLimitTargetAccount, 'owner-canonical-id');
                done();
            });
            api.callApiMethod('bucketGet', request, response, log);
        });

        it('should not set targetAccount when no bucket owner is cached', done => {
            request.bucketName = 'rl-bucket';
            authServer.doAuth.resetBehavior();
            authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts, authOptions) => {
                assert.deepStrictEqual(authOptions, {});
                assert.strictEqual(request.rateLimitTargetAccount, undefined);
                done();
            });
            api.callApiMethod('bucketGet', request, response, log);
        });

        it('should not set targetAccount when the request has no bucket name', done => {
            rateLimitCache.setCachedBucketOwner('rl-bucket', 'owner-canonical-id', 30000);
            authServer.doAuth.resetBehavior();
            authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts, authOptions) => {
                assert.deepStrictEqual(authOptions, {});
                assert.strictEqual(request.rateLimitTargetAccount, undefined);
                done();
            });
            api.callApiMethod('bucketGet', request, response, log);
        });
    });

    describe('MD5 checksum validation', () => {
        const methodsWithChecksumValidation = [
            'bucketPutACL',
            'bucketPutCors',
            'bucketPutEncryption',
            'bucketPutLifecycle',
            'bucketPutNotification',
            'bucketPutObjectLock',
            'bucketPutPolicy',
            'bucketPutReplication',
            'bucketPutVersioning',
            'bucketPutWebsite',
            'multiObjectDelete',
            'objectPutACL',
            'objectPutLegalHold',
            'objectPutTagging',
            'objectPutRetention',
        ];

        methodsWithChecksumValidation.forEach(method => {
            it(`should return BadDigest for ${method} when bad MD5 checksum is provided`, done => {
                const body = '<xml></xml>';
                const headers = {
                    'content-md5': '1B2M2Y8AsgTpgAmY7PhCfg==', // Wrong MD5
                    'content-length': body.length.toString()
                };

                const requestWithBody = new DummyRequest(
                    {
                        headers,
                        query: {},
                        socket: { remoteAddress: '127.0.0.1', destroy: sandbox.stub() },
                    },
                    body,
                );

                sandbox.stub(api, method).callsFake(() => {
                    done(new Error(`${method} was called despite bad checksum`));
                });

                api.callApiMethod(method, requestWithBody, response, log, err => {
                    assert(err, `Expected error for ${method} with bad checksum`);
                    assert(err.is.BadDigest, `Expected BadDigest error for ${method}, got: ${err.code}`);
                    done();
                });
            });
        });

        methodsWithChecksumValidation.forEach(method => {
            it(`should return InvalidDigest for ${method} when invalid MD5 checksum is provided`, done => {
                const body = '<xml></xml>';
                const headers = {
                    'content-md5': 'x', // Invalid MD5
                    'content-length': body.length.toString()
                };

                const requestWithBody = new DummyRequest({
                    headers,
                    query: {},
                    socket: { remoteAddress: '127.0.0.1', destroy: sandbox.stub() }
                }, body);

                sandbox.stub(api, method).callsFake(() => {
                    done(new Error(`${method} was called despite bad checksum`));
                });

                api.callApiMethod(method, requestWithBody, response, log, err => {
                    assert(err, `Expected error for ${method} with bad checksum`);
                    assert(err.is.InvalidDigest, `Expected InvalidDigest error for ${method}, got: ${err.code}`);
                    done();
                });
            });
        });

        methodsWithChecksumValidation.forEach(method => {
            it(`should succeed for ${method} when correct MD5 checksum is provided`, done => {
                const body = '<xml></xml>';
                const correctMd5 = crypto.createHash('md5').update(body).digest('base64');
                const headers = {
                    'content-md5': correctMd5,
                    'content-length': body.length.toString(),
                };

                const requestWithBody = new DummyRequest(
                    {
                        headers,
                        query: {},
                        socket: { remoteAddress: '127.0.0.1', destroy: sandbox.stub() },
                    },
                    body,
                );

                sandbox.stub(api, method).callsFake((userInfo, _request, log, cb) => {
                    cb();
                });

                api.callApiMethod(method, requestWithBody, response, log, err => {
                    assert.ifError(err, `Unexpected error for ${method} with good checksum: ${err}`);
                    done();
                });
            });
        });

        methodsWithChecksumValidation.forEach(method => {
            it(`should fail for ${method} when empty MD5 checksum is provided`, done => {
                const body = '<xml></xml>';
                const headers = {
                    'content-md5': '',
                    'content-length': body.length.toString(),
                };

                const requestWithBody = new DummyRequest(
                    {
                        headers,
                        query: {},
                        socket: { remoteAddress: '127.0.0.1', destroy: sandbox.stub() },
                    },
                    body,
                );

                sandbox.stub(api, method).callsFake((userInfo, _request, log, cb) => {
                    cb();
                });

                api.callApiMethod(method, requestWithBody, response, log, err => {
                    assert(err, `expected error for ${method} with no checksum`);
                    done();
                });
            });
        });
    });
});
