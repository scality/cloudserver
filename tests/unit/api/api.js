const sinon = require('sinon');
const { errors, auth } = require('@scality/arsenal');
const api = require('../../../lib/api/api');
const constants = require('../../../constants');
const { config } = require('../../../lib/Config');
const rateLimitCache = require('../../../lib/api/apiUtils/rateLimit/cache');
const recordStore = require('../../../lib/api/apiUtils/notificationDestination/recordStore');
const DummyRequest = require('../DummyRequest');
const { default: AuthInfo } = require('@scality/arsenal/build/lib/auth/AuthInfo');
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
        beforeEach(() => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                serviceUserArn: 'arn:aws:iam::000000000000:user/rate-limit-service-user',
                nodes: 1,
                tokenBucketBufferSize: 50,
                tokenBucketRefillThreshold: 20,
                error: errors.SlowDown,
                bucket: {
                    configCacheTTL: 30000,
                    defaultConfig: { RequestsPerSecond: { BurstCapacity: 1 } },
                },
                account: {
                    configCacheTTL: 30000,
                    defaultConfig: { RequestsPerSecond: { BurstCapacity: 1 } },
                },
            });
        });

        afterEach(() => {
            rateLimitCache.bucketOwnerCache.clear();
            rateLimitCache.configCache.clear();
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

        it('should not set targetAccount when rate limiting is disabled', done => {
            sandbox.stub(config, 'rateLimiting').value({ enabled: false });
            request.bucketName = 'rl-bucket';
            rateLimitCache.setCachedBucketOwner('rl-bucket', 'owner-canonical-id', 30000);
            authServer.doAuth.resetBehavior();
            authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts, authOptions) => {
                assert.deepStrictEqual(authOptions, {});
                assert.strictEqual(request.rateLimitTargetAccount, undefined);
                done();
            });
            api.callApiMethod('bucketGet', request, response, log);
        });

        it('should set targetAccount from the cached owner even when the account config is cached', done => {
            request.bucketName = 'rl-bucket';
            rateLimitCache.setCachedBucketOwner('rl-bucket', 'owner-canonical-id', 30000);
            rateLimitCache.setCachedConfig(
                rateLimitCache.namespace.account,
                'owner-canonical-id',
                { RequestsPerSecond: { Limit: 100, BurstCapacity: 1, source: 'resource' } },
                30000,
            );
            authServer.doAuth.resetBehavior();
            authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts, authOptions) => {
                assert.strictEqual(authOptions.targetAccount, 'owner-canonical-id');
                assert.strictEqual(request.rateLimitAccountAlreadyChecked, true);
                done();
            });
            api.callApiMethod('bucketGet', request, response, log);
        });

        it('should store the limits returned by doAuth as the target account limits', done => {
            request.bucketName = 'rl-bucket';
            const limits = { RequestsPerSecond: { Limit: 250 } };
            authServer.doAuth.resetBehavior();
            authServer.doAuth.callsArgWith(2, null, new AuthInfo({}), [{ isAllowed: true, isImplicit: false }], null, {
                accountQuota: 5000,
                limits,
            });
            sandbox.stub(api, 'bucketGet').callsFake((userInfo, _request, log, cb) => cb());
            api.callApiMethod('bucketGet', request, response, log, err => {
                assert.ifError(err);
                assert.strictEqual(request.rateLimitTargetAccountLimits, limits);
                assert.strictEqual(request.accountLimits, undefined);
                done();
            });
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

    // Set the destination API flag for each test of the enclosing describe.
    // After each test, restore whatever was there before. When the flag is
    // on, the record store is ready.
    function useDestinationApiFlag(enabled) {
        let saved;
        beforeEach(() => {
            saved = config.notificationDestinationApi;
            config.notificationDestinationApi = { enabled, bucketd: null };
            if (enabled) {
                sandbox.stub(recordStore, 'state').returns('ready');
            }
        });
        afterEach(() => {
            if (saved === undefined) {
                delete config.notificationDestinationApi;
            } else {
                config.notificationDestinationApi = saved;
            }
        });
    }

    describe('implicit deny from Vault', () => {
        useDestinationApiFlag(true);

        // Resolve with the error that callApiMethod returns, so that a failed
        // assertion fails the test at once instead of in the API callback.
        function callApi(method) {
            return new Promise(resolve => api.callApiMethod(method, request, response, log, resolve));
        }

        function denyImplicitly() {
            authServer.doAuth.callsArgWith(
                2,
                null,
                new AuthInfo({}),
                [{ isAllowed: false, isImplicit: true }],
                null,
                {},
            );
        }

        [
            'notificationDestinationCreate',
            'notificationDestinationUpdate',
            'notificationDestinationGet',
            'notificationDestinationDelete',
            'notificationDestinationList',
        ].forEach(method => {
            describe(method, () => {
                let handler;
                let existingHandler;

                beforeEach(() => {
                    // Stub the handler for the test. After the test, restore
                    // whatever was there before.
                    existingHandler = api[method];
                    handler = sandbox.stub().callsFake((userInfo, _request, _log, cb) => cb());
                    api[method] = handler;
                    request.query = method === 'notificationDestinationList' ? {} : { name: 'my-destination' };
                });

                afterEach(() => {
                    if (existingHandler === undefined) {
                        delete api[method];
                    } else {
                        api[method] = existingHandler;
                    }
                });

                it('should return AccessDenied, since no bucket policy or ACL can allow it', async () => {
                    denyImplicitly();
                    const err = await callApi(method);
                    assert.strictEqual(err?.message, 'AccessDenied');
                    assert.strictEqual(handler.called, false);
                });

                it('should call the handler when Vault allows it', async () => {
                    const err = await callApi(method);
                    assert.ifError(err);
                    assert.strictEqual(handler.calledOnce, true);
                    assert.strictEqual(authServer.doAuth.firstCall.args[3], 's3');
                    assert.strictEqual(authServer.doAuth.firstCall.args[4][0].getAwsService(), 'bucketnotif');
                });
            });
        });

        ['bucketPut', 'serviceGet'].forEach(method => {
            it(`should still return AccessDenied for ${method}`, async () => {
                const handler = sandbox.stub(api, method).callsFake((userInfo, _request, _log, cb) => cb());
                denyImplicitly();
                const err = await callApi(method);
                assert.strictEqual(err?.message, 'AccessDenied');
                assert.strictEqual(handler.called, false);
            });
        });

        it('should still let an S3 bucket method reach its handler, for its bucket policy and ACL', async () => {
            const handler = sandbox.stub(api, 'bucketGet').callsFake((userInfo, _request, _log, cb) => cb());
            denyImplicitly();
            const err = await callApi('bucketGet');
            assert.ifError(err);
            assert.strictEqual(handler.calledOnce, true);
        });
    });

    describe('destination API flag', () => {
        function callApi(method) {
            return new Promise(resolve => api.callApiMethod(method, request, response, log, resolve));
        }

        const destinationMethods = [
            'notificationDestinationCreate',
            'notificationDestinationUpdate',
            'notificationDestinationGet',
            'notificationDestinationDelete',
            'notificationDestinationList',
        ];

        let stubbed;

        // Stub each handler for the test. After the test, restore whatever
        // was there before.
        beforeEach(() => {
            stubbed = {};
            destinationMethods.forEach(method => {
                stubbed[method] = { existing: api[method] };
                api[method] = sandbox.stub().callsFake((userInfo, _request, _log, cb) => cb());
            });
        });

        afterEach(() => {
            destinationMethods.forEach(method => {
                if (stubbed[method].existing === undefined) {
                    delete api[method];
                } else {
                    api[method] = stubbed[method].existing;
                }
            });
        });

        describe('when off', () => {
            useDestinationApiFlag(false);

            destinationMethods.forEach(method => {
                it(`should answer a plain NotImplemented to ${method} without asking Vault`, async () => {
                    request.query = method === 'notificationDestinationList' ? {} : { name: 'my-destination' };
                    const err = await callApi(method);
                    assert.strictEqual(err?.message, 'NotImplemented');
                    assert.strictEqual(err.description, errors.NotImplemented.description);
                    assert.strictEqual(authServer.doAuth.called, false);
                    assert.strictEqual(api[method].called, false);
                });
            });

            it('should let a bucket method through', async () => {
                const handler = sandbox.stub(api, 'bucketGet').callsFake((userInfo, _request, _log, cb) => cb());
                const err = await callApi('bucketGet');
                assert.ifError(err);
                assert.strictEqual(authServer.doAuth.calledOnce, true);
                assert.strictEqual(handler.calledOnce, true);
            });
        });

        describe('when on', () => {
            useDestinationApiFlag(true);

            destinationMethods.forEach(method => {
                it(`should send ${method} to Vault and its handler`, async () => {
                    request.query = method === 'notificationDestinationList' ? {} : { name: 'my-destination' };
                    const err = await callApi(method);
                    assert.ifError(err);
                    assert.strictEqual(authServer.doAuth.calledOnce, true);
                    assert.strictEqual(api[method].calledOnce, true);
                });

                it(`should answer AccessDenied to an anonymous ${method} before its handler`, async () => {
                    authServer.doAuth.callsArgWith(
                        2,
                        null,
                        new AuthInfo({ canonicalID: constants.publicId }),
                        // an anonymous request gets no authorization results
                        undefined,
                        null,
                        undefined,
                    );
                    request.query = method === 'notificationDestinationList' ? {} : { name: 'my-destination' };
                    const err = await callApi(method);
                    assert.strictEqual(err?.message, 'AccessDenied');
                    assert.strictEqual(authServer.doAuth.calledOnce, true);
                    assert.strictEqual(api[method].called, false);
                });
            });

            it('should let a bucket method through', async () => {
                const handler = sandbox.stub(api, 'bucketGet').callsFake((userInfo, _request, _log, cb) => cb());
                const err = await callApi('bucketGet');
                assert.ifError(err);
                assert.strictEqual(authServer.doAuth.calledOnce, true);
                assert.strictEqual(handler.calledOnce, true);
            });

            it('should let an anonymous bucket method through', async () => {
                authServer.doAuth.callsArgWith(
                    2,
                    null,
                    new AuthInfo({ canonicalID: constants.publicId }),
                    undefined,
                    null,
                    undefined,
                );
                const handler = sandbox.stub(api, 'bucketGet').callsFake((userInfo, _request, _log, cb) => cb());
                const err = await callApi('bucketGet');
                assert.ifError(err);
                assert.strictEqual(handler.calledOnce, true);
            });

            describe('while the record store has not read its marker', () => {
                beforeEach(() => {
                    recordStore.state.returns('starting');
                });

                destinationMethods.forEach(method => {
                    it(`should answer ServiceUnavailable to ${method} without asking Vault`, async () => {
                        request.query = method === 'notificationDestinationList' ? {} : { name: 'my-destination' };
                        const err = await callApi(method);
                        assert.strictEqual(err?.message, 'ServiceUnavailable');
                        assert.strictEqual(authServer.doAuth.called, false);
                        assert.strictEqual(api[method].called, false);
                    });
                });

                it('should let a bucket method through', async () => {
                    const handler = sandbox.stub(api, 'bucketGet').callsFake((userInfo, _request, _log, cb) => cb());
                    const err = await callApi('bucketGet');
                    assert.ifError(err);
                    assert.strictEqual(authServer.doAuth.calledOnce, true);
                    assert.strictEqual(handler.calledOnce, true);
                });
            });

            describe('when the record store marker holds an unknown format', () => {
                beforeEach(() => {
                    recordStore.state.returns('unknownFormat');
                });

                destinationMethods.forEach(method => {
                    it(`should answer NotImplemented to ${method} without asking Vault`, async () => {
                        request.query = method === 'notificationDestinationList' ? {} : { name: 'my-destination' };
                        const err = await callApi(method);
                        assert.strictEqual(err?.message, 'NotImplemented');
                        assert.strictEqual(authServer.doAuth.called, false);
                        assert.strictEqual(api[method].called, false);
                    });
                });

                it('should let a bucket method through', async () => {
                    const handler = sandbox.stub(api, 'bucketGet').callsFake((userInfo, _request, _log, cb) => cb());
                    const err = await callApi('bucketGet');
                    assert.ifError(err);
                    assert.strictEqual(authServer.doAuth.calledOnce, true);
                    assert.strictEqual(handler.calledOnce, true);
                });
            });
        });
    });
});
