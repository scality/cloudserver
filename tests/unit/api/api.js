const sinon = require('sinon');
const { errors, auth } = require('arsenal');
const api = require('../../../lib/api/api');
const DummyRequest = require('../DummyRequest');
const { default: AuthInfo } = require('arsenal/build/lib/auth/AuthInfo');
const assert = require('assert');

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
        request.socket = {
            remoteAddress: '127.0.0.1',
        };

        response = {
            write: sandbox.stub(),
            end: sandbox.stub()
        };

        log = {
            addDefaultFields: sandbox.stub(),
            trace: sandbox.stub(),
            error: sandbox.stub(),
            debug: sandbox.stub()
        };

        authServer = {
            doAuth: sandbox.stub().callsArgWith(2, null, new AuthInfo({}), [{
                isAllowed: true,
                isImplicit: false,
            }], null, {
                accountQuota: 5000,
            }),
        };

        sandbox.stub(auth, 'server').value(authServer);
    });

    afterEach(() => {
        sandbox.restore();
    });

    it('should attach apiMethod to request', done => {
        const testMethod = 'bucketGet';
        api.callApiMethod(testMethod, request, response, log, () => {
            assert.strictEqual(request.apiMethod, testMethod);
            done();
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
        sandbox.stub(api, 'completeMultipartUpload').callsFake(
            (userInfo, _request, streamingV4Params, log, cb) => cb);
        api.callApiMethod('completeMultipartUpload', request, response, log);
    });

    it('should set _needQuota to true for multipartDelete', done => {
        authServer.doAuth.callsFake((req, log, cb, awsService, requestContexts) => {
            assert.strictEqual(requestContexts[0]._needQuota, true);
            done();
        });
        sandbox.stub(api, 'multipartDelete').callsFake(
            (userInfo, _request, streamingV4Params, log, cb) => cb);
        api.callApiMethod('multipartDelete', request, response, log);
    });
});
