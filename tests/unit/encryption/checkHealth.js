const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('arsenal');

const Cache = require('../../../lib/kms/Cache');
const { DummyRequestLogger } = require('../helpers');
const memBackend = require('../../../lib/kms/in_memory/backend');
const kms = require('../../../lib/kms/wrapper');

const log = new DummyRequestLogger();

describe('KMS.checkHealth', () => {
    let setResultSpy;
    let shouldRefreshStub;
    let clock;

    beforeEach(() => {
        clock = sinon.useFakeTimers({
            now: 1625077800000,
            toFake: ['Date'],
        });

        setResultSpy = sinon.spy(Cache.prototype, 'setResult');
        shouldRefreshStub = sinon.stub(Cache.prototype, 'shouldRefresh').returns(true);

        delete memBackend.backend.healthcheck;
    });

    afterEach(() => {
        sinon.restore();
        if (clock) {
            clock.restore();
        }
    });

    it('should return OK when kms backend does not have healthcheck method', done => {
        kms.checkHealth(log, (err, result) => {
            assert.ifError(err);
            assert.deepStrictEqual(result, {
                memoryKms: { code: 200, message: 'OK' },
            });

            assert(shouldRefreshStub.notCalled, 'shouldRefresh should not be called');
            assert(setResultSpy.notCalled, 'setResult should not be called');

            done();
        });
    });

    it('should return OK when healthcheck succeeds', done => {
        memBackend.backend.healthcheck = sinon.stub().callsFake((log, cb) => cb(null));

        kms.checkHealth(log, (err, result) => {
            assert.ifError(err);

            const expectedLastChecked = new Date(clock.now).toISOString();

            assert.deepStrictEqual(result, {
                memoryKms: { code: 200, message: 'OK', lastChecked: expectedLastChecked },
            });

            assert(shouldRefreshStub.calledOnce, 'shouldRefresh should be called once');

            assert(setResultSpy.calledOnceWithExactly({
                code: 200,
                message: 'OK',
            }));

            done();
        });
    });

    it('should return failure message when healthcheck fails', done => {
        memBackend.backend.healthcheck = sinon.stub().callsFake((log, cb) => cb(errors.InternalError));

        kms.checkHealth(log, (err, result) => {
            assert.ifError(err);

            const expectedLastChecked = new Date(clock.now).toISOString();

            assert.deepStrictEqual(result, {
                memoryKms: {
                    code: 500,
                    message: 'KMS health check failed',
                    description: 'We encountered an internal error. Please try again.',
                    lastChecked: expectedLastChecked,
                },
            });

            assert(shouldRefreshStub.calledOnce, 'shouldRefresh should be called once');

            assert(setResultSpy.calledOnceWithExactly({
                code: 500,
                message: 'KMS health check failed',
                description: 'We encountered an internal error. Please try again.',
            }));

            done();
        });
    });

    it('should use cached result when not refreshing', done => {
        memBackend.backend.healthcheck = sinon.stub().callsFake((log, cb) => cb(null));
        // first call to populate the cache
        kms.checkHealth(log, err => {
            assert.ifError(err);
            shouldRefreshStub.returns(false);

            // second call should use the cached result
            kms.checkHealth(log, (err, result) => {
                assert.ifError(err);

                const expectedLastChecked = new Date(clock.now).toISOString();
                assert.deepStrictEqual(result, {
                    memoryKms: {
                        code: 200,
                        message: 'OK',
                        lastChecked: expectedLastChecked,
                    },
                });

                // once each call
                assert.strictEqual(shouldRefreshStub.callCount, 2, 'shouldRefresh should be called twice');

                // only the first call
                assert.strictEqual(setResultSpy.callCount, 1, 'setResult should be called once');

                done();
            });
        });
    });
});
