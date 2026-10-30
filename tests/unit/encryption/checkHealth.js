const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('@scality/arsenal');

const Cache = require('../../../lib/kms/Cache');
const { DummyRequestLogger, kmsTestEntries, parseKmsEntries } = require('../helpers');
const memBackend = require('../../../lib/kms/in_memory/backend');
const kms = require('../../../lib/kms/wrapper');
const logger = require('../../../lib/utilities/logger');

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
                scality: { code: 200, message: 'OK' },
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
                scality: { code: 200, message: 'OK', lastChecked: expectedLastChecked },
            });

            assert(shouldRefreshStub.calledOnce, 'shouldRefresh should be called once');

            assert(
                setResultSpy.calledOnceWithExactly({
                    code: 200,
                    message: 'OK',
                }),
            );

            done();
        });
    });

    it('should return failure message when healthcheck fails', done => {
        memBackend.backend.healthcheck = sinon.stub().callsFake((log, cb) => cb(errors.InternalError));

        kms.checkHealth(log, (err, result) => {
            assert.ifError(err);

            const expectedLastChecked = new Date(clock.now).toISOString();

            assert.deepStrictEqual(result, {
                scality: {
                    code: 500,
                    message: 'KMS health check failed',
                    description: 'We encountered an internal error. Please try again.',
                    lastChecked: expectedLastChecked,
                },
            });

            assert(shouldRefreshStub.calledOnce, 'shouldRefresh should be called once');

            assert(
                setResultSpy.calledOnceWithExactly({
                    code: 500,
                    message: 'KMS health check failed',
                    description: 'We encountered an internal error. Please try again.',
                }),
            );

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
                    scality: {
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

describe('KMS health of multiple KMSs', () => {
    const { kmip: kmipEntry, otherKmip: otherKmipEntry, awsKms: awsEntry, file: fileEntry } = kmsTestEntries;
    let clock;
    let warnStub;

    beforeEach(() => {
        clock = sinon.useFakeTimers({
            now: 1625077800000,
            toFake: ['Date'],
        });
        sinon.stub(logger, 'info');
        warnStub = sinon.stub(logger, 'warn');
    });

    afterEach(() => {
        sinon.restore();
        clock.restore();
    });

    function createInstances(entries) {
        const { clientInstances } = kms.createClients(parseKmsEntries(entries));
        return clientInstances;
    }

    function checkKmsHealthByProvider(instances) {
        return new Promise(resolve =>
            kms.checkKmsHealthByProvider(instances, log, (err, result) => {
                assert.ifError(err);
                resolve(result);
            }),
        );
    }

    it('should report one entry per KMS, keyed by provider name', async () => {
        const instances = createInstances([kmipEntry, awsEntry, fileEntry]);
        const kmipHealthcheck = sinon
            .stub(instances['external:kmip:thales'].client, 'healthcheck')
            .callsFake((log, cb) => cb(null));

        const result = await checkKmsHealthByProvider(Object.values(instances));

        // AWS KMS and file have no health check, they report OK without being checked
        assert.deepStrictEqual(result, {
            thales: { code: 200, message: 'OK', lastChecked: new Date(clock.now).toISOString() },
            acme: { code: 200, message: 'OK' },
            scality: { code: 200, message: 'OK' },
        });
        assert(kmipHealthcheck.calledOnce);
    });

    it('should report a failing KMS next to healthy ones', async () => {
        const instances = createInstances([kmipEntry, awsEntry]);
        sinon
            .stub(instances['external:kmip:thales'].client, 'healthcheck')
            .callsFake((log, cb) => cb(errors.InternalError));

        const result = await checkKmsHealthByProvider(Object.values(instances));

        assert.deepStrictEqual(result, {
            thales: {
                code: 500,
                message: 'KMS health check failed',
                description: 'We encountered an internal error. Please try again.',
                lastChecked: new Date(clock.now).toISOString(),
            },
            acme: { code: 200, message: 'OK' },
        });
        assert(warnStub.calledOnceWith('KMS health check failed', sinon.match({ provider: 'thales', errorCode: 500 })));
    });

    it('should keep a health check cache per KMS', async () => {
        const instances = createInstances([kmipEntry, otherKmipEntry]);
        const thales = instances['external:kmip:thales'];
        const hytrust = instances['external:kmip:hytrust'];
        const thalesHealthcheck = sinon.stub(thales.client, 'healthcheck').callsFake((log, cb) => cb(null));
        const hytrustHealthcheck = sinon
            .stub(hytrust.client, 'healthcheck')
            .callsFake((log, cb) => cb(errors.InternalError));

        await checkKmsHealthByProvider([thales]);
        const result = await checkKmsHealthByProvider([thales, hytrust]);

        // thales comes from its cache, hytrust has no cached result yet
        assert(thalesHealthcheck.calledOnce);
        assert(hytrustHealthcheck.calledOnce);
        assert.strictEqual(result.thales.code, 200);
        assert.strictEqual(result.hytrust.code, 500);
    });
});
