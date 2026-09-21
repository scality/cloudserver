const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('arsenal');

const { clientCheck } = require('../../../lib/utilities/healthcheckHandler');
const { DummyRequestLogger } = require('../helpers');
const { data } = require('../../../lib/data/wrapper');
const metadata = require('../../../lib/metadata/wrapper');
const vault = require('../../../lib/auth/vault');
const kms = require('../../../lib/kms/wrapper');

const log = new DummyRequestLogger();

describe('clientCheck - failure detection logic', () => {
    let dataStub;
    let metadataStub;
    let vaultStub;
    let kmsStub;

    beforeEach(() => {
        // Create stubs for all client checkHealth methods
        dataStub = sinon.stub(data, 'checkHealth');
        metadataStub = sinon.stub(metadata, 'checkHealth');
        vaultStub = sinon.stub(vault, 'checkHealth');
        kmsStub = sinon.stub(kms, 'checkHealth');
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should succeed when all backends are healthy', done => {
        dataStub.callsFake((log, cb) =>
            cb(null, {
                'sproxyd-loc1': { code: 200, message: 'OK' },
                'sproxyd-loc2': { code: 200, message: 'OK' },
            }),
        );
        metadataStub.callsFake((log, cb) =>
            cb(null, {
                metadata: { code: 200, message: 'OK' },
            }),
        );
        vaultStub.callsFake((log, cb) =>
            cb(null, {
                vault: { code: 200, message: 'OK' },
            }),
        );
        kmsStub.callsFake((log, cb) =>
            cb(null, {
                kms: { code: 200, message: 'OK' },
            }),
        );

        clientCheck(false, log, (err, result) => {
            assert.ifError(err);
            assert.deepStrictEqual(result, {
                'sproxyd-loc1': { code: 200, message: 'OK' },
                'sproxyd-loc2': { code: 200, message: 'OK' },
                metadata: { code: 200, message: 'OK' },
                vault: { code: 200, message: 'OK' },
                kms: { code: 200, message: 'OK' },
            });
            done();
        });
    });

    it('should fail when ALL backends of data client fail while metadata is healthy', done => {
        dataStub.callsFake((log, cb) =>
            cb(null, {
                'sproxyd-loc1': { error: errors.InternalError, code: 500 },
                'sproxyd-loc2': { error: errors.InternalError, code: 500 },
            }),
        );
        metadataStub.callsFake((log, cb) =>
            cb(null, {
                metadata: { code: 200, message: 'OK' },
            }),
        );
        vaultStub.callsFake((log, cb) =>
            cb(null, {
                vault: { code: 200, message: 'OK' },
            }),
        );
        kmsStub.callsFake((log, cb) =>
            cb(null, {
                kms: { code: 200, message: 'OK' },
            }),
        );

        clientCheck(false, log, (err, result) => {
            assert(err);
            assert.strictEqual(err.InternalError, true);
            assert.deepStrictEqual(result, {
                'sproxyd-loc1': { error: errors.InternalError, code: 500 },
                'sproxyd-loc2': { error: errors.InternalError, code: 500 },
                metadata: { code: 200, message: 'OK' },
                vault: { code: 200, message: 'OK' },
                kms: { code: 200, message: 'OK' },
            });
            done();
        });
    });

    it('should succeed when ONE data location fails but another is healthy', done => {
        dataStub.callsFake((log, cb) =>
            cb(null, {
                'sproxyd-loc1': { error: errors.InternalError, code: 500 },
                'sproxyd-loc2': { code: 200, message: 'OK' },
            }),
        );
        metadataStub.callsFake((log, cb) =>
            cb(null, {
                metadata: { code: 200, message: 'OK' },
            }),
        );
        vaultStub.callsFake((log, cb) =>
            cb(null, {
                vault: { code: 200, message: 'OK' },
            }),
        );
        kmsStub.callsFake((log, cb) =>
            cb(null, {
                kms: { code: 200, message: 'OK' },
            }),
        );

        clientCheck(false, log, (err, result) => {
            assert.ifError(err);
            assert.deepStrictEqual(result, {
                'sproxyd-loc1': { error: errors.InternalError, code: 500 },
                'sproxyd-loc2': { code: 200, message: 'OK' },
                metadata: { code: 200, message: 'OK' },
                vault: { code: 200, message: 'OK' },
                kms: { code: 200, message: 'OK' },
            });
            done();
        });
    });

    it('should fail when ALL backends of multiple clients fail', done => {
        dataStub.callsFake((log, cb) =>
            cb(null, {
                'sproxyd-loc1': { error: errors.InternalError, code: 500 },
                'sproxyd-loc2': { error: errors.InternalError, code: 500 },
            }),
        );
        metadataStub.callsFake((log, cb) =>
            cb(null, {
                metadata: { error: errors.InternalError, code: 500 },
            }),
        );
        vaultStub.callsFake((log, cb) =>
            cb(null, {
                vault: { code: 200, message: 'OK' },
            }),
        );
        kmsStub.callsFake((log, cb) =>
            cb(null, {
                kms: { code: 200, message: 'OK' },
            }),
        );

        clientCheck(false, log, (err, result) => {
            assert(err);
            assert.strictEqual(err.InternalError, true);
            assert.deepStrictEqual(result, {
                'sproxyd-loc1': { error: errors.InternalError, code: 500 },
                'sproxyd-loc2': { error: errors.InternalError, code: 500 },
                metadata: { error: errors.InternalError, code: 500 },
                vault: { code: 200, message: 'OK' },
                kms: { code: 200, message: 'OK' },
            });
            done();
        });
    });

    it('should succeed when client returns empty result', done => {
        dataStub.callsFake((log, cb) => cb(null, {}));
        metadataStub.callsFake((log, cb) =>
            cb(null, {
                metadata: { code: 200, message: 'OK' },
            }),
        );
        vaultStub.callsFake((log, cb) =>
            cb(null, {
                vault: { code: 200, message: 'OK' },
            }),
        );
        kmsStub.callsFake((log, cb) =>
            cb(null, {
                kms: { code: 200, message: 'OK' },
            }),
        );

        clientCheck(false, log, (err, result) => {
            assert.ifError(err);
            assert.deepStrictEqual(result, {
                metadata: { code: 200, message: 'OK' },
                vault: { code: 200, message: 'OK' },
                kms: { code: 200, message: 'OK' },
            });
            done();
        });
    });

    describe('external backend error handling', () => {
        it(
            'should NOT fail on external backend errors during normal operation ' + '(flightCheckOnStartUp=false)',
            done => {
                dataStub.callsFake((log, cb) =>
                    cb(null, {
                        's3-backend': { error: errors.InternalError, code: 500, external: true },
                    }),
                );
                metadataStub.callsFake((log, cb) =>
                    cb(null, {
                        metadata: { code: 200, message: 'OK' },
                    }),
                );
                vaultStub.callsFake((log, cb) => cb(null, {}));
                kmsStub.callsFake((log, cb) => cb(null, {}));

                clientCheck(false, log, (err, result) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(result, {
                        's3-backend': { error: errors.InternalError, code: 500, external: true },
                        metadata: { code: 200, message: 'OK' },
                    });
                    done();
                });
            },
        );

        it('should fail on external backend errors during startup (flightCheckOnStartUp=true)', done => {
            dataStub.callsFake((log, cb) =>
                cb(null, {
                    's3-backend': { error: errors.InternalError, code: 500, external: true },
                }),
            );
            metadataStub.callsFake((log, cb) =>
                cb(null, {
                    metadata: { code: 200, message: 'OK' },
                }),
            );
            vaultStub.callsFake((log, cb) => cb(null, {}));
            kmsStub.callsFake((log, cb) => cb(null, {}));

            clientCheck(true, log, (err, result) => {
                assert(err);
                assert.strictEqual(err.InternalError, true);
                assert.deepStrictEqual(result, {
                    's3-backend': { error: errors.InternalError, code: 500, external: true },
                    metadata: { code: 200, message: 'OK' },
                });
                done();
            });
        });

        it(
            'should succeed when external backend fails but internal backend is healthy ' +
                '(flightCheckOnStartUp=false)',
            done => {
                dataStub.callsFake((log, cb) =>
                    cb(null, {
                        'sproxyd-loc1': { code: 200, message: 'OK' },
                        's3-backend': { error: errors.InternalError, code: 500, external: true },
                    }),
                );
                metadataStub.callsFake((log, cb) =>
                    cb(null, {
                        metadata: { code: 200, message: 'OK' },
                    }),
                );
                vaultStub.callsFake((log, cb) => cb(null, {}));
                kmsStub.callsFake((log, cb) => cb(null, {}));

                clientCheck(false, log, (err, result) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(result, {
                        'sproxyd-loc1': { code: 200, message: 'OK' },
                        's3-backend': { error: errors.InternalError, code: 500, external: true },
                        metadata: { code: 200, message: 'OK' },
                    });
                    done();
                });
            },
        );
    });
});
