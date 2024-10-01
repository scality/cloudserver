const assert = require('assert');

const { errors } = require('arsenal');
const {
    startRestore,
    validatePutVersionId,
    verifyColdObjectAvailable
} = require('../../../../lib/api/apiUtils/object/coldStorage');
const { DummyRequestLogger } = require('../../helpers');
const { ObjectMD, ObjectMDArchive } = require('arsenal/build/lib/models');
const { config } = require('../../../../lib/Config');
const { scaledMsPerDay } = config.getTimeOptions();
const log = new DummyRequestLogger();
const oneDay = 24 * 60 * 60 * 1000;

describe('cold storage', () => {
    describe('validatePutVersionId', () => {
        [
            {
                description: 'should return NoSuchKey if object metadata is empty',
                expectedRes: errors.NoSuchKey,
            },
            {
                description: 'should return NoSuchVersion if object md is empty and version id is provided',
                expectedRes: errors.NoSuchVersion,
                versionId: '123',
            },
            {
                description: 'should return MethodNotAllowed if object is a delete marker',
                objMD: {
                    isDeleteMarker: true,
                },
                expectedRes: errors.MethodNotAllowed,
            },
            {
                description: 'should return InvalidObjectState if object data is not stored in cold location',
                objMD: {
                    dataStoreName: 'us-east-1',
                },
                expectedRes: errors.InvalidObjectState,
            },
            {
                description: 'should return InvalidObjectState if object is not archived',
                objMD: {
                    dataStoreName: 'location-dmf-v1',
                },
                expectedRes: errors.InvalidObjectState,
            },
            {
                description: 'should return InvalidObjectState if object is already restored',
                objMD: {
                    dataStoreName: 'location-dmf-v1',
                    archive: {
                        restoreRequestedAt: new Date(0),
                        restoreRequestedDays: 5,
                        restoreCompletedAt: new Date(1000),
                        restoreWillExpireAt: new Date(1000 + 5 * oneDay),
                    },
                },
                expectedRes: errors.InvalidObjectState,
            },
            {
                description: 'should pass if object archived',
                objMD: {
                    dataStoreName: 'location-dmf-v1',
                    archive: {
                        restoreRequestedAt: new Date(0),
                        restoreRequestedDays: 5,
                    },
                },
                expectedRes: undefined,
            },
        ].forEach(testCase => it(testCase.description, () => {
            const res = validatePutVersionId(testCase.objMD, testCase.versionId, log);
            assert.deepStrictEqual(res, testCase.expectedRes);
        }));
    });

    describe('verifyColdObjectAvailable', () => {
        [
            {
                description: 'should return error if object is in a cold location',
                objectMd: new ObjectMD()
                    .setArchive(new ObjectMDArchive({
                        archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                        archiveVersion: 5577006791947779
                    }))
            },
            {
                description: 'should return error if object is restoring',
                objectMd: new ObjectMD()
                    .setArchive(new ObjectMDArchive({
                        archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                        archiveVersion: 5577006791947779,
                    }, Date.now()))
            },
        ].forEach(params => {
            it(`${params.description}`, () => {
                const err = verifyColdObjectAvailable(params.objectMd.getValue());
                assert(err.InvalidObjectState);
            });
        });

        it('should return null if object data is not in cold', () => {
            const objectMd = new ObjectMD();
            const err = verifyColdObjectAvailable(objectMd.getValue());
            assert.ifError(err);
        });

        it('should return null if object is transitioning to cold', () => {
            const objectMd = new ObjectMD().setTransitionInProgress(true);
            const err = verifyColdObjectAvailable(objectMd.getValue());
            assert.ifError(err);
        });

        it('should return null if object is restored', () => {
            const objectMd = new ObjectMD().setArchive(new ObjectMDArchive(
                {
                    archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                    archiveVersion: 5577006791947779,
                },
                /*restoreRequestedAt*/  new Date(0),
                /*restoreRequestedDays*/ 5,
                /*restoreCompletedAt*/  new Date(1000),
                /*restoreWillExpireAt*/ new Date(1000 + 5 * oneDay),
            ));
            const err = verifyColdObjectAvailable(objectMd.getValue());
            assert.ifError(err);
        });
    });

    describe('startRestore', () => {
        it('should fail for non-cold object', done => {
            const objectMd = new ObjectMD().setDataStoreName('aws_s3').getValue();

            startRestore(objectMd, { days: 5 }, log, err => {
                assert.deepStrictEqual(err, errors.InvalidObjectState);
                done();
            });
        });

        it('should fail when object is being restored', done => {
            const objectMd = new ObjectMD().setDataStoreName(
                'location-dmf-v1'
            ).setArchive(new ObjectMDArchive(
                {
                    archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                    archiveVersion: 5577006791947779,
                },
                /*restoreRequestedAt*/  new Date(0),
                /*restoreRequestedDays*/ 5,
            )).getValue();

            startRestore(objectMd, { days: 5 }, log, err => {
                assert.deepStrictEqual(err, errors.RestoreAlreadyInProgress);
                done();
            });
        });

        it('should fail when restored object is expired', done => {
            const objectMd = new ObjectMD().setDataStoreName(
                'location-dmf-v1'
            ).setArchive(new ObjectMDArchive(
                {
                    archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                    archiveVersion: 5577006791947779,
                },
                /*restoreRequestedAt*/  new Date(0),
                /*restoreRequestedDays*/ 5,
                /*restoreCompletedAt*/  new Date(Date.now() - 6 * oneDay),
                /*restoreWillExpireAt*/ new Date(Date.now() - 1 * oneDay),
            )).getValue();

            startRestore(objectMd, { days: 5 }, log, err => {
                assert.deepStrictEqual(err, errors.InvalidObjectState);
                done();
            });
        });

        it('should succeed for cold object', done => {
            const objectMd = new ObjectMD().setDataStoreName(
                'location-dmf-v1'
            ).setArchive(new ObjectMDArchive({
                archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                archiveVersion: 5577006791947779,
            })).getValue();

            const t = new Date();
            startRestore(objectMd, { days: 7 }, log, (err, isObjectAlreadyRestored) => {
                assert.ifError(err);
                assert.ok(!isObjectAlreadyRestored);

                assert.equal(objectMd.archive.restoreRequestedDays, 7);
                assert.notStrictEqual(objectMd.archive.restoreRequestedAt, undefined);
                assert.ok(objectMd.archive.restoreRequestedAt.getTime() >= t.getTime());
                assert.ok(objectMd.archive.restoreRequestedAt.getTime() <= Date.now());

                assert.strictEqual(objectMd.archive.restoreCompletedAt, undefined);
                assert.strictEqual(objectMd.archive.restoreWillExpireAt, undefined);

                done();
            });
        });

        it('should succeed for restored object', done => {
            const objectMd = new ObjectMD().setDataStoreName(
                'location-dmf-v1'
            ).setArchive(new ObjectMDArchive(
                {
                    archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                    archiveVersion: 5577006791947779,
                },
                /*restoreRequestedAt*/  new Date(0),
                /*restoreRequestedDays*/ 2,
                /*restoreCompletedAt*/  new Date(Date.now() - 1 * oneDay),
                /*restoreWillExpireAt*/ new Date(Date.now() + 1 * oneDay),
            )).setAmzRestore({
                'ongoing-request': false,
                'expiry-date': new Date(Date.now() + 1 * oneDay),
                'content-md5': '12345'
            }).getValue();

            const restoreCompletedAt = objectMd.archive.restoreCompletedAt;
            const t = new Date();
            startRestore(objectMd, { days: 5 }, log, (err, isObjectAlreadyRestored) => {
                assert.ifError(err);
                assert.ok(isObjectAlreadyRestored);

                assert.equal(objectMd.archive.restoreRequestedDays, 5);
                assert.notStrictEqual(objectMd.archive.restoreRequestedAt, undefined);
                assert.ok(objectMd.archive.restoreRequestedAt.getTime() >= t.getTime());
                assert.ok(objectMd.archive.restoreRequestedAt.getTime() <= new Date());

                assert.strictEqual(objectMd.archive.restoreCompletedAt, restoreCompletedAt);
                assert.strictEqual(objectMd.archive.restoreWillExpireAt.getTime(),
                    objectMd.archive.restoreRequestedAt.getTime() + (5 * scaledMsPerDay));
                assert.deepEqual(objectMd['x-amz-restore'], {
                    'ongoing-request': false,
                    'expiry-date': objectMd.archive.restoreWillExpireAt,
                    'content-md5': '12345'
                });

                done();
            });
        });
    });
});
