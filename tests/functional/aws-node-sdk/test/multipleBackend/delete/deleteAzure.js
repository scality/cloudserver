const assert = require('assert');
const async = require('async');
const crypto = require('crypto');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');
const {
    describeSkipIfNotMultiple,
    uniqName,
    getAzureClient,
    getAzureContainerName,
    getAzureKeys,
    azureLocation,
    azureLocationMismatch,
    enableVersioning,
    assertVersionedObj,
    deleteAllSnapShots,
    deleteAllBlobs,
    expectedETag,
} = require('../utils');

const keyObject = 'deleteazure';
const azureContainerName = getAzureContainerName(azureLocation);
const keys = getAzureKeys();
const azureClient = getAzureClient();

const normalBody = Buffer.from('I am a body', 'utf8');
const azureTimeout = 20000;

const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

function getAzureMD5(content) {
    return crypto.createHash('md5').update(content).digest('base64');
}

describeSkipIfNotMultiple('Multiple backend delete object from Azure',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: azureContainerName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });
        keys.forEach(key => {
            const keyName = uniqName(keyObject);
            describe(`${key.describe} size`, () => {
                before(done => {
                    s3.putObject({
                        Bucket: azureContainerName,
                        Key: keyName,
                        Body: key.body,
                        Metadata: {
                            'scal-location-constraint': azureLocation,
                        },
                    }, done);
                });

                it(`should delete an ${key.describe} object from Azure`,
                done => {
                    s3.deleteObject({
                        Bucket: azureContainerName,
                        Key: keyName,
                    }, err => {
                        assert.equal(err, null, 'Expected success ' +
                            `but got error ${err}`);
                        setTimeout(() =>
                        azureClient.getBlobProperties(azureContainerName,
                        keyName, err => {
                            assert.strictEqual(err.statusCode, 404);
                            assert.strictEqual(err.code, 'NotFound');
                            return done();
                        }), azureTimeout);
                    });
                });
            });
        });

        describe('delete from Azure location with bucketMatch set to false',
        () => {
            beforeEach(function beforeF(done) {
                this.currentTest.azureObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: this.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocationMismatch,
                    },
                }, done);
            });

            it('should delete object', function itF(done) {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: this.test.azureObject,
                }, err => {
                    assert.equal(err, null, 'Expected success ' +
                        `but got error ${err}`);
                    setTimeout(() =>
                    azureClient.getBlobProperties(azureContainerName,
                    `${azureContainerName}/${this.test.azureObject}`,
                    err => {
                        assert.strictEqual(err.statusCode, 404);
                        assert.strictEqual(err.code, 'NotFound');
                        return done();
                    }), azureTimeout);
                });
            });
        });

        describe('returning no error', () => {
            beforeEach(function beF(done) {
                this.currentTest.azureObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: this.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, err => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error ${err}`);
                    azureClient.deleteBlob(azureContainerName,
                    this.currentTest.azureObject, err => {
                        assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                        done(err);
                    });
                });
            });

            it('should return no error on deleting an object deleted ' +
            'from Azure', function itF(done) {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: this.test.azureObject,
                }, err => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error ${err}`);
                    done();
                });
            });
        });

        describe('Versioning:: ', () => {
            beforeEach(function beF(done) {
                this.currentTest.azureObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: this.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, done);
            });

            it('should not delete object when deleting a non-existing ' +
            'version from Azure', function itF(done) {
                async.waterfall([
                    next => s3.deleteObject({
                        Bucket: azureContainerName,
                        Key: this.test.azureObject,
                        VersionId: nonExistingId,
                    }, err => next(err)),
                    next => s3.getObject({
                        Bucket: azureContainerName,
                        Key: this.test.azureObject,
                    }, (err, res) => {
                        assert.equal(err, null, 'getObject: Expected success ' +
                        `but got error ${err}`);
                        assert.deepStrictEqual(res.Body, normalBody);
                        return next(err);
                    }),
                    next => azureClient.getBlobToText(azureContainerName,
                    this.test.azureObject, (err, res) => {
                        assert.equal(err, null, 'getBlobToText: Expected ' +
                        `successbut got error ${err}`);
                        assert.deepStrictEqual(Buffer.from(res, 'utf8'),
                        normalBody);
                        return next();
                    }),
                ], done);
            });
        });

        describe('with ongoing MPU: ', () => {
            beforeEach(function beF(done) {
                this.currentTest.key = uniqName(keyObject);
                const params = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.key,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Err putting object to Azure: ' +
                        `${err}`);
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    };
                    s3.createMultipartUpload(params, (err, res) => {
                        assert.equal(err, null, 'Err initiating MPU on ' +
                            `Azure: ${err}`);
                        this.currentTest.uploadId = res.UploadId;
                        setTimeout(() => done(), azureTimeout);
                    });
                });
            });

            afterEach(function afF(done) {
                s3.abortMultipartUpload({
                    Bucket: azureContainerName,
                    Key: this.currentTest.key,
                    UploadId: this.currentTest.uploadId,
                }, err => {
                    assert.equal(err, null, `Err aborting MPU: ${err}`);
                    setTimeout(() => done(), azureTimeout);
                });
            });

            it('should return InternalError', function itFn(done) {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: this.test.key,
                }, err => {
                    assert.strictEqual(err.code, 'MPUinProgress');
                    done();
                });
            });
        });

        describe('versioning behavior', () => {
            const keyPrefix = crypto.createHash('md5')
                .update(Math.random().toString())
                .digest('hex');
            const body1 = 'a';
            let key;
            let masterVersionID;

            before(done => enableVersioning(s3, azureContainerName, done));

            beforeEach(done => {
                key = `${keyPrefix}/versioned-blob-${Date.now()}`;
                async.waterfall([
                    next =>
                        s3.putObject({
                            Bucket: azureContainerName,
                            Key: key,
                            Body: body1,
                            Metadata: {
                                'scal-location-constraint': azureLocation,
                            },
                        }, next),
                    (data, next) => {
                        masterVersionID = data.VersionId;
                        assertVersionedObj(s3, azureContainerName, key,
                            data.VersionId, body1, next);
                    },
                ], done);
            });

            afterEach(done =>
                deleteAllSnapShots(azureClient, azureContainerName, keyPrefix,
                    err => {
                        if (err) {
                            return done(err);
                        }
                        return deleteAllBlobs(azureClient, azureContainerName,
                            keyPrefix, done);
                    }));

            it('should put delete marker on master version', done =>
            async.series([
                next =>
                    s3.deleteObject({
                        Bucket: azureContainerName,
                        Key: key,
                    }, (err, data) => {
                        assert.strictEqual(err, null);
                        assert.strictEqual(data.DeleteMarker, 'true');
                        return next();
                    }),
                next =>
                    s3.getObject({
                        Bucket: azureContainerName,
                        Key: key,
                    }, err => {
                        assert.strictEqual(err.code, 'NoSuchKey');
                        return next();
                    }),
                next => {
                    const options = { include: 'snapshots' };
                    azureClient.listBlobsSegmentedWithPrefix(azureContainerName,
                        keyPrefix, null, options, (err, result) => {
                            if (err) {
                                return done(err);
                            }
                            // One snapshot of the previous version exists and
                            // a zero-byte blob denoting the "delete marker".
                            assert.strictEqual(result.entries.length, 2);
                            const snapshots = result.entries.filter(entry =>
                                entry.snapshot !== undefined);
                            assert.strictEqual(snapshots.length, 1);
                            const { contentMD5 } = snapshots[0].contentSettings;
                            assert.strictEqual(contentMD5, getAzureMD5(body1));
                            return next();
                        });
                },
                next =>
                    azureClient.getBlobProperties(azureContainerName, key,
                    (err, result) => {
                        const { contentSettings } = result;
                        assert.strictEqual(
                            contentSettings.contentMD5, getAzureMD5(''));
                        return next();
                    }),
            ], done));

            it('should restore version if deleting a delete marker', done => {
                let deleteMarkerVersionId;
                async.series([
                    next =>
                        s3.deleteObject({
                            Bucket: azureContainerName,
                            Key: key,
                        }, (err, data) => {
                            assert.strictEqual(err, null);
                            assert.strictEqual(data.DeleteMarker, 'true');
                            deleteMarkerVersionId = data.VersionId;
                            return next();
                        }),
                    next =>
                        s3.getObject({
                            Bucket: azureContainerName,
                            Key: key,
                        }, err => {
                            assert.notStrictEqual(err, null);
                            assert.strictEqual(err.code, 'NoSuchKey');
                            return next();
                        }),
                    next =>
                        s3.deleteObject({
                            Bucket: azureContainerName,
                            Key: key,
                            VersionId: deleteMarkerVersionId,
                        }, err => {
                            assert.strictEqual(err, null);
                            return next();
                        }),
                    next =>
                        s3.getObject({
                            Bucket: azureContainerName,
                            Key: key,
                        }, (err, data) => {
                            assert.strictEqual(err, null);
                            assert.strictEqual(
                                data.ETag, expectedETag(data.Body));
                            setTimeout(() => next(), 3000);
                        }),
                    next =>
                        // The blob should be equivalent to the latest snapshot.
                        azureClient.getBlobProperties(azureContainerName, key,
                        (err, result) => {
                            const { contentSettings } = result;
                            assert.strictEqual(
                                contentSettings.contentMD5, getAzureMD5(''));
                            return next();
                        }),
                ], done);
            });

            it('should delete snapshot if deleting a version', done => {
                const body2 = 'b';
                let secondSnapshot;
                async.waterfall([
                    next =>
                        s3.putObject({
                            Bucket: azureContainerName,
                            Key: key,
                            Body: body2,
                            Metadata: {
                                'scal-location-constraint': azureLocation,
                            },
                        }, next),
                    (data, next) =>
                        assertVersionedObj(s3, azureContainerName, key,
                            data.VersionId, body2, next),
                    next => {
                        const options = { include: 'snapshots' };
                        azureClient.listBlobsSegmentedWithPrefix(
                            azureContainerName, keyPrefix, null, options,
                            (err, result) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.strictEqual(result.entries.length, 3);
                                const snapshots = result.entries.filter(
                                    entry => entry.snapshot !== undefined);
                                assert.strictEqual(snapshots.length, 2);
                                secondSnapshot = snapshots[1];
                                assert.strictEqual(
                                    snapshots[0].contentSettings.contentMD5,
                                    getAzureMD5(body1));
                                assert.strictEqual(
                                    snapshots[1].contentSettings.contentMD5,
                                    getAzureMD5(body2));
                                return next();
                            });
                    },
                    next =>
                        s3.deleteObject({
                            Bucket: azureContainerName,
                            Key: key,
                            VersionId: masterVersionID,
                        }, err => {
                            assert.strictEqual(err, null);
                            setTimeout(() => next(), azureTimeout);
                        }),
                    next => {
                        const options = { include: 'snapshots' };
                        azureClient.listBlobsSegmentedWithPrefix(
                            azureContainerName, keyPrefix, null, options,
                            (err, result) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.strictEqual(result.entries.length, 2);
                                const snapshots = result.entries.filter(
                                    entry => entry.snapshot !== undefined);
                                assert.strictEqual(snapshots.length, 1);
                                assert.deepStrictEqual(
                                    secondSnapshot, snapshots[0]);
                                return next();
                            });
                    },
                ], done);
            });

            it('should delete snapshot and blob if deleting last version',
            done =>
                async.waterfall([
                    next =>
                        s3.deleteObject({
                            Bucket: azureContainerName,
                            Key: key,
                            VersionId: masterVersionID,
                        }, err => {
                            assert.strictEqual(err, null);
                            setTimeout(() => next(), azureTimeout);
                        }),
                    next => {
                        const options = { include: 'snapshots' };
                        azureClient.listBlobsSegmentedWithPrefix(
                            azureContainerName, keyPrefix, null, options,
                            (err, result) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.strictEqual(result.entries.length, 0);
                                return next();
                            });
                    },
                ], done));
        });
    });
});
