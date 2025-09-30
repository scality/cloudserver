const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const { GetObjectCommand,
    PutObjectCommand,
    CreateBucketCommand,
    DeleteObjectCommand } = require('@aws-sdk/client-s3');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    awsS3,
    awsLocation,
    awsBucket,
    enableVersioning,
    suspendVersioning,
    mapToAwsPuts,
    putNullVersionsToAws,
    putVersionsToAws,
    getAndAssertResult,
    describeSkipIfNotMultiple,
    genUniqID,
} = require('../utils');

const someBody = 'testbody';
const bucket = `getawsversioning${genUniqID()}`;

function getAndAssertVersions(s3, bucket, key, versionIds, expectedData,
    cb) {
    async.mapSeries(versionIds, (versionId, next) => {
        s3.send(new GetObjectCommand({ Bucket: bucket, Key: key,
            VersionId: versionId })).then(async result => {
                const resultBody = await result.Body.transformToString();
                next(null, {
                    VersionId: result.VersionId,
                    Body: resultBody
                });
            })
            .catch(err => {
                next(err);
            });
    }, (err, results) => {
        if (err) {
            return cb(err);
        }
        const resultIds = results.map(result => result.VersionId);
        const resultData = results.map(result => result.Body);
        assert.deepStrictEqual(resultIds, versionIds);
        assert.deepStrictEqual(resultData, expectedData);
        return cb();
    });
}

describeSkipIfNotMultiple('AWS backend get object with versioning',
function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.send(new CreateBucketCommand({ Bucket: bucket }))
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });

        it('should not return version ids when versioning has not been ' +
        'configured via CloudServer', done => {
            const key = `somekey-${genUniqID()}`;
            s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: someBody,
            Metadata: { 'scal-location-constraint': awsLocation } })).then(data => {
                assert.strictEqual(data.VersionId, undefined);
                getAndAssertResult(s3, { bucket, key, body: someBody,
                    expectedVersionId: false }, done);
            }).catch(err => {
                assert.strictEqual(err, null, 'Expected success ' +
                    `putting object, got error ${err}`);
                done();
            });
        });

        it('should not return version ids when versioning has not been ' +
        'configured via CloudServer, even when version id specified', done => {
            const key = `somekey-${genUniqID()}`;
            s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: someBody,
            Metadata: { 'scal-location-constraint': awsLocation } })).then(data => {
                assert.strictEqual(data.VersionId, undefined);
                getAndAssertResult(s3, { bucket, key, body: someBody,
                    versionId: 'null', expectedVersionId: false }, done);
            }).catch(err => {   
                assert.strictEqual(err, null, 'Expected success ' +
                    `putting object, got error ${err}`);
                done();
            });
        });

        it('should return version id for null version when versioning ' +
        'has been configured via CloudServer', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: someBody,
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(() => next())
                .catch(err => {
                    assert.strictEqual(err, null, 'Expected success ' +
                        `putting object, got error ${err}`);
                    next(err);
                }),
                next => enableVersioning(s3, bucket, next),
                // get with version id specified
                next => getAndAssertResult(s3, { bucket, key, body: someBody,
                    versionId: 'null', expectedVersionId: 'null' }, next),
                // get without version id specified
                next => getAndAssertResult(s3, { bucket, key, body: someBody,
                    expectedVersionId: 'null' }, next),
            ], done);
        });

        it('should overwrite the null version if putting object twice ' +
        'before versioning is configured', done => {
            const key = `somekey-${genUniqID()}`;
            const data = ['data1', 'data2'];
            async.waterfall([
                next => mapToAwsPuts(s3, bucket, key, data, err => next(err)),
                // get latest version
                next => getAndAssertResult(s3, { bucket, key, body: data[1],
                    expectedVersionId: false }, next),
                // get specific version
                next => getAndAssertResult(s3, { bucket, key, body: data[1],
                    versionId: 'null', expectedVersionId: false }, next),
            ], done);
        });

        it('should overwrite existing null version if putting object ' +
        'after suspending versioning', done => {
            const key = `somekey-${genUniqID()}`;
            const data = ['data1', 'data2'];
            async.waterfall([
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: data[0],
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(() => next())
                .catch(err => {
                    assert.strictEqual(err, null, 'Expected success ' +
                        `putting object, got error ${err}`);
                    next(err);
                }),
                next => suspendVersioning(s3, bucket, next),
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: data[1],
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(() => next())
                .catch(err => {
                    assert.strictEqual(err, null, 'Expected success ' +
                        `putting object, got error ${err}`);
                    next(err);
                }),
                // get latest version
                next => getAndAssertResult(s3, { bucket, key, body: data[1],
                    expectedVersionId: 'null' }, next),
                // get specific version
                next => getAndAssertResult(s3, { bucket, key, body: data[1],
                    versionId: 'null', expectedVersionId: 'null' }, next),
            ], done);
        });

        it('should overwrite null version if putting object when ' +
        'versioning is suspended after versioning enabled', done => {
            const key = `somekey-${genUniqID()}`;
            const data = [...Array(3).keys()].map(i => `data${i}`);
            let firstVersionId;
            async.waterfall([
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: data[0],
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(() => next())
                .catch(err => {
                    assert.strictEqual(err, null, 'Expected success ' +
                        `putting object, got error ${err}`);
                    next(err);
                }),
                next => enableVersioning(s3, bucket, next),
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: data[1],
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(result => {
                        assert.notEqual(result.VersionId, 'null');
                        firstVersionId = result.VersionId;
                        next();
                    }).catch(err => {
                        assert.strictEqual(err, null, 'Expected success ' +
                            `putting object, got error ${err}`);
                        next(err);
                    }),
                next => suspendVersioning(s3, bucket, next),
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: data[3],
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(() => next())
                .catch(err => {
                    assert.strictEqual(err, null, 'Expected success ' +
                        `putting object, got error ${err}`);
                    next(err);
                }),
                // get latest version
                next => getAndAssertResult(s3, { bucket, key, body: data[3],
                    expectedVersionId: 'null' }, next),
                // get specific version (null)
                next => getAndAssertResult(s3, { bucket, key, body: data[3],
                    versionId: 'null', expectedVersionId: 'null' }, next),
                // assert getting first version put for good measure
                next => getAndAssertResult(s3, { bucket, key, body: data[1],
                    versionId: firstVersionId,
                    expectedVersionId: firstVersionId }, next),
            ], done);
        });

        it('should get correct data from aws backend using version IDs',
        done => {
            const key = `somekey-${genUniqID()}`;
            const data = [...Array(5).keys()].map(i => i.toString());
            const versionIds = ['null'];
            async.waterfall([
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: data[0],
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(() => next())
                .catch(err => {
                    next(err);
                }),
                next => putVersionsToAws(s3, bucket, key, data.slice(1), next),
                (ids, next) => {
                    versionIds.push(...ids);
                    next();
                },
                next => getAndAssertVersions(s3, bucket, key, versionIds, data,
                    next),
            ], done);
        });

        it('should get correct version when getting without version ID',
        done => {
            const key = `somekey-${genUniqID()}`;
            const data = [...Array(5).keys()].map(i => i.toString());
            const versionIds = ['null'];
            async.waterfall([
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: data[0],
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(() => next())
                .catch(err => {
                    next(err);
                }),
                next => putVersionsToAws(s3, bucket, key, data.slice(1), next),
                (ids, next) => {
                    versionIds.push(...ids);
                    next();
                },
                next => getAndAssertResult(s3, { bucket, key, body: data[4],
                    expectedVersionId: versionIds[4] }, next),
            ], done);
        });

        it('should get correct data from aws backend using version IDs ' +
        'after putting null versions, putting versions, putting more null ' +
        'versions and then putting more versions',
        done => {
            const key = `somekey-${genUniqID()}`;
            const data = [...Array(16).keys()].map(i => i.toString());
            // put three null versions,
            // 5 real versions,
            // three null versions,
            // 5 versions again
            const firstThreeNullVersions = data.slice(0, 3);
            const firstFiveVersions = data.slice(3, 8);
            const secondThreeNullVersions = data.slice(8, 11);
            const secondFiveVersions = data.slice(11, 16);
            const versionIds = [];
            const lastNullVersion = secondThreeNullVersions[2];
            const finalDataArr = firstFiveVersions.concat([lastNullVersion])
                .concat(secondFiveVersions);
            async.waterfall([
                next => mapToAwsPuts(s3, bucket, key, firstThreeNullVersions,
                    err => next(err)),
                next => putVersionsToAws(s3, bucket, key, firstFiveVersions,
                    next),
                (ids, next) => {
                    versionIds.push(...ids);
                    next();
                },
                next => putNullVersionsToAws(s3, bucket, key,
                    secondThreeNullVersions, err => next(err)),
                next => putVersionsToAws(s3, bucket, key, secondFiveVersions,
                    next),
                (ids, next) => {
                    versionIds.push('null');
                    versionIds.push(...ids);
                    next();
                },
                // get versions by id
                next => getAndAssertVersions(s3, bucket, key, versionIds,
                    finalDataArr, next),
                // get and assert latest version
                next => getAndAssertResult(s3, { bucket, key, body: data[16],
                    versionId: versionIds[versionIds.length - 1],
                    expectedVersionId: versionIds[versionIds.length - 1] },
                    next),
            ], done);
        });

        it('should return the correct data getting versioned object ' +
        'even if object was deleted from AWS (creating a delete marker)',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: someBody,
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(res => next(null, res.VersionId))
                .catch(err => {
                    next(err);
                }),
                // create a delete marker in AWS
                (versionId, next) => awsS3.deleteObject({ Bucket: awsBucket,
                    Key: key }, err => next(err, versionId)),
                (versionId, next) => getAndAssertResult(s3, { bucket, key,
                    body: someBody, expectedVersionId: versionId }, next),
            ], done);
        });

        it('should return the correct data getting versioned object ' +
        'even if object is put directly to AWS (creating new version)',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: someBody,
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(res => next(null, res.VersionId))
                .catch(err => {
                    next(err);
                }),
                // put an object in AWS
                (versionId, next) => awsS3.send(new PutObjectCommand({ Bucket: awsBucket,
                    Key: key })).then(() => next(null, versionId))
                .catch(err => {
                    next(err);
                }),
                (versionId, next) => getAndAssertResult(s3, { bucket, key,
                    body: someBody, expectedVersionId: versionId }, next),
            ], done);
        });

        it('should return a LocationNotFound if trying to get an object ' +
        'that was deleted in AWS but exists in s3 metadata',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: someBody,
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(res => next(null, res.VersionId))
                .catch(err => {
                    next(err);
                }),
                // get the latest version id in aws
                (s3vid, next) => awsS3.send(new GetObjectCommand({ Bucket: awsBucket,
                    Key: key })).then(res => next(null, s3vid, res.VersionId))
                .catch(err => {
                    next(err);
                }),
                (s3VerId, awsVerId, next) => awsS3.send(new DeleteObjectCommand({
                    Bucket: awsBucket, Key: key, VersionId: awsVerId })).then(() => next(null, s3VerId))
                .catch(err => {
                    next(err);
                }),
                (s3VerId, next) => s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
                .then(res => next(null, s3VerId, res.VersionId))
                .catch(err => {
                        assert.strictEqual(err.name, 'LocationNotFound');
                        assert.strictEqual(err.$metadata.httpStatusCode, 424);
                        next();
                    }),
            ], done);
        });

        it('should return a LocationNotFound if trying to get a version ' +
        'that was deleted in AWS but exists in s3 metadata',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: someBody,
                    Metadata: { 'scal-location-constraint': awsLocation } })).then(res => next(null, res.VersionId))
                .catch(err => {
                    next(err);
                }),
                // get the latest version id in aws
                (s3vid, next) => awsS3.send(new GetObjectCommand({ Bucket: awsBucket,
                    Key: key })).then(res => next(null, s3vid, res.VersionId))
                .catch(err => {
                    next(err);
                }),
                (s3VerId, awsVerId, next) => awsS3.send(new DeleteObjectCommand({
                    Bucket: awsBucket, Key: key, VersionId: awsVerId })).then(() => next(null, s3VerId))
                .catch(err => {
                    next(err);
                }),
                (s3VerId, next) => s3.send(new GetObjectCommand({ Bucket: bucket, Key: key,
                    VersionId: s3VerId })).then(() => {
                    next();
                }).catch(err => {
                    assert.strictEqual(err.name, 'LocationNotFound');
                    assert.strictEqual(err.$metadata.httpStatusCode, 424);
                    next();
                }),
            ], done);
        });
    });
});
