const assert = require('assert');
const async = require('async');
const { errors } = require('arsenal');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');

const {
    describeSkipIfNotMultiple,
    awsS3,
    awsLocation,
    awsBucket,
    putToAwsBackend,
    enableVersioning,
    suspendVersioning,
    mapToAwsPuts,
    putNullVersionsToAws,
    putVersionsToAws,
    getAndAssertResult,
    awsGetLatestVerId,
} = require('../utils');

const someBody = 'testbody';
const bucket = 'buckettestmultiplebackenddeleteversioning';

// order of items by index:
// 0 - whether to expect a version id
// 1 - whether version id should match request version id (undef if n/a)
// 2 - whether x-amz-delete-marker response header should be true
const _deleteResultSchema = {
    nonVersionedDelete: [false, undefined, false],
    newDeleteMarker: [true, false, true],
    deleteVersion: [true, true, false],
    deleteDeleteMarker: [true, true, true],
};

const [nonVersionedDelete, newDeleteMarker, deleteVersion, deleteDeleteMarker]
    = Object.keys(_deleteResultSchema);

function _assertDeleteResult(result, resultType, requestVersionId) {
    if (!_deleteResultSchema[resultType]) {
        throw new Error(`undefined result type "${resultType}"`);
    }
    const [expectVersionId, matchReqVersionId, expectDeleteMarker] =
        _deleteResultSchema[resultType];
    if (expectVersionId && matchReqVersionId) {
        assert.strictEqual(result.VersionId, requestVersionId);
    } else if (expectVersionId) {
        assert(result.VersionId, 'expected version id in result');
    } else {
        assert.strictEqual(result.VersionId, undefined,
            `did not expect version id in result, got "${result.VersionId}"`);
    }
    if (expectDeleteMarker) {
        assert.strictEqual(result.DeleteMarker, 'true');
    } else {
        assert.strictEqual(result.DeleteMarker, undefined);
    }
}

function delAndAssertResult(s3, params, cb) {
    const { bucket, key, versionId, resultType, resultError } = params;
    return s3.deleteObject({ Bucket: bucket, Key: key, VersionId:
        versionId }, (err, result) => {
        if (resultError) {
            assert(err, `expected ${resultError} but found no error`);
            assert.strictEqual(err.code, resultError);
            assert.strictEqual(err.statusCode, errors[resultError].code);
            return cb(null);
        }
        assert.strictEqual(err, null, 'Expected success ' +
            `deleting object, got error ${err}`);
        _assertDeleteResult(result, resultType, versionId);
        return cb(null, result.VersionId);
    });
}

function _createDeleteMarkers(s3, bucket, key, count, cb) {
    return async.timesSeries(count,
        (i, next) => delAndAssertResult(s3, { bucket, key,
            resultType: newDeleteMarker }, next),
        cb);
}

function _deleteDeleteMarkers(s3, bucket, key, deleteMarkerVids, cb) {
    return async.mapSeries(deleteMarkerVids, (versionId, next) => {
        delAndAssertResult(s3, { bucket, key, versionId,
            resultType: deleteDeleteMarker }, next);
    }, () => cb());
}

function _getAssertDeleted(s3, params, cb) {
    const { key, versionId, errorCode } = params;
    return s3.getObject({ Bucket: bucket, Key: key, VersionId: versionId },
        err => {
            assert.strictEqual(err.code, errorCode);
            assert.strictEqual(err.statusCode, 404);
            return cb();
        });
}

function _awsGetAssertDeleted(params, cb, isRetry) {
    const { key, versionId, errorCode } = params;
    const getObject = awsS3.getObject.bind(awsS3);
    const timeout = isRetry ? 30000 : 10000;
    return setTimeout(getObject, timeout, { Bucket: awsBucket, Key: key,
        VersionId: versionId }, err => {
            if ((!err || err.statusCode !== 404) && !isRetry) {
                // expected 404 error, retry once with a longer timeout
                _awsGetAssertDeleted(params, cb, true);
            }
            assert.strictEqual(err.code, errorCode);
            assert.strictEqual(err.statusCode, 404);
            return cb();
        });
}

describeSkipIfNotMultiple('AWS backend delete object w. versioning: ' +
    'using object location constraint', function testSuite() {
    this.timeout(120000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        beforeEach(() => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
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

        it('versioning not configured: if specifying "null" version, should ' +
        'delete specific version in AWS backend', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putToAwsBackend(s3, bucket, key, someBody,
                    err => next(err)),
                next => awsGetLatestVerId(key, someBody, next),
                (awsVerId, next) => delAndAssertResult(s3, { bucket,
                    key, versionId: 'null', resultType: deleteVersion },
                    err => next(err, awsVerId)),
                (awsVerId, next) => _awsGetAssertDeleted({ key,
                    versionId: awsVerId, errorCode: 'NoSuchVersion' }, next),
            ], done);
        });

        it('versioning not configured: specifying any version id other ' +
        'than null should not result in its deletion in AWS backend', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putToAwsBackend(s3, bucket, key, someBody,
                    err => next(err)),
                next => awsGetLatestVerId(key, someBody, next),
                (awsVerId, next) => delAndAssertResult(s3, { bucket,
                    key, versionId: 'awsVerId', resultError:
                    'InvalidArgument' }, err => next(err, awsVerId)),
                (awsVerId, next) => awsGetLatestVerId(key, someBody,
                    (err, resultVid) => {
                        assert.strictEqual(resultVid, awsVerId);
                        next();
                    }),
            ], done);
        });

        it('versioning suspended: should delete a specific version in AWS ' +
        'backend successfully', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putNullVersionsToAws(s3, bucket, key, [someBody],
                    err => next(err)),
                next => awsGetLatestVerId(key, someBody, next),
                (awsVerId, next) => delAndAssertResult(s3, { bucket,
                    key, versionId: 'null', resultType: deleteVersion },
                    err => next(err, awsVerId)),
                (awsVerId, next) => _awsGetAssertDeleted({ key,
                    versionId: awsVerId, errorCode: 'NoSuchVersion' }, next),
            ], done);
        });

        it('versioning enabled: should delete a specific version in AWS ' +
        'backend successfully', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    (err, versionIds) => next(err, versionIds[0])),
                (s3vid, next) => awsGetLatestVerId(key, someBody,
                    (err, awsVid) => next(err, s3vid, awsVid)),
                (s3VerId, awsVerId, next) => delAndAssertResult(s3, { bucket,
                    key, versionId: s3VerId, resultType: deleteVersion },
                    err => next(err, awsVerId)),
                (awsVerId, next) => _awsGetAssertDeleted({ key,
                    versionId: awsVerId, errorCode: 'NoSuchVersion' }, next),
            ], done);
        });

        it('versioning not configured: deleting existing object should ' +
        'not return version id or x-amz-delete-marker: true but should ' +
        'create a delete marker in aws ', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putToAwsBackend(s3, bucket, key, someBody,
                    err => next(err)),
                next => delAndAssertResult(s3, { bucket, key,
                    resultType: nonVersionedDelete }, err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                next => _awsGetAssertDeleted({ key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });

        it('versioning suspended: should create a delete marker in s3 ' +
        'and aws successfully when deleting existing object', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putNullVersionsToAws(s3, bucket, key, [someBody],
                    err => next(err)),
                next => delAndAssertResult(s3, { bucket, key, resultType:
                    newDeleteMarker }, err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                next => _awsGetAssertDeleted({ key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });

        // NOTE: Normal deletes when versioning is suspended create a
        // delete marker with the version id "null", which overwrites an
        // existing null version in s3 metadata.
        it('versioning suspended: creating a delete marker will overwrite an ' +
        'existing null version that is the latest version in s3 metadata,' +
        ' but the data of the first null version will remain in AWS',
        function itF(done) {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putNullVersionsToAws(s3, bucket, key, [someBody],
                    err => next(err)),
                next => awsGetLatestVerId(key, someBody, next),
                (awsNullVid, next) => {
                    this.test.awsNullVid = awsNullVid;
                    next();
                },
                // following call should generate a delete marker
                next => delAndAssertResult(s3, { bucket, key, resultType:
                    newDeleteMarker }, next),
                // delete delete marker
                (dmVid, next) => delAndAssertResult(s3, { bucket, key,
                    versionId: dmVid, resultType: deleteDeleteMarker },
                    err => next(err)),
                // should get no such object even after deleting del marker
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                // get directly to aws however will give us first null version
                next => awsGetLatestVerId(key, someBody, next),
                (awsLatestVid, next) => {
                    assert.strictEqual(awsLatestVid, this.test.awsNullVid);
                    next();
                },
            ], done);
        });

        // NOTE: Normal deletes when versioning is suspended create a
        // delete marker with the version id "null" which is supposed to
        // overwrite any existing null version.
        it('versioning suspended: creating a delete marker will overwrite an ' +
        'existing null version that is not the latest version in s3 metadata,' +
        ' but the data of the first null version will remain in AWS',
        function itF(done) {
            const key = `somekey-${Date.now()}`;
            const data = [undefined, 'data1'];
            async.waterfall([
                // put null version
                next => putToAwsBackend(s3, bucket, key, data[0],
                    err => next(err)),
                next => awsGetLatestVerId(key, '', next),
                (awsNullVid, next) => {
                    this.test.awsNullVid = awsNullVid;
                    next();
                },
                // enable versioning and put another version
                next => putVersionsToAws(s3, bucket, key, [data[1]], next),
                (versions, next) => {
                    this.test.s3vid = versions[0];
                    next();
                },
                next => suspendVersioning(s3, bucket, next),
                // overwrites null version in s3 metadata but does not send
                // additional delete to AWS to clean up previous "null" version
                next => delAndAssertResult(s3, { bucket, key,
                    resultType: newDeleteMarker }, next),
                (s3dmVid, next) => {
                    this.test.s3DeleteMarkerId = s3dmVid;
                    next();
                },
                // delete delete marker
                next => delAndAssertResult(s3, { bucket, key,
                    versionId: this.test.s3DeleteMarkerId,
                    resultType: deleteDeleteMarker }, err => next(err)),
                // deleting latest version after del marker
                next => delAndAssertResult(s3, { bucket, key,
                    versionId: this.test.s3vid, resultType: deleteVersion },
                    err => next(err)),
                // should get no such object instead of null version
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                // we get the null version that should have been "overwritten"
                // when getting the latest version in AWS now
                next => awsGetLatestVerId(key, '', next),
                (awsLatestVid, next) => {
                    assert.strictEqual(awsLatestVid, this.test.awsNullVid);
                    next();
                },
            ], done);
        });

        it('versioning enabled: should create a delete marker in s3 and ' +
        'aws successfully when deleting existing object', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    err => next(err)),
                next => delAndAssertResult(s3, { bucket, key, resultType:
                    newDeleteMarker }, err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                next => _awsGetAssertDeleted({ key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });

        it('versioning enabled: should delete a delete marker in s3 and ' +
        'aws successfully', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    (err, versionIds) => next(err, versionIds[0])),
                // create a delete marker
                (s3vid, next) => delAndAssertResult(s3, { bucket, key,
                    resultType: newDeleteMarker }, (err, delMarkerVid) =>
                    next(err, s3vid, delMarkerVid)),
                // delete delete marker
                (s3vid, dmVid, next) => delAndAssertResult(s3, { bucket, key,
                    versionId: dmVid, resultType: deleteDeleteMarker },
                    err => next(err, s3vid)),
                // should be able to get object originally put from s3
                (s3vid, next) => getAndAssertResult(s3, { bucket, key,
                    body: someBody, expectedVersionId: s3vid }, next),
                // latest version in aws should now be object originally put
                next => awsGetLatestVerId(key, someBody, next),
            ], done);
        });

        it('multiple delete markers: should be able to get pre-existing ' +
        'versions after creating and deleting several delete markers', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    (err, versionIds) => next(err, versionIds[0])),
                (s3vid, next) => _createDeleteMarkers(s3, bucket, key, 3,
                    (err, dmVids) => next(err, s3vid, dmVids)),
                (s3vid, dmVids, next) => _deleteDeleteMarkers(s3, bucket, key,
                    dmVids, () => next(null, s3vid)),
                // should be able to get object originally put from s3
                (s3vid, next) => getAndAssertResult(s3, { bucket, key,
                    body: someBody, expectedVersionId: s3vid }, next),
                // latest version in aws should now be object originally put
                next => awsGetLatestVerId(key, someBody, next),
            ], done);
        });

        it('multiple delete markers: should get NoSuchObject if only ' +
        'one of the delete markers is deleted', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    err => next(err)),
                next => _createDeleteMarkers(s3, bucket, key, 3,
                    (err, dmVids) => next(err, dmVids[2])),
                (lastDmVid, next) => delAndAssertResult(s3, { bucket,
                    key, versionId: lastDmVid, resultType: deleteDeleteMarker },
                    err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                next => _awsGetAssertDeleted({ key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });

        it('should get the new latest version after deleting the latest' +
        'specific version', done => {
            const key = `somekey-${Date.now()}`;
            const data = [...Array(4).keys()].map(i => i.toString());
            async.waterfall([
                // put 3 null versions
                next => mapToAwsPuts(s3, bucket, key, data.slice(0, 3),
                    err => next(err)),
                // put one version
                next => putVersionsToAws(s3, bucket, key, [data[3]],
                    (err, versionIds) => next(err, versionIds[0])),
                // delete the latest version
                (versionId, next) => delAndAssertResult(s3, { bucket,
                    key, versionId, resultType: deleteVersion },
                    err => next(err)),
                // should get the last null version
                next => getAndAssertResult(s3, { bucket, key,
                    body: data[2], expectedVersionId: 'null' }, next),
                next => awsGetLatestVerId(key, data[2],
                    err => next(err)),
                // delete the null version
                next => delAndAssertResult(s3, { bucket,
                    key, versionId: 'null', resultType: deleteVersion },
                    err => next(err)),
                // s3 metadata should report no existing versions for keyname
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                // NOTE: latest version in aws will be the second null version
                next => awsGetLatestVerId(key, data[1],
                    err => next(err)),
            ], done);
        });

        it('should delete the correct version even if other versions or ' +
        'delete markers put directly on aws', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    (err, versionIds) => next(err, versionIds[0])),
                (s3vid, next) => awsGetLatestVerId(key, someBody,
                    (err, awsVid) => next(err, s3vid, awsVid)),
                // put an object in AWS
                (s3vid, awsVid, next) => awsS3.putObject({ Bucket: awsBucket,
                    Key: key }, err => next(err, s3vid, awsVid)),
                // create a delete marker in AWS
                (s3vid, awsVid, next) => awsS3.deleteObject({ Bucket: awsBucket,
                    Key: key }, err => next(err, s3vid, awsVid)),
                // delete original version in s3
                (s3vid, awsVid, next) => delAndAssertResult(s3, { bucket, key,
                    versionId: s3vid, resultType: deleteVersion },
                    err => next(err, awsVid)),
                (awsVid, next) => _getAssertDeleted(s3, { key,
                    errorCode: 'NoSuchKey' }, () => next(null, awsVid)),
                (awsVerId, next) => _awsGetAssertDeleted({ key,
                    versionId: awsVerId, errorCode: 'NoSuchVersion' }, next),
            ], done);
        });

        it('should not return an error deleting a version that was already ' +
        'deleted directly from AWS backend', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    (err, versionIds) => next(err, versionIds[0])),
                (s3vid, next) => awsGetLatestVerId(key, someBody,
                    (err, awsVid) => next(err, s3vid, awsVid)),
                // delete the object in AWS
                (s3vid, awsVid, next) => awsS3.deleteObject({ Bucket: awsBucket,
                    Key: key, VersionId: awsVid }, err => next(err, s3vid)),
                // then try to delete in S3
                (s3vid, next) => delAndAssertResult(s3, { bucket, key,
                    versionId: s3vid, resultType: deleteVersion },
                    err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });
    });
});

describeSkipIfNotMultiple('AWS backend delete object w. versioning: ' +
    'using bucket location constraint', function testSuite() {
    this.timeout(120000);
    const createBucketParams = {
        Bucket: bucket,
        CreateBucketConfiguration: {
            LocationConstraint: awsLocation,
        },
    };
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        beforeEach(() => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync(createBucketParams)
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

        it('versioning not configured: deleting non-existing object should ' +
        'not return version id or x-amz-delete-marker: true nor create a ' +
        'delete marker in aws ', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => delAndAssertResult(s3, { bucket, key,
                    resultType: nonVersionedDelete }, err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                next => _awsGetAssertDeleted({ key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });

        it('versioning suspended: should create a delete marker in s3 ' +
        'and aws successfully when deleting non-existing object', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => suspendVersioning(s3, bucket, next),
                next => delAndAssertResult(s3, { bucket, key, resultType:
                    newDeleteMarker }, err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                next => _awsGetAssertDeleted({ key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });

        it('versioning enabled: should create a delete marker in s3 and ' +
        'aws successfully when deleting non-existing object', done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => delAndAssertResult(s3, { bucket, key, resultType:
                    newDeleteMarker }, err => next(err)),
                next => _getAssertDeleted(s3, { key, errorCode: 'NoSuchKey' },
                    next),
                next => _awsGetAssertDeleted({ key, errorCode: 'NoSuchKey' },
                    next),
            ], done);
        });
    });
});
