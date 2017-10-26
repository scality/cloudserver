const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
    awsS3,
    awsBucket,
    memLocation,
    fileLocation,
    awsLocation,
    enableVersioning,
    suspendVersioning,
    putToAwsBackend,
    awsGetLatestVerId,
    getAndAssertResult,
} = require('../utils');

const sourceBucketName = 'buckettestobjectcopyawsversioning-source';
const destBucketName = 'buckettestobjectcopyawsversioning-dest';

const someBody = Buffer.from('I am a body', 'utf8');
const wrongVersionBody = 'this is not the content you wanted';
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const testMetadata = { 'test-header': 'copyme' };

let bucketUtil;
let s3;

function _getCreateBucketParams(bucket, location) {
    return {
        Bucket: bucket,
        CreateBucketConfiguration: {
            LocationConstraint: location,
        },
    };
}

function createBuckets(testParams, cb) {
    const { sourceBucket, sourceLocation, destBucket, destLocation }
        = testParams;
    const sourceParams = _getCreateBucketParams(sourceBucket, sourceLocation);
    const destParams = _getCreateBucketParams(destBucket, destLocation);
    if (sourceBucket === destBucket) {
        return s3.createBucket(sourceParams, err => cb(err));
    }
    return async.map([sourceParams, destParams],
        (createParams, next) => s3.createBucket(createParams, next),
        err => cb(err));
}

function putSourceObj(testParams, cb) {
    const { sourceBucket, isEmptyObj } = testParams;
    const sourceKey = `sourcekey-${Date.now()}`;
    const sourceParams = {
        Bucket: sourceBucket,
        Key: sourceKey,
        Metadata: testMetadata,
    };
    if (!isEmptyObj) {
        sourceParams.Body = someBody;
    }
    s3.putObject(sourceParams, (err, result) => {
        assert.strictEqual(err, null,
            `Error putting source object: ${err}`);
        if (isEmptyObj) {
            assert.strictEqual(result.ETag, `"${emptyMD5}"`);
        } else {
            assert.strictEqual(result.ETag, `"${correctMD5}"`);
        }
        Object.assign(testParams, {
            sourceKey,
            sourceVersionId: result.VersionId,
        });
        cb();
    });
}

function copyObject(testParams, cb) {
    const { sourceBucket, sourceKey, sourceVersionId, sourceVersioningState,
        destBucket, directive, destVersioningState, isEmptyObj }
        = testParams;
    const destKey = `destkey-${Date.now()}`;
    const copyParams = {
        Bucket: destBucket,
        Key: destKey,
        CopySource: `/${sourceBucket}/${sourceKey}`,
        MetadataDirective: directive,
    };
    if (sourceVersionId) {
        copyParams.CopySource =
            `${copyParams.CopySource}?versionId=${sourceVersionId}`;
    } else if (sourceVersioningState === 'Suspended') {
        copyParams.CopySource =
            `${copyParams.CopySource}?versionId=null`;
    }
    s3.copyObject(copyParams, (err, data) => {
        assert.strictEqual(err, null,
            `Error copying object to destination: ${err}`);
        if (destVersioningState === 'Enabled') {
            assert.notEqual(data.VersionId, undefined);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }
        const expectedBody = isEmptyObj ? '' : someBody;
        return awsGetLatestVerId(destKey, expectedBody, (err, awsVersionId) => {
            Object.assign(testParams, {
                destKey,
                destVersionId: data.VersionId,
                awsVersionId,
            });
            if (!data.VersionId && destVersioningState === 'Suspended') {
                // eslint-disable-next-line no-param-reassign
                testParams.destVersionId = 'null';
            }
            cb();
        });
    });
}

function assertGetObjects(testParams, cb) {
    const {
        sourceBucket,
        sourceKey,
        sourceVersionId,
        destBucket,
        destKey,
        destVersionId,
        awsVersionId,
        isEmptyObj,
        directive,
    } = testParams;
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey,
        VersionId: sourceVersionId };
    const destGetParams = { Bucket: destBucket, Key: destKey,
        VersionId: destVersionId };
    const awsParams = { Bucket: awsBucket, Key: destKey,
        VersionId: awsVersionId };

    async.series([
        cb => s3.getObject(sourceGetParams, cb),
        cb => s3.getObject(destGetParams, cb),
        cb => awsS3.getObject(awsParams, cb),
    ], (err, results) => {
        assert.strictEqual(err, null, `Error in assertGetObjects: ${err}`);
        const [sourceRes, destRes, awsRes] = results;
        if (isEmptyObj) {
            assert.strictEqual(sourceRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(destRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(awsRes.ETag, `"${emptyMD5}"`);
        } else {
            assert.strictEqual(sourceRes.ETag, `"${correctMD5}"`);
            assert.strictEqual(destRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, destRes.Body);
            assert.strictEqual(awsRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, awsRes.Body);
        }
        if (directive === 'COPY') {
            assert.deepStrictEqual(sourceRes.Metadata, testMetadata);
            assert.deepStrictEqual(sourceRes.Metadata, destRes.Metadata);
            assert.deepStrictEqual(sourceRes.Metadata, awsRes.Metadata);
        } else if (directive === 'REPLACE') {
            assert.deepStrictEqual(destRes.Metadata, {});
            assert.deepStrictEqual(awsRes.Metadata, {});
        }
        assert.strictEqual(sourceRes.ContentLength, destRes.ContentLength);
        cb();
    });
}

describeSkipIfNotMultiple('AWS backend object copy with versioning',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;

        afterEach(() => bucketUtil.empty(sourceBucketName)
            .then(() => bucketUtil.deleteOne(sourceBucketName))
            .catch(err => {
                process.stdout.write('Error deleting source bucket ' +
                `in afterEach: ${err}\n`);
                throw err;
            })
            .then(() => bucketUtil.empty(destBucketName))
            .then(() => bucketUtil.deleteOne(destBucketName))
            .catch(err => {
                if (err.code === 'NoSuchBucket') {
                    process.stdout.write('Warning: did not find dest bucket ' +
                    'for deletion');
                    // we do not throw err since dest bucket may not exist
                    // if we are using source as dest
                } else {
                    process.stdout.write('Error deleting dest bucket ' +
                    `in afterEach: ${err}\n`);
                    throw err;
                }
            })
        );

        [{
            directive: 'REPLACE',
            isEmptyObj: true,
        }, {
            directive: 'REPLACE',
            isEmptyObj: false,
        }, {
            directive: 'COPY',
            isEmptyObj: false,
        }].forEach(testParams => {
            Object.assign(testParams, {
                sourceBucket: sourceBucketName,
                sourceLocation: awsLocation,
                destBucket: destBucketName,
                destLocation: awsLocation,
            });
            const { isEmptyObj, directive } = testParams;
            it(`should copy ${isEmptyObj ? 'an empty' : ''} object from AWS ` +
            'backend non-versioned bucket to AWS backend versioned bucket ' +
            `with ${directive} directive`, done => {
                Object.assign(testParams, {
                    sourceVersioningState: undefined,
                    destVersioningState: 'Enabled',
                });
                async.waterfall([
                    next => createBuckets(testParams, next),
                    next => putSourceObj(testParams, next),
                    next => enableVersioning(s3, testParams.destBucket, next),
                    next => copyObject(testParams, next),
                    // put another version to test and make sure version id from
                    // copy was stored to get the right version
                    next => putToAwsBackend(s3, destBucketName,
                        testParams.destKey, wrongVersionBody, () => next()),
                    next => assertGetObjects(testParams, next),
                ], done);
            });

            it(`should copy ${isEmptyObj ? 'an empty ' : ''}version from one ` +
            `AWS backend versioned bucket to another on ${directive} directive`,
            done => {
                Object.assign(testParams, {
                    sourceVersioningState: 'Enabled',
                    destVersioningState: 'Enabled',
                });
                async.waterfall([
                    next => createBuckets(testParams, next),
                    next => enableVersioning(s3, testParams.sourceBucket, next),
                    next => putSourceObj(testParams, next),
                    next => enableVersioning(s3, testParams.destBucket, next),
                    next => copyObject(testParams, next),
                    // put another version to test and make sure version id from
                    // copy was stored to get the right version
                    next => putToAwsBackend(s3, destBucketName,
                        testParams.destKey, wrongVersionBody, () => next()),
                    next => assertGetObjects(testParams, next),
                ], done);
            });

            it(`should copy ${isEmptyObj ? 'an empty ' : ''}null version ` +
            'from one AWS backend versioning suspended bucket to another '
            + `versioning suspended bucket with ${directive} directive`,
            done => {
                Object.assign(testParams, {
                    sourceVersioningState: 'Suspended',
                    destVersioningState: 'Suspended',
                });
                async.waterfall([
                    next => createBuckets(testParams, next),
                    next => suspendVersioning(s3, testParams.sourceBucket,
                        next),
                    next => putSourceObj(testParams, next),
                    next => suspendVersioning(s3, testParams.destBucket, next),
                    next => copyObject(testParams, next),
                    next => enableVersioning(s3, testParams.destBucket, next),
                    // put another version to test and make sure version id from
                    // copy was stored to get the right version
                    next => putToAwsBackend(s3, destBucketName,
                        testParams.destKey, wrongVersionBody, () => next()),
                    next => assertGetObjects(testParams, next),
                ], done);
            });

            it(`should copy ${isEmptyObj ? 'an empty ' : ''}version from a ` +
            'AWS backend versioned bucket to a versioned-suspended one with '
            + `${directive} directive`, done => {
                Object.assign(testParams, {
                    sourceVersioningState: 'Enabled',
                    destVersioningState: 'Suspended',
                });
                async.waterfall([
                    next => createBuckets(testParams, next),
                    next => enableVersioning(s3, testParams.sourceBucket, next),
                    next => putSourceObj(testParams, next),
                    next => suspendVersioning(s3, testParams.destBucket, next),
                    next => copyObject(testParams, next),
                    // put another version to test and make sure version id from
                    // copy was stored to get the right version
                    next => enableVersioning(s3, testParams.destBucket, next),
                    next => putToAwsBackend(s3, destBucketName,
                        testParams.destKey, wrongVersionBody, () => next()),
                    next => assertGetObjects(testParams, next),
                ], done);
            });
        });

        it('versioning not configured: if copy object to a pre-existing ' +
        'object on AWS backend, metadata should be overwritten but data of ' +
        'previous version in AWS should not be deleted', function itF(done) {
            const destKey = `destkey-${Date.now()}`;
            const testParams = {
                sourceBucket: sourceBucketName,
                sourceLocation: awsLocation,
                sourceVersioningState: undefined,
                destBucket: sourceBucketName,
                destLocation: awsLocation,
                destVersioningState: undefined,
                isEmptyObj: true,
                directive: 'REPLACE',
            };
            async.waterfall([
                next => createBuckets(testParams, next),
                next => putToAwsBackend(s3, testParams.destBucket, destKey,
                    someBody, err => next(err)),
                next => awsGetLatestVerId(destKey, someBody, next),
                (awsVerId, next) => {
                    this.test.awsVerId = awsVerId;
                    next();
                },
                next => putSourceObj(testParams, next),
                next => s3.copyObject({
                    Bucket: testParams.destBucket,
                    Key: destKey,
                    CopySource: `/${testParams.sourceBucket}` +
                        `/${testParams.sourceKey}`,
                    MetadataDirective: testParams.directive,
                    Metadata: {
                        'scal-location-constraint': testParams.destLocation,
                    },
                }, next),
                (copyResult, next) => awsGetLatestVerId(destKey, '',
                    (err, awsVersionId) => {
                        testParams.destKey = destKey;
                        testParams.destVersionId = copyResult.VersionId;
                        testParams.awsVersionId = awsVersionId;
                        next();
                    }),
                next => s3.deleteObject({ Bucket: testParams.destBucket,
                    Key: testParams.destKey, VersionId: 'null' }, next),
                (delData, next) => getAndAssertResult(s3, { bucket:
                    testParams.destBucket, key: testParams.destKey,
                    expectedError: 'NoSuchKey' }, next),
                next => awsGetLatestVerId(testParams.destKey, someBody, next),
                (awsVerId, next) => {
                    assert.strictEqual(awsVerId, this.test.awsVerId);
                    next();
                },
            ], done);
        });

        [{
            sourceLocation: memLocation,
            directive: 'REPLACE',
            isEmptyObj: true,
        }, {
            sourceLocation: fileLocation,
            directive: 'REPLACE',
            isEmptyObj: true,
        }, {
            sourceLocation: memLocation,
            directive: 'COPY',
            isEmptyObj: false,
        }, {
            sourceLocation: fileLocation,
            directive: 'COPY',
            isEmptyObj: false,
        }].forEach(testParams => {
            Object.assign(testParams, {
                sourceBucket: sourceBucketName,
                sourceVersioningState: 'Enabled',
                destBucket: destBucketName,
                destLocation: awsLocation,
                destVersioningState: 'Enabled',
            });
            const { sourceLocation, directive, isEmptyObj } = testParams;

            it(`should copy ${isEmptyObj ? 'empty ' : ''}object from ` +
            `${sourceLocation} to bucket on AWS backend with ` +
            `versioning with ${directive}`, done => {
                async.waterfall([
                    next => createBuckets(testParams, next),
                    next => putSourceObj(testParams, next),
                    next => enableVersioning(s3, testParams.destBucket, next),
                    next => copyObject(testParams, next),
                    next => assertGetObjects(testParams, next),
                ], done);
            });

            it(`should copy ${isEmptyObj ? 'an empty ' : ''}version from ` +
            `${sourceLocation} to bucket on AWS backend with ` +
            `versioning with ${directive} directive`, done => {
                async.waterfall([
                    next => createBuckets(testParams, next),
                    next => enableVersioning(s3, testParams.sourceBucket, next),
                    // returns a version id which is added to testParams
                    // to be used in object copy
                    next => putSourceObj(testParams, next),
                    next => enableVersioning(s3, testParams.destBucket, next),
                    next => copyObject(testParams, next),
                    // put another version to test and make sure version id
                    // from copy was stored to get the right version
                    next => putToAwsBackend(s3, destBucketName,
                        testParams.destKey, wrongVersionBody, () => next()),
                    next => assertGetObjects(testParams, next),
                ], done);
            });
        });
    });
});
