const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
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
} = require('../utils');

const sourceBucketName = 'buckettestobjectcopyawsversioning-source';
const destBucketName = 'buckettestobjectcopyawsversioning-dest';

const someBody = Buffer.from('I am a body', 'utf8');
const wrongVersionBody = 'this is not the content you wanted';
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);

let bucketUtil;
let s3;

function _getTestMetadata(location) {
    return {
        'scal-location-constraint': location,
        'test-header': 'copyme',
    };
}

function putSourceObj(testParams, cb) {
    const { sourceBucket, sourceLocation, isEmptyObj } = testParams;
    const sourceKey = `sourcekey-${Date.now()}`;
    const sourceParams = {
        Bucket: sourceBucket,
        Key: sourceKey,
        Metadata: _getTestMetadata(sourceLocation),
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
        destBucket, destLocation, directive, destVersioningState, isEmptyObj }
        = testParams;
    const destKey = `destkey-${Date.now()}`;
    const copyParams = {
        Bucket: destBucket,
        Key: destKey,
        CopySource: `/${sourceBucket}/${sourceKey}`,
        MetadataDirective: directive,
        Metadata: {
            'scal-location-constraint': destLocation,
        },
    };
    if (sourceVersionId) {
        copyParams.CopySource =
            `${copyParams.CopySource}?versionId=${sourceVersionId}`;
    } else if (sourceVersioningState === 'Suspended') {
        copyParams.CopySource =
            `${copyParams.CopySource}?versionId=null`;
    }
    console.log('===================');
    console.log('params sent to object copy', copyParams)
    s3.copyObject(copyParams, (err, data) => {
        assert.strictEqual(err, null,
            `Error copying object to destination: ${err}`);
            console.log('copy object result', data)
        if (destVersioningState === 'Enabled') {
            console.log('got version id for dest object', data.VersionId)
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
    console.log('assertGetObjecs');
    const {
        sourceBucket,
        sourceLocation,
        sourceKey,
        sourceVersionId,
        sourceVersioningState,
        destBucket,
        destLocation,
        destKey,
        destVersionId,
        destVersioningState,
        awsVersionId,
        isEmptyObj,
        directive,
    } = testParams;
    console.log('testParams in assertGetObjects..', testParams)
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
        console.log('******** sourceGetParams', sourceGetParams);
        console.log('==== result of getting source object', sourceRes);
        console.log('******** destGetParams', destGetParams);
        console.log('==== result of getting dest object', destRes);
        console.log('******** awsGetParams', awsParams);
        console.log('==== result of getting aws object', awsRes)
        // NOTE: assert version ids?
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
            assert.deepStrictEqual(sourceRes.Metadata['test-header'],
                destRes.Metadata['test-header']);
        } else if (directive === 'REPLACE') {
            assert.strictEqual(destRes.Metadata['test-header'],
              undefined);
        }
        assert.strictEqual(awsRes.Metadata[locMetaHeader], destLocation);
        if (directive === 'COPY') {
            assert.deepStrictEqual(sourceRes.Metadata['test-header'],
                awsRes.Metadata['test-header']);
        } else if (directive === 'REPLACE') {
            assert.strictEqual(awsRes.Metadata['test-header'],
              undefined);
        }
        assert.strictEqual(sourceRes.ContentLength, destRes.ContentLength);
        assert.strictEqual(sourceRes.Metadata[locMetaHeader], sourceLocation);
        assert.strictEqual(destRes.Metadata[locMetaHeader], destLocation);
        cb();
    });
}

/*
const testParams = {
    sourceBucket: sourceBucketName,
    sourceLocation: awsLocation,
    sourceVersioningState: undefined,
    destBucket: destBucketName,
    destLocation: awsLocation,
    destVersioningState: 'Enabled',
    isEmptyObj: false,
    directive: 'REPLACE',
};*/

// describeSkipIfNotMultiple
describe.only('AWS backend object copy with versioning',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating buckets\n');
            /* if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            } */
            return s3.createBucketAsync({ Bucket: sourceBucketName })
            .then(() => s3.createBucketAsync({ Bucket: destBucketName }))
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => bucketUtil.empty(sourceBucketName)
            .then(() => bucketUtil.deleteOne(sourceBucketName))
            .catch(err => {
                process.stdout.write('Error deleting source bucket ' +
                `in afterEach: ${err}\n`);
                throw err;
            })
            .then(() => bucketUtil.empty(destBucketName))
            .then(() => bucketUtil.deleteOne(destBucketName))
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
            it(`should copy a${isEmptyObj ? 'n empty' : ''} object from AWS ` +
            'backend non-versioned bucket to AWS backend versioned bucket ' +
            `with ${directive} directive`, done => {
                Object.assign(testParams, {
                    sourceVersioningState: undefined,
                    destVersioningState: 'Enabled',
                });
                async.waterfall([
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

            it(`should copy ${isEmptyObj ? 'an empty' : ''}version from one ` +
            `AWS backend versioned bucket to another on ${directive} directive`,
            done => {
                Object.assign(testParams, {
                    sourceVersioningState: 'Enabled',
                    destVersioningState: 'Enabled',
                });
                async.waterfall([
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

            it(`should copy ${isEmptyObj ? 'an empty' : ''} null version ` +
            'from an AWS backend versioned bucket to a non-versioned one with '
            + `${directive} directive`, done => {
                Object.assign(testParams, {
                    sourceVersioningState: 'Suspend',
                    destVersioningState: 'Suspended',
                });
                async.waterfall([
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

            it(`should copy a ${isEmptyObj ? 'empty ' : ''}version from a ` +
            'AWS backend versioned bucket to a non-versioned one with '
            + `${directive} directive`, done => {
                Object.assign(testParams, {
                    sourceVersioningState: 'Enabled',
                    destVersioningState: 'Suspended',
                });
                async.waterfall([
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
                destBucket: sourceBucketName,
                destLocation: awsLocation,
                destVersioningState: 'Enabled',
            });
            const { sourceLocation, directive, isEmptyObj } = testParams;

            it(`should copy ${isEmptyObj ? 'empty ' : ''}object from ` +
            `${sourceLocation} to same bucket on AWS backend with ` +
            `versioning with ${directive}`, done => {
                async.waterfall([
                    next => putSourceObj(testParams, next),
                    next => enableVersioning(s3, testParams.sourceBucket, next),
                    next => copyObject(testParams, next),
                    next => assertGetObjects(testParams, next),
                ], done);
            });

            it(`should copy a ${isEmptyObj ? 'empty ' : ''}version from ` +
            `${sourceLocation} to same bucket on AWS backend with ` +
            `versioning with ${directive} directive`, done => {
                async.waterfall([
                    next => enableVersioning(s3, testParams.sourceBucket, next),
                    // returns a version id which is added to testParams
                    // to be used in object copy
                    next => putSourceObj(testParams, next),
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
