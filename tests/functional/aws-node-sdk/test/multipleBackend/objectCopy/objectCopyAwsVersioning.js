const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');
const {
    describeSkipIfNotMultiple,
    awsS3,
    awsBucket,
    memLocation,
    fileLocation,
    awsLocation,
    awsLocation2,
    awsLocationMismatch,
    awsLocationEncryption,
    enableVersioning,
} = require('../utils');

const bucket = 'buckettestmultiplebackendobjectcopyawsversioning';
const someBody = Buffer.from('I am a body', 'utf8');
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
    const { sourceBucket, sourceKey, sourceVersionId, destBucket,
        destLocation, directive, destVersioningState }
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
    }
    console.log('===================');
    console.log('params sent to object copy', copyParams)
    s3.copyObject(copyParams, (err, data) => {
        assert.strictEqual(err, null,
            `Error copying object to destination: ${err}`);
            console.log('copy object result', data)
        if (destVersioningState === 'Enabled') {
            assert.notEqual(data.VersionId, undefined);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }
        Object.assign(testParams, {
            destKey,
            destVersionId: data.VersionId,
        });
        cb();
    });
}

function assertGetObjects(testParams, cb/* sourceParams, destParams, destKey,
destBucket, destLoc, awsKey, mdDirective, isEmptyObj, awsS3, awsLocation,
callback*/) {
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
        isEmpty,
        directive,
    } = testParams;
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey,
        VersionId: sourceVersionId };
    const destGetParams = { Bucket: destBucket, Key: destKey,
        VersionId: destVersionId };
    const awsParams = { Bucket: awsBucket, Key: destKey };

    async.series([
        cb => s3.getObject(sourceGetParams, cb),
        cb => s3.getObject(destGetParams, cb),
        cb => awsS3.getObject(awsParams, cb),
    ], (err, results) => {
        assert.strictEqual(err, null, `Error in assertGetObjects: ${err}`);
        const [sourceRes, destRes, awsRes] = results;
        console.log('==== result of getting source object', sourceRes);
        console.log('==== result of getting dest object', destRes);
        console.log('==== result of getting aws object', awsRes)
        // NOTE: assert version ids?
        if (isEmpty) {
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

// describeSkipIfNotMultiple
describe.only('AWS backend object copy with versioning',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            }
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
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should copy an object from one bucket to another on AWS backend',
        done => {

        });

        [{
            sourceLocation: memLocation,
            directive: 'REPLACE',
        }, {
            sourceLocation: fileLocation,
            directive: 'REPLACE',
        }, {
            sourceLocation: memLocation,
            directive: 'COPY',
        }, {
            sourceLocation: fileLocation,
            directive: 'COPY',
        }].forEach(testParams => {
            Object.assign(testParams, {
                sourceBucket: bucket,
                sourceVersioningState: 'Enabled',
                destBucket: bucket,
                destLocation: awsLocation,
                destVersioningState: 'Enabled',
                isEmpty: false,
            });
            const { sourceLocation, directive } = testParams;
            it(`should copy a version from ${sourceLocation} to same bucket ` +
                `on AWS backend with versioning with ${directive} directive`,
                done => {
                    async.waterfall([
                        next => enableVersioning(s3, testParams.sourceBucket,
                            next),
                        // returns a version id which is added to testParams
                        // to be used in object copy
                        next => putSourceObj(testParams, next),
                        next => copyObject(testParams, next),
                        next => assertGetObjects(testParams, next),
                    ], done);
                });

            it(`should copy normal object from ${sourceLocation} to same ` +
                `bucket on AWS backend with versioning with ${directive}`,
                done => {
                    async.waterfall([
                        next => putSourceObj(testParams, next),
                        next => enableVersioning(s3, testParams.sourceBucket,
                            next),
                        next => copyObject(testParams, next),
                        next => assertGetObjects(testParams, next),
                    ], done);
                });
        });
    });
});
