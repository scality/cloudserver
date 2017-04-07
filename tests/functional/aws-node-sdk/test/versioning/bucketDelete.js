import assert from 'assert';
import async from 'async';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

import { removeAllVersions } from '../../lib/utility/versioning-util.js';

const bucketName = `versioning-bucket-${Date.now()}`;
const key = 'anObject';


function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
}

function checkNoError(err) {
    assert.ifError(err, `Expected success, got error ${JSON.stringify(err)}`);
}

describe('aws-node-sdk test delete bucket', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        // setup test
        beforeEach(done => {
            async.waterfall([
                next => s3.createBucket({ Bucket: bucketName },
                    err => next(err)),
                next => s3.putBucketVersioningAsync({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                }, err => next(err)),
            ], done);
        });

        // empty and delete bucket after testing if bucket exists
        afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
                if (err && err.code === 'NoSuchBucket') {
                    return done();
                } else if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, done);
            });
        });

        it('should be able to delete empty bucket with version enabled',
        done => {
            s3.deleteBucket({ Bucket: bucketName }, err => {
                checkNoError(err);
                return done();
            });
        });

        it('should return error 409 BucketNotEmpty if trying to delete bucket' +
        ' containing delete marker', done => {
            s3.deleteObject({ Bucket: bucketName, Key: key }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, err => {
                    checkError(err, 'BucketNotEmpty');
                    return done();
                });
            });
        });

        it('should return error 409 BucketNotEmpty if trying to delete bucket' +
        ' containing version and delete marker', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: key },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName, Key: key },
                  err => next(err)),
                next => s3.deleteBucket({ Bucket: bucketName }, err => {
                    checkError(err, 'BucketNotEmpty');
                    return next();
                }),
            ], done);
        });
    });
});
