import assert from 'assert';

import bucketDelete from '../../../lib/api/bucketDelete';
import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];
const usersBucket = constants.usersBucket;
// TODO: Remove references to metastore.  This is GH Issue #172
const metastore = undefined;

describe("bucketDelete API", () => {
    afterEach(done => {
        metadata.deleteBucket(bucketName, () => {
            metadata.deleteBucket(usersBucket, () => {
                done();
            });
        });
    });

    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace,
    };
    const testDeleteRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace,
    };

    it('should return an error if the bucket is not empty', (done) => {
        const objectName = 'objectName';
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testBucketPutRequest, log, () => {
            objectPut(accessKey, metastore, testPutObjectRequest, log, () => {
                bucketDelete(accessKey, metastore, testDeleteRequest, log,
                    err => {
                        assert.strictEqual(err, 'BucketNotEmpty');
                        metadata.getBucket(bucketName, (err, md) => {
                            assert.strictEqual(md.name, bucketName);
                            metadata.listObject(usersBucket, accessKey,
                                null, null, null, (err, listResponse) => {
                                    assert.strictEqual(listResponse.Contents.
                                        length, 1);
                                    done();
                                });
                        });
                    });
            });
        });
    });

    it('should delete a bucket', (done) => {
        bucketPut(accessKey, metastore, testBucketPutRequest, log, () => {
            bucketDelete(accessKey, metastore, testDeleteRequest, log, () => {
                metadata.getBucket(bucketName, (err, md) => {
                    assert.strictEqual(err, 'NoSuchBucket');
                    assert.strictEqual(md, undefined);
                    metadata.listObject(usersBucket, accessKey,
                        null, null, null, (err, listResponse) => {
                            assert.strictEqual(listResponse.Contents.length, 0);
                            done();
                        });
                });
            });
        });
    });

    it('should prevent anonymous user from accessing delete bucket API',
        done => {
            bucketDelete('http://acs.amazonaws.com/groups/global/AllUsers',
                metastore, testDeleteRequest, log, err => {
                    assert.strictEqual(err, 'AccessDenied');
                    done();
                });
        });
});
