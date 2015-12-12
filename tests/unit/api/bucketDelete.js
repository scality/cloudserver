import assert from 'assert';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import bucketDelete from '../../../lib/api/bucketDelete';
import metadata from '../../../lib/metadata/wrapper';
import utils from '../../../lib/utils';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const testBucketUID =
    utils.getResourceUID(namespace, bucketName);

describe("bucketDelete API", () => {
    let metastore;

    beforeEach((done) => {
        metastore = {
            "users": {
                "accessKey1": {
                    "buckets": []
                },
                "accessKey2": {
                    "buckets": []
                }
            },
            "buckets": {}
        };
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const testDeleteRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace
    };

    it('should return an error if the bucket is not empty', (done) => {
        const postBody = 'I am a body';
        const objectName = 'objectName';
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            objectPut(accessKey, metastore, testPutObjectRequest,
                () => {
                    bucketDelete(accessKey, metastore, testDeleteRequest,
                        (err) => {
                            assert.strictEqual(err, 'BucketNotEmpty');
                            assert.strictEqual(metastore.users[accessKey]
                                .buckets.length, 1);
                            metadata.getBucket(testBucketUID, (err, md) => {
                                assert.strictEqual(md.name, bucketName);
                                done();
                            });
                        });
                });
        });
    });

    it('should delete a bucket', (done) => {
        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            bucketDelete(accessKey, metastore, testDeleteRequest,
                (err, response) => {
                    assert.strictEqual(response, 'Bucket deleted permanently');
                    assert.strictEqual(metastore
                        .users[accessKey].buckets.length, 0);
                    metadata.getBucket(testBucketUID, (err, md) => {
                        assert.strictEqual(err, 'NoSuchBucket');
                        assert.strictEqual(md, undefined);
                    });
                    done();
                });
        });
    });
});
