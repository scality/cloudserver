import assert from 'assert';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import bucketDelete from '../../../lib/api/bucketDelete';
import metadata from '../metadataswitch';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];

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
        metadata.deleteBucket(bucketName, ()=> {
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
                            metadata.getBucket(bucketName, (err, md) => {
                                assert.strictEqual(md.name, bucketName);
                                done();
                            });
                        });
                });
        });
    });

    it('should delete a bucket', (done) => {
        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            bucketDelete(accessKey, metastore, testDeleteRequest, () => {
                assert.strictEqual(metastore
                    .users[accessKey].buckets.length, 0);
                metadata.getBucket(bucketName, (err, md) => {
                    assert.strictEqual(err, 'NoSuchBucket');
                    assert.strictEqual(md, undefined);
                    done();
                });
            });
        });
    });

    it('should prevent anonymous user from accessing ' +
        'delete bucket API', (done) => {
        bucketDelete('http://acs.amazonaws.com/groups/global/AllUsers',
            metastore, testDeleteRequest,
                (err) => {
                    assert.strictEqual(err, 'AccessDenied');
                });
        done();
    });
});
