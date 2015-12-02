import { expect } from 'chai';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import bucketDelete from '../../../lib/api/bucketDelete';

const accessKey = 'accessKey1';
const namespace = 'default';

describe("bucketDelete API", () => {
    let metastore;

    beforeEach(() => {
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
    });

    const bucketName = 'bucketname';
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
                            expect(err).to.equal('BucketNotEmpty');
                            expect(metastore.users[accessKey]
                                .buckets).to.have.length.of(1);
                            expect(Object.keys(metastore.buckets))
                                .to.have.length.of(1);
                            done();
                        });
                });
        });
    });

    it('should delete a bucket', (done) => {
        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            bucketDelete(accessKey, metastore, testDeleteRequest,
                (err, response) => {
                    expect(response).to
                        .equal('Bucket deleted permanently');
                    expect(metastore.users[accessKey].buckets)
                        .to.have.length.of(0);
                    expect(Object.keys(metastore.buckets))
                        .to.have.length.of(0);
                    done();
                });
        });
    });
});
