import { expect } from 'chai';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import objectDelete from '../../../lib/api/objectDelete';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('objectDelete API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });

    const bucketName = 'bucketname';
    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const postBody = 'I am a body';
    const objectName = 'objectName';
    const testPutObjectRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };
    const testDeleteRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName}`,
        namespace: namespace
    };

    it.skip('should set delete markers ' +
            'when versioning enabled', () => {
        // TODO
    });

    it('should delete an object', (done) => {
        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                () => {
                    objectDelete(accessKey, datastore, metastore,
                        testDeleteRequest, (err, response) => {
                            expect(response)
                                .to.equal('Object deleted permanently');
                            expect(Object.keys(datastore)
                                .length).to.equal(0);
                            done();
                        });
                });
        });
    });
});
