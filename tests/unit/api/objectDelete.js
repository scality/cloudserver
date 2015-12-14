import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../../../lib/metadata/wrapper';
import objectPut from '../../../lib/api/objectPut';
import objectDelete from '../../../lib/api/objectDelete';
import utils from '../../../lib/utils';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const testBucketUID = utils.getResourceUID(namespace, bucketName);
const postBody = [ new Buffer('I am a body')];

describe('objectDelete API', () => {
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

    after((done) => {
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const objectName = 'objectName';
    const testPutObjectRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
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
            objectPut(accessKey, metastore, testPutObjectRequest, () => {
                objectDelete(accessKey, metastore, testDeleteRequest,
                    (err, response) => {
                        assert.strictEqual(response,
                            'ObjectDeletedPermanently');
                        done();
                    });
            });
        });
    });
});
