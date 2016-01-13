import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import objectDelete from '../../../lib/api/objectDelete';
import objectGet from '../../../lib/api/objectGet';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
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
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
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
    const testGetObjectRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName}`,
        namespace: namespace
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
        bucketPut(accessKey, metastore, testBucketPutRequest, log, () => {
            objectPut(accessKey, metastore, testPutObjectRequest, log, () => {
                objectDelete(accessKey, metastore, testDeleteRequest, log,
                    (err) => {
                        assert.strictEqual(err, undefined);
                        objectGet(accessKey, metastore, testGetObjectRequest,
                            log, (err) => {
                                assert.strictEqual(err, 'NoSuchKey');
                                done();
                            });
                    });
            });
        });
    });

    it('should prevent anonymous user from accessing ' +
        'deleteObject API', (done) => {
        objectDelete('http://acs.amazonaws.com/groups/global/AllUsers',
            metastore, testDeleteRequest, log,
                (err) => {
                    assert.strictEqual(err, 'AccessDenied');
                });
        done();
    });
});
