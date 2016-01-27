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
    beforeEach((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    const testBucketPutRequest = {
        bucketName,
        namespace,
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
    };
    const objectKey = 'objectName';
    const testPutObjectRequest = {
        bucketName,
        namespace,
        objectKey,
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectKey}`,
        post: postBody,
        calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
    };
    const testGetObjectRequest = {
        bucketName,
        namespace,
        objectKey,
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectKey}`,
    };
    const testDeleteRequest = {
        bucketName,
        namespace,
        objectKey,
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectKey}`,
    };

    it.skip('should set delete markers ' +
            'when versioning enabled', () => {
        // TODO
    });

    it('should delete an object', (done) => {
        bucketPut(accessKey,  testBucketPutRequest, log, () => {
            objectPut(accessKey,  testPutObjectRequest, log, () => {
                objectDelete(accessKey,  testDeleteRequest, log,
                    (err) => {
                        assert.strictEqual(err, undefined);
                        objectGet(accessKey,  testGetObjectRequest,
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
             testDeleteRequest, log,
                (err) => {
                    assert.strictEqual(err, 'AccessDenied');
                });
        done();
    });
});
