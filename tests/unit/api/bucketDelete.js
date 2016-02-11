import assert from 'assert';

import bucketDelete from '../../../lib/api/bucketDelete';
import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';
import DummyRequest from '../DummyRequest';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = new Buffer('I am a body');
const usersBucket = constants.usersBucket;

describe("bucketDelete API", () => {
    afterEach(done => {
        metadata.deleteBucket(bucketName, log, () => {
            metadata.deleteBucket(usersBucket, log, () => {
                done();
            });
        });
    });

    const testRequest = {
        bucketName,
        namespace,
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
    };

    it('should return an error if the bucket is not empty', done => {
        const objectName = 'objectName';
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            objectKey: objectName,
        }, postBody);

        bucketPut(authInfo, testRequest, log, err => {
            assert.strictEqual(err, null);
            objectPut(authInfo, testPutObjectRequest, log, (err) => {
                assert.strictEqual(err, undefined);
                bucketDelete(authInfo, testRequest, log, err => {
                    assert.strictEqual(err, 'BucketNotEmpty');
                    metadata.getBucket(bucketName, log, (err, md) => {
                        assert.strictEqual(md.name, bucketName);
                        metadata.listObject(usersBucket, canonicalID,
                            null, null, null, log, (err, listResponse) => {
                                assert.strictEqual(listResponse.Contents.length,
                                                   1);
                                done();
                            });
                    });
                });
            });
        });
    });

    it('should delete a bucket', done => {
        bucketPut(authInfo, testRequest, log, () => {
            bucketDelete(authInfo, testRequest, log, () => {
                metadata.getBucket(bucketName, log, (err, md) => {
                    assert.strictEqual(err, 'NoSuchBucket');
                    assert.strictEqual(md, undefined);
                    metadata.listObject(usersBucket, canonicalID,
                        null, null, null, log, (err, listResponse) => {
                            assert.strictEqual(listResponse.Contents.length, 0);
                            done();
                        });
                });
            });
        });
    });

    it('should prevent anonymous user delete bucket API access', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        bucketDelete(publicAuthInfo, testRequest, log, err => {
            assert.strictEqual(err, 'AccessDenied');
            done();
        });
    });
});
