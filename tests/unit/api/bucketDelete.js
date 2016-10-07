import { errors } from 'arsenal';
import assert from 'assert';

import bucketDelete from '../../../lib/api/bucketDelete';
import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import DummyRequest from '../DummyRequest';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const usersBucket = constants.usersBucket;
const locationConstraint = 'us-west-1';

describe('bucketDelete API', () => {
    beforeEach(() => {
        cleanup();
    });

    const testRequest = {
        bucketName,
        namespace,
        headers: {},
        url: `/${bucketName}`,
    };

    it('should return an error if the bucket is not empty', done => {
        const objectName = 'objectName';
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            objectKey: objectName,
        }, postBody);

        bucketPut(authInfo, testRequest, locationConstraint, log, err => {
            assert.strictEqual(err, undefined);
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                bucketDelete(authInfo, testRequest, log, err => {
                    assert.deepStrictEqual(err, errors.BucketNotEmpty);
                    metadata.getBucket(bucketName, log, (err, md) => {
                        assert.strictEqual(md.getName(), bucketName);
                        metadata.listObject(usersBucket,
                            authInfo.getCanonicalID(),
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
        bucketPut(authInfo, testRequest, locationConstraint, log, () => {
            bucketDelete(authInfo, testRequest, log, () => {
                metadata.getBucket(bucketName, log, (err, md) => {
                    assert.deepStrictEqual(err, errors.NoSuchBucket);
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
            assert.deepStrictEqual(err, errors.AccessDenied);
            done();
        });
    });
});
