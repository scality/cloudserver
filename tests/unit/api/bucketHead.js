import assert from 'assert';

import bucketHead from '../../../lib/api/bucketHead';
import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const testRequest = {
    bucketName,
    namespace,
    lowerCaseHeaders: {},
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};
describe('bucketHead API', () => {
    beforeEach((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    it('should return an error if the bucket does not exist', (done) => {
        bucketHead(authInfo, testRequest, log, (err) => {
            assert.strictEqual(err, 'NoSuchBucket');
            done();
        });
    });

    it('should return an error if user is not authorized', (done) => {
        const otherAuthInfo = makeAuthInfo('accessKey2');
        bucketPut(otherAuthInfo, testRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                bucketHead(authInfo, testRequest, log,
                    (err) => {
                        assert.strictEqual(err, 'AccessDenied');
                        done();
                    });
            });
    });

    it('should return a success message if ' +
       'bucket exists and user is authorized', (done) => {
        bucketPut(authInfo, testRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                bucketHead(authInfo, testRequest, log,
                    (err, result) => {
                        assert.strictEqual(result,
                        'Bucket exists and user authorized -- 200');
                        done();
                    });
            });
    });
});
