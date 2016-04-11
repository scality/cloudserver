import assert from 'assert';
import tv4 from 'tv4';
import Promise from 'bluebird';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import bucketSchema from '../../schema/bucket';

describe('GET Bucket - AWS.S3.listObjects', () => {
    describe('When user is unauthorized', () => {
        let bucketUtil;
        let bucketName;

        before(done => {
            bucketUtil = new BucketUtility();
            bucketUtil.createRandom(1).catch(done)
                      .then(created => {
                          bucketName = created;
                          done();
                      });
        });

        after(done => {
            bucketUtil.deleteOne(bucketName)
                      .then(() => done())
                      .catch(done);
        });

        it('should return 403 and AccessDenied on a private bucket', done => {
            const params = { Bucket: bucketName };

            bucketUtil.s3
                .makeUnauthenticatedRequest('listObjects', params, error => {
                    assert(error);
                    assert.strictEqual(error.statusCode, 403);
                    assert.strictEqual(error.code, 'AccessDenied');
                    done();
                });
        });
    });

    withV4(sigCfg => {
        let bucketUtil;
        let bucketName;

        before(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            bucketUtil.createRandom(1)
                      .catch(done)
                      .then(created => {
                          bucketName = created;
                          done();
                      });
        });

        after(done => {
            bucketUtil.deleteOne(bucketName).then(() => done()).catch(done);
        });

        afterEach(done => {
            bucketUtil.empty(bucketName).catch(done).done(() => done());
        });

        it('should return created objects in alphabetical order', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const objects = [
                { Bucket, Key: 'testB/' },
                { Bucket, Key: 'testB/test.json', Body: '{}' },
                { Bucket, Key: 'testA/' },
                { Bucket, Key: 'testA/test.json', Body: '{}' },
                { Bucket, Key: 'testA/test/test.json', Body: '{}' },
            ];

            Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.listObjectsAsync({ Bucket }))
                .then(data => {
                    const isValidResponse = tv4.validate(data, bucketSchema);
                    if (!isValidResponse) {
                        throw new Error(tv4.error);
                    }
                    return data;
                }).then(data => {
                    const keys = data.Contents.map(object => object.Key);
                    assert.equal(data.Name, Bucket, 'Bucket name mismatch');
                    assert.deepEqual(keys, [
                        'testA/',
                        'testA/test.json',
                        'testA/test/test.json',
                        'testB/',
                        'testB/test.json',
                    ], 'Bucket content mismatch');
                    done();
                }).catch(done);
        });
    });
});
