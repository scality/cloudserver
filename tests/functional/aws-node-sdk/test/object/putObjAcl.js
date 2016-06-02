import assert from 'assert';
import Promise from 'bluebird';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

describe.only('PUT Object ACL', () => {
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

        it('should put object ACLs', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const Key = 'aclTest';
            const objects = [
                { Bucket, Key },
            ];

            Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.putObjectAclAsync({ Bucket, Key,
                    ACL: 'public-read' }))
                .then(data => {
                    assert(data);
                    done();
                }).catch(done);
        });

        it('should return NoSuchKey if try to put object ACLs ' +
            'for nonexistent object', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const Key = 'aclTest';

            s3.putObjectAcl({
                Bucket,
                Key,
                ACL: 'public-read' }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchKey');
                done();
            });
        });
    });
});
