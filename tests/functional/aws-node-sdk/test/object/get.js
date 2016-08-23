import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';


describe('GET object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });


        it('should return an error to get request without a valid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'NoSuchBucket');
                    done();
                });
            });
    });
});
