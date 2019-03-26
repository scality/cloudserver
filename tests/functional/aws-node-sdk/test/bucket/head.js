const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');


describe('HEAD bucket', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeAll(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });


        test(
            'should return an error to a head request without a bucket name',
            done => {
                s3.headBucket({ Bucket: '' }, err => {
                    expect(err).not.toEqual(null);
                    expect(err.code).toBe(405);
                    done();
                });
            }
        );
    });
});
