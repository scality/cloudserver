const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');


describe('HEAD bucket', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        it.skip('should return an error to a head request without a ' +
        'bucket name',
            done => {
                s3.headBucket({ Bucket: '' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 405);
                    done();
                });
            });
    });
});
