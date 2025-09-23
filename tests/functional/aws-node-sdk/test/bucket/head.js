const assert = require('assert');
const { S3Client, HeadBucketCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');


describe('HEAD bucket', () => {
    withV4(sigCfg => {
        let s3;

        before(() => {
            const config = getConfig('default', sigCfg);
            s3 = new S3Client(config);
        });

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        it.skip('should return an error to a head request without a ' +
        'bucket name',
            async () => {
                try {
                    await s3.send(new HeadBucketCommand({ Bucket: '' }));
                    assert.fail('Expected failure but got success');
                } catch (err) {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 405);
                }
            });
    });
});
