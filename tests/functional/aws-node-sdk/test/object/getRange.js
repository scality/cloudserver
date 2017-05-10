const assert = require('assert');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'bucket-for-range-test';
const objName = 'largerput';
let s3;

const endRangeTest = (inputRange, expectedRange, cb) => {
    const params = {
        Bucket: bucketName,
        Key: objName,
        Range: inputRange,
    };

    s3.getObject(params, (err, data) => {
        assert.strictEqual(data.ContentLength, '90');
        assert.strictEqual(data.ContentRange, expectedRange);
        assert.deepStrictEqual(data.Body, Buffer.allocUnsafe(90).fill(1));
        cb();
    });
};

describe('aws-node-sdk range test of large end position', () => {
    withV4(sigCfg => {
        let bucketUtil;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucketName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            }).then(() =>
                s3.putObjectAsync({
                    Bucket: bucketName,
                    Key: objName,
                    Body: Buffer.allocUnsafe(2890).fill(0, 0, 2800)
                                                  .fill(1, 2800),
                }))
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucketName)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucketName);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-',
            done => endRangeTest('bytes=2800-', 'bytes 2800-2889/2890', done)
        );

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-Number.MAX_SAFE_INTEGER',
            done => endRangeTest(`bytes=2800-${Number.MAX_SAFE_INTEGER}`,
                                 'bytes 2800-2889/2890', done)
        );
    });
});
