const assert = require('assert');
const { 
    GetObjectCommand, 
    CreateBucketCommand, 
    PutObjectCommand 
} = require('@aws-sdk/client-s3');
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

    s3.send(new GetObjectCommand(params))
        .then(async data => {
            assert.strictEqual(data.ContentLength, 90);
            assert.strictEqual(data.ContentRange, expectedRange);
            const chunks = [];
            for await (const chunk of data.Body) {
                chunks.push(chunk);
            }
            const bodyBuffer = Buffer.concat(chunks);
            const expectedBuffer = Buffer.allocUnsafe(90).fill(1);
            assert.deepStrictEqual(bodyBuffer, expectedBuffer);
            cb();
        })
        .catch(err => {
            cb(err);
        });
};

describe('aws-node-sdk range test of large end position', () => {
    withV4(sigCfg => {
        let bucketUtil;

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            try {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objName,
                    Body: Buffer.allocUnsafe(2890).fill(0, 0, 2800)
                                                  .fill(1, 2800),
                }));
            } catch (err) {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            }
        });

        afterEach(async () => {
            process.stdout.write('Emptying bucket');
            try {
                await bucketUtil.empty(bucketName);
                process.stdout.write('Deleting bucket');
                await bucketUtil.deleteOne(bucketName);
            } catch (err) {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            }
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
