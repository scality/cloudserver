const assert = require('assert');
const {
    PutObjectCommand,
    HeadObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const objectName = 'someObject';
const firstPutMetadata = {
    firstput: 'firstValue',
    firstputagain: 'firstValue',
    evenmoreonfirst: 'stuff',
};
const secondPutMetadata = {
    secondput: 'secondValue',
    secondputagain: 'secondValue',
};


describe('Put object with same key as prior object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let bucketName;

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            bucketName = await bucketUtil.createRandom(1);
        });

        beforeEach(async () => {
            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName,
                Body: 'I am the best content ever',
                Metadata: firstPutMetadata,
            }));
            const res = await s3.send(new HeadObjectCommand({ 
                Bucket: bucketName, 
                Key: objectName 
            }));
            assert.deepStrictEqual(res.Metadata, firstPutMetadata);
        });

        afterEach(async () => await bucketUtil.empty(bucketName));

        after(async () => await bucketUtil.deleteOne(bucketName));

        it('should overwrite all user metadata and data on overwrite put',
            async () => {
                await s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'Much different',
                    Metadata: secondPutMetadata,
                }));
                const res = await s3.send(new GetObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }));
                assert.deepStrictEqual(res.Metadata, secondPutMetadata);
                const bodyText = await res.Body.transformToString();
                assert.deepStrictEqual(bodyText, 'Much different');
            });
    });
});
