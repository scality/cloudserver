const assert = require('assert');
const { CreateBucketCommand, PutObjectCommand, DeleteObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
    memLocation,
    fileLocation,
    awsLocation,
    awsLocationMismatch,
    genUniqID,
} = require('../utils');

const bucket = `deleteaws${genUniqID()}`;
const memObject = `memObject-${genUniqID()}`;
const fileObject = `fileObject-${genUniqID()}`;
const awsObject = `awsObject-${genUniqID()}`;
const emptyObject = `emptyObject-${genUniqID()}`;
const bigObject = `bigObject-${genUniqID()}`;
const mismatchObject = `mismatchOjbect-${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);

describeSkipIfNotMultiple('Multiple backend delete', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(async () => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            await s3.send(new CreateBucketCommand({ Bucket: bucket }));

            process.stdout.write('Putting object to mem\n');
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: memObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': memLocation },
                }),
            );

            process.stdout.write('Putting object to file\n');
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: fileObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation },
                }),
            );

            process.stdout.write('Putting object to AWS\n');
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: awsObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation },
                }),
            );

            process.stdout.write('Putting 0-byte object to AWS\n');
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: emptyObject,
                    Metadata: { 'scal-location-constraint': awsLocation },
                }),
            );

            process.stdout.write('Putting large object to AWS\n');
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: bigObject,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': awsLocation },
                }),
            );

            process.stdout.write('Putting object to AWS\n');
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: mismatchObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocationMismatch },
                }),
            );
        });

        after(async () => {
            process.stdout.write('Emptying bucket\n');
            await bucketUtil.empty(bucket);
            process.stdout.write('Deleting bucket\n');
            await bucketUtil.deleteOne(bucket);
        });

        it('should delete object from mem', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: memObject }));
            try {
                await s3.send(new GetObjectCommand({ Bucket: bucket, Key: memObject }));
                assert.fail('Expected NoSuchKey error but got success');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
            }
        });

        it('should delete object from file', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: fileObject }));

            try {
                await s3.send(new GetObjectCommand({ Bucket: bucket, Key: fileObject }));
                assert.fail('Expected NoSuchKey error but got success');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
            }
        });

        it('should delete an object from AWS', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: awsObject }));

            try {
                await s3.send(new GetObjectCommand({ Bucket: bucket, Key: awsObject }));
                assert.fail('Expected NoSuchKey error but got success');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
            }
        });

        it('should delete 0-byte object from AWS', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: emptyObject }));

            try {
                await s3.send(new GetObjectCommand({ Bucket: bucket, Key: emptyObject }));
                assert.fail('Expected NoSuchKey error but got success');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
            }
        });

        it('should delete large object from AWS', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: bigObject }));

            try {
                await s3.send(new GetObjectCommand({ Bucket: bucket, Key: bigObject }));
                assert.fail('Expected NoSuchKey error but got success');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
            }
        });

        it('should delete object from AWS location with bucketMatch set to ' + 'false', async () => {
            try {
                await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: mismatchObject }));
                await s3.send(
                    new GetObjectCommand({
                        Bucket: bucket,
                        Key: mismatchObject,
                    }),
                );
                assert.fail('Expected NoSuchKey error but got success');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
            }
        });
    });
});
