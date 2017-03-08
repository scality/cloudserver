import assert from 'assert';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'buckettestmultiplebackenddelete';
const memObject = 'memObject';
const fileObject = 'fileObject';
const body = Buffer.from('I am a body', 'utf8');

describe('Multiple backend delete', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            })
            .then(() => {
                process.stdout.write('Putting object to mem\n');
                const params = { Bucket: bucket, Key: memObject, Body: body,
                    Metadata: { 'scal-location-constraint': 'mem' } };
                return s3.putObject(params);
            })
            .then(() => {
                process.stdout.write('Putting object to file\n');
                const params = { Bucket: bucket, Key: fileObject, Body: body,
                    Metadata: { 'scal-location-constraint': 'file' } };
                return s3.putObject(params);
            })
            .catch(err => {
                process.stdout.write(`Error putting objects: ${err}\n`);
                throw err;
            });
        });
        after(() => {
            process.stdout.write('Deleting bucket\n');
            return bucketUtil.deleteOne(bucket)
            .catch(err => {
                process.stdout.write(`Error deleting bucket: ${err}\n`);
                throw err;
            });
        });

        it('should delete object from mem', done => {
            s3.deleteObject({ Bucket: bucket, Key: memObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: memObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from file', done => {
            s3.deleteObject({ Bucket: bucket, Key: fileObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: fileObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got sucess');
                    done();
                });
            });
        });
    });
});
