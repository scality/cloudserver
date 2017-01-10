import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'testgetcorsbucket';
const sampleCors = { CORSRules: [
  { AllowedMethods: ['PUT', 'POST', 'DELETE'],
    AllowedOrigins: ['http://www.example.com'],
    AllowedHeaders: ['*'],
    MaxAgeSeconds: 3000,
    ExposeHeaders: ['x-amz-server-side-encryption'] },
  { AllowedMethods: ['GET'],
    AllowedOrigins: ['*'],
    ExposeHeaders: [],
    AllowedHeaders: ['*'],
    MaxAgeSeconds: 3000 },
] };

describe('GET bucket cors', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        afterEach(() => bucketUtil.deleteOne(bucketName));

        describe('on bucket with existing cors configuration', () => {
            before(() =>
                s3.createBucketAsync({ Bucket: bucketName })
                .then(() => s3.putBucketCorsAsync({
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors,
                })));

            it('should return cors configuration successfully', done => {
                s3.getBucketCors({ Bucket: bucketName }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    assert.deepStrictEqual(data.CORSRules,
                        sampleCors.CORSRules);
                    return done();
                });
            });
        });

        describe('on bucket without cors configuration', () => {
            before(done => {
                process.stdout.write('about to create bucket\n');
                s3.createBucket({ Bucket: bucketName }, err => {
                    if (err) {
                        process.stdout.write('error creating bucket', err);
                        return done(err);
                    }
                    return done();
                });
            });

            it('should return NoSuchCORSConfiguration', done => {
                s3.getBucketCors({ Bucket: bucketName }, err => {
                    assert(err);
                    assert.strictEqual(err.code, 'NoSuchCORSConfiguration');
                    assert.strictEqual(err.statusCode, 404);
                    return done();
                });
            });
        });
    });
});
