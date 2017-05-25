const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'testgetcorsbucket';

describe('GET bucket cors', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        afterEach(() => bucketUtil.deleteOne(bucketName));

        describe('on bucket with existing cors configuration', () => {
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

        describe('mixed case for AllowedHeader', () => {
            const testValue = 'tEsTvAlUe';
            const sampleCors = { CORSRules: [
              { AllowedMethods: ['PUT', 'POST', 'DELETE'],
                AllowedOrigins: ['http://www.example.com'],
                AllowedHeaders: [testValue] },
            ] };
            before(() =>
                s3.createBucketAsync({ Bucket: bucketName })
                .then(() => s3.putBucketCorsAsync({
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors,
                })));

            it('should be preserved when putting / getting cors resource',
            done => {
                s3.getBucketCors({ Bucket: bucketName }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    assert.deepStrictEqual(data.CORSRules[0].AllowedHeaders,
                        sampleCors.CORSRules[0].AllowedHeaders);
                    return done();
                });
            });
        });

        describe('uppercase for AllowedMethod', () => {
            const sampleCors = { CORSRules: [
              { AllowedMethods: ['PUT', 'POST', 'DELETE'],
                AllowedOrigins: ['http://www.example.com'] },
            ] };
            before(() =>
                s3.createBucketAsync({ Bucket: bucketName })
                .then(() => s3.putBucketCorsAsync({
                    Bucket: bucketName,
                    CORSConfiguration: sampleCors,
                })));

            it('should be preserved when retrieving cors resource',
            done => {
                s3.getBucketCors({ Bucket: bucketName }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    assert.deepStrictEqual(data.CORSRules[0].AllowedMethods,
                        sampleCors.CORSRules[0].AllowedMethods);
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
