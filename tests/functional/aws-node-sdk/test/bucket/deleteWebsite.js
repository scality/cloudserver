const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const bucketName = 'testdeletewebsitebucket';

describe('DELETE bucket website', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        describe('without existing bucket', () => {
            test('should return NoSuchBucket', done => {
                s3.deleteBucketWebsite({ Bucket: bucketName }, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('NoSuchBucket');
                    expect(err.statusCode).toBe(404);
                    return done();
                });
            });
        });

        describe('with existing bucket', () => {
            beforeEach(() => s3.createBucketAsync({ Bucket: bucketName }));
            afterEach(() => bucketUtil.deleteOne(bucketName));

            describe('without existing configuration', () => {
                test('should return a 204 response', done => {
                    const request =
                    s3.deleteBucketWebsite({ Bucket: bucketName }, err => {
                        const statusCode =
                            request.response.httpResponse.statusCode;
                        expect(statusCode).toBe(204);
                        expect(err).toBe(null);
                        return done();
                    });
                });
            });

            describe('with existing configuration', () => {
                beforeEach(done => {
                    const config = new WebsiteConfigTester('index.html');
                    s3.putBucketWebsite({ Bucket: bucketName,
                        WebsiteConfiguration: config }, done);
                });

                test('should delete bucket configuration successfully', done => {
                    s3.deleteBucketWebsite({ Bucket: bucketName }, err => {
                        expect(err).toBe(null);
                        return done();
                    });
                });

                test('should return AccessDenied if user is not bucket owner', done => {
                    otherAccountS3.deleteBucketWebsite({ Bucket: bucketName },
                    err => {
                        expect(err).toBeTruthy();
                        expect(err.code).toBe('AccessDenied');
                        expect(err.statusCode).toBe(403);
                        return done();
                    });
                });
            });
        });
    });
});
