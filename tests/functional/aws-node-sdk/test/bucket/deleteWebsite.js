import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const bucketName = 'testdeletewebsitebucket';

describe('DELETE bucket website', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        describe('without existing bucket', () => {
            it('should return NoSuchBucket', done => {
                s3.deleteBucketWebsite({ Bucket: bucketName }, err => {
                    assert(err);
                    assert.strictEqual(err.code, 'NoSuchBucket');
                    assert.strictEqual(err.statusCode, 404);
                    return done();
                });
            });
        });

        describe('with existing bucket', () => {
            beforeEach(() => s3.createBucketAsync({ Bucket: bucketName }));
            afterEach(() => bucketUtil.deleteOne(bucketName));

            describe('without existing configuration', () => {
                it('should return a 204 response', done => {
                    const request =
                    s3.deleteBucketWebsite({ Bucket: bucketName }, err => {
                        const statusCode =
                            request.response.httpResponse.statusCode;
                        assert.strictEqual(statusCode, 204,
                            `Found unexpected statusCode ${statusCode}`);
                        assert.strictEqual(err, null,
                            `Found unexpected err ${err}`);
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

                it('should delete bucket configuration successfully', done => {
                    s3.deleteBucketWebsite({ Bucket: bucketName }, err => {
                        assert.strictEqual(err, null,
                            `Found unexpected err ${err}`);
                        return done();
                    });
                });

                it('should return AccessDenied if user is not bucket owner',
                done => {
                    otherAccountS3.deleteBucketWebsite({ Bucket: bucketName },
                    err => {
                        assert(err);
                        assert.strictEqual(err.code, 'AccessDenied');
                        assert.strictEqual(err.statusCode, 403);
                        return done();
                    });
                });
            });
        });
    });
});
