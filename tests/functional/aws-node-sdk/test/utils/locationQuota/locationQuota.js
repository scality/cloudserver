const assert = require('assert');
const async = require('async');
const { errors } = require('arsenal');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { getLocationMetric } = require('../../../../../../lib/utapi/utilities');
const { genUniqID } = require('../../multipleBackend/utils');
const DummyRequestLogger =
    require('../../../../../unit/helpers').DummyRequestLogger;

const bucket = `locationquota-testbucket-${genUniqID()}`;
const fileQuotaLocation = 'file-quota';
const cloudQuotaLocation = 'awsbackendquota';
const log = new DummyRequestLogger();
const bodySize = 10485760; // 10mb

describe.only('Location quota metric', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        describe('with file backend', () => {
            beforeEach(() => {
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
                return s3.createBucketAsync({ Bucket: bucket,
                    CreateBucketConfiguration: {
                        LocationConstraint: fileQuotaLocation,
                    },
                })
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    throw err;
                });
            });

            afterEach(() => {
                process.stdout.write('Emptying bucket');
                return bucketUtil.empty(bucket)
                .then(() => {
                    process.stdout.write('Deleting bucket');
                    return bucketUtil.deleteOne(bucket);
                })
                .catch(err => {
                    process.stdout.write('Error in afterEach');
                    throw err;
                });
            });

            it('should increment location metric counter on object put',
            done => {
                const key = `quota-key-${genUniqID()}`;
                s3.putObject({ Bucket: bucket, Key: key,
                Body: Buffer.alloc(bodySize) }, err => {
                    assert.equal(err, null, `Error putting object: ${err}\n`);
                    getLocationMetric(fileQuotaLocation, log,
                    (err, bytesStored) => {
                        assert.equal(err, null, 'Error getting location ' +
                            `metric: ${err}`);
                        assert.strictEqual(parseInt(bytesStored, 10), bodySize);
                        done();
                    });
                });
            });

            it('should increment location metric counter on object copy',
            done => {

            });

            it('should return error if quota will be exceeded on object put',
            done => {

            });

            it('should decrement location metric counter on object delete',
            done => {

            });

            it('should increment location metric counter on MPU part put',
            done => {

            });

            it('should increment location metric counter on MPU copy part put',
            done=> {

            });

            it('should decrement location metric counter on abort MPU',
            done => {

            });

        });

        describe('with cloud backend', () => {
            beforeEach(() => {
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
                return s3.createBucketAsync({ Bucket: bucket,
                    CreateBucketConfiguration: {
                        LocationConstraint: cloudQuotaLocation,
                    },
                })
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    throw err;
                });
            });

            afterEach(() => {
                process.stdout.write('Emptying bucket');
                return bucketUtil.empty(bucket)
                .then(() => {
                    process.stdout.write('Deleting bucket');
                    return bucketUtil.deleteOne(bucket);
                })
                .catch(err => {
                    process.stdout.write('Error in afterEach');
                    throw err;
                });
            });

        })
    });
});
