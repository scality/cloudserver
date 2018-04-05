const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');

const {
    describeSkipIfNotMultiple,
    uniqName,
    getAzureClient,
    getAzureContainerName,
    getAzureKeys,
    azureLocation,
} = require('../utils');

const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();
const keys = getAzureKeys();
const keyObject = 'getazure';

const normalBody = Buffer.from('I am a body', 'utf8');

const azureTimeout = 10000;

describeSkipIfNotMultiple('Multiple backend get object from Azure',
function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: azureContainerName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });
        keys.forEach(key => {
            describe(`${key.describe} size`, function fn() {
                // This test has been observed to be flaky, so allow the test to
                // fail three times consecutively before the suite itself fails.
                this.retries(2);
                const testKey = `${key.name}-${Date.now()}`;
                before(done => {
                    setTimeout(() => {
                        s3.putObject({
                            Bucket: azureContainerName,
                            Key: testKey,
                            Body: key.body,
                            Metadata: {
                                'scal-location-constraint': azureLocation,
                            },
                        }, done);
                    }, azureTimeout);
                });

                it(`should get an ${key.describe} object from Azure`, done => {
                    // Log the key name to help investigate potential flakiness.
                    process.stdout.write(`key: ${testKey}`);
                    s3.getObject({ Bucket: azureContainerName, Key:
                      testKey },
                        (err, res) => {
                            assert.equal(err, null, 'Expected success ' +
                                `but got error ${err}`);
                            assert.strictEqual(res.ETag, `"${key.MD5}"`);
                            done();
                        });
                });
            });
        });

        describe('with range', () => {
            const azureObject = uniqName(keyObject);
            before(done => {
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Body: '0123456789',
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, done);
            });

            it('should get an object with body 012345 with "bytes=0-5"',
            done => {
                s3.getObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Range: 'bytes=0-5',
                }, (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                      `error ${err}`);
                    assert.equal(res.ContentLength, 6);
                    assert.strictEqual(res.ContentRange, 'bytes 0-5/10');
                    assert.strictEqual(res.Body.toString(), '012345');
                    done();
                });
            });
            it('should get an object with body 456789 with "bytes=4-"',
            done => {
                s3.getObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Range: 'bytes=4-',
                }, (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                      `error ${err}`);
                    assert.equal(res.ContentLength, 6);
                    assert.strictEqual(res.ContentRange, 'bytes 4-9/10');
                    assert.strictEqual(res.Body.toString(), '456789');
                    done();
                });
            });
        });

        describe('returning error', () => {
            const azureObject = uniqName(keyObject);
            before(done => {
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, err => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error ${err}`);
                    azureClient.deleteBlob(azureContainerName, azureObject,
                    err => {
                        assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                        done(err);
                    });
                });
            });

            it('should return an error on get done to object deleted ' +
            'from Azure', done => {
                s3.getObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                }, err => {
                    assert.strictEqual(err.code, 'ServiceUnavailable');
                    done();
                });
            });
        });
    });
});
