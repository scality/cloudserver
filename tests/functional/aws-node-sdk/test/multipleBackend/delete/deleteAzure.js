const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');
const { config } = require('../../../../../../lib/Config');
const { uniqName, getAzureClient, getAzureContainerName, getAzureKeys } =
  require('../utils');

const keyObject = 'deleteazure';
const azureLocation = 'azuretest';
const azureContainerName = getAzureContainerName();
const keys = getAzureKeys();
const azureClient = getAzureClient();

const normalBody = Buffer.from('I am a body', 'utf8');
const azureTimeout = 20000;


const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

describeSkipIfNotMultiple('Multiple backend delete object from Azure',
function testSuite() {
    this.timeout(250000);
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
            const keyName = uniqName(keyObject);
            describe(`${key.describe} size`, () => {
                before(done => {
                    s3.putObject({
                        Bucket: azureContainerName,
                        Key: keyName,
                        Body: key.body,
                        Metadata: {
                            'scal-location-constraint': azureLocation,
                        },
                    }, done);
                });

                it(`should delete an ${key.describe} object from Azure`,
                done => {
                    s3.deleteObject({
                        Bucket: azureContainerName,
                        Key: keyName,
                    }, err => {
                        assert.equal(err, null, 'Expected success ' +
                            `but got error ${err}`);
                        setTimeout(() =>
                        azureClient.getBlobProperties(azureContainerName,
                        keyName, err => {
                            assert.strictEqual(err.statusCode, 404);
                            assert.strictEqual(err.code, 'NotFound');
                            return done();
                        }), azureTimeout);
                    });
                });
            });
        });
        describe('returning no error', () => {
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

            it('should return no error on deleting an object deleted ' +
            'from Azure', done => {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                }, err => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error ${err}`);
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
                }, done);
            });

            it('should return error on deleting an object with a version ID',
            done => {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    VersionId: 'versionid',
                }, err => {
                    assert.strictEqual(err.statusCode, 400);
                    assert.strictEqual(err.code, 'InvalidArgument');
                    done();
                });
            });
        });
    });
});
