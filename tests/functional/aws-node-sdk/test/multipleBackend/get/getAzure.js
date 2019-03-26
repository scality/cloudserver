const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');

const {
    describeSkipIfNotMultipleOrCeph,
    uniqName,
    getAzureClient,
    getAzureContainerName,
    getAzureKeys,
    azureLocation,
} = require('../utils');

const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName(azureLocation);
const keys = getAzureKeys();
const keyObject = 'getazure';

const normalBody = Buffer.from('I am a body', 'utf8');

const azureTimeout = 10000;

describeSkipIfNotMultipleOrCeph('Multiple backend get object from Azure',
function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeAll(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: azureContainerName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterAll(() => {
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
            describe(`${key.describe} size`, () => {
                const testKey = `${key.name}-${Date.now()}`;
                beforeAll(done => {
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

                test(`should get an ${key.describe} object from Azure`, done => {
                    s3.getObject({ Bucket: azureContainerName, Key:
                      testKey },
                        (err, res) => {
                            expect(err).toEqual(null);
                            expect(res.ETag).toBe(`"${key.MD5}"`);
                            done();
                        });
                });
            });
        });

        describe('with range', () => {
            const azureObject = uniqName(keyObject);
            beforeAll(done => {
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Body: '0123456789',
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, done);
            });

            test('should get an object with body 012345 with "bytes=0-5"', done => {
                s3.getObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Range: 'bytes=0-5',
                }, (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ContentLength).toEqual(6);
                    expect(res.ContentRange).toBe('bytes 0-5/10');
                    expect(res.Body.toString()).toBe('012345');
                    done();
                });
            });
            test('should get an object with body 456789 with "bytes=4-"', done => {
                s3.getObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Range: 'bytes=4-',
                }, (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ContentLength).toEqual(6);
                    expect(res.ContentRange).toBe('bytes 4-9/10');
                    expect(res.Body.toString()).toBe('456789');
                    done();
                });
            });
        });

        describe('returning error', () => {
            const azureObject = uniqName(keyObject);
            beforeAll(done => {
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, err => {
                    expect(err).toEqual(null);
                    azureClient.deleteBlob(azureContainerName, azureObject,
                    err => {
                        expect(err).toEqual(null);
                        done(err);
                    });
                });
            });

            test('should return an error on get done to object deleted ' +
            'from Azure', done => {
                s3.getObject({
                    Bucket: azureContainerName,
                    Key: azureObject,
                }, err => {
                    expect(err.code).toBe('ServiceUnavailable');
                    done();
                });
            });
        });
    });
});
