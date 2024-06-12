const AWS = require('aws-sdk');

const axios = require('axios');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const FormData = require('form-data');
const assert = require('assert');

// const filename = 'test-file.txt';
// const bucketName = 'your-bucket-name';
// const url = `http://localhost:8000/${bucketName}/`;

const generateBucketName = () => `test-bucket-${crypto.randomBytes(8).toString('hex')}`;
let bucketName;
const filename = 'test-file.txt';

let url;
const s3 = new AWS.S3();

const calculateFields = (additionalConditions) => {
    const ak = process.env.AWS_ACCESS_KEY_ID;
    const sk = process.env.AWS_SECRET_ACCESS_KEY;
    const region = 'us-east-1';
    const service = 's3';

    const now = new Date();
    const formattedDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
    const formatDate = (date) => {
        const year = date.getUTCFullYear();
        const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
        const day = date.getUTCDate().toString().padStart(2, '0');
        return `${year}${month}${day}`;
    };
    const shortFormattedDate = formatDate(now);

    const credential = `${ak}/${shortFormattedDate}/${region}/${service}/aws4_request`;
    const conditionsFields = [
        { bucket: bucketName },
        { key: filename },
        { 'x-amz-credential': credential },
        { 'x-amz-algorithm': 'AWS4-HMAC-SHA256' },
        { 'x-amz-date': formattedDate },
    ];
    if (additionalConditions) {
        additionalConditions.forEach(field => {
            const key = Object.keys(field)[0];
            const value = field[key];
            conditionsFields.push({ [key]: value });
        });
    }
    const policy = {
        expiration: new Date(new Date().getTime() + 60000).toISOString(),
        conditions: conditionsFields,
    };
    const policyBase64 = Buffer.from(JSON.stringify(policy)).toString('base64');

    const getSignatureKey = (key, dateStamp, regionName, serviceName) => {
        const kDate = crypto.createHmac('sha256', `AWS4${key}`).update(dateStamp).digest();
        const kRegion = crypto.createHmac('sha256', kDate).update(regionName).digest();
        const kService = crypto.createHmac('sha256', kRegion).update(serviceName).digest();
        const kSigning = crypto.createHmac('sha256', kService).update('aws4_request').digest();
        return kSigning;
    };
    const signingKey = getSignatureKey(sk, shortFormattedDate, region, service);
    const signature = crypto.createHmac('sha256', signingKey).update(policyBase64).digest('hex');

    const returnFields = [
        { name: 'X-Amz-Credential', value: credential },
        { name: 'X-Amz-Algorithm', value: 'AWS4-HMAC-SHA256' },
        { name: 'X-Amz-Signature', value: signature },
        { name: 'X-Amz-Date', value: formattedDate },
        { name: 'Policy', value: policyBase64 },
        { name: 'bucket', value: bucketName },
        { name: 'key', value: filename },
    ];
    if (!additionalConditions) {
        return returnFields;
    }
    if (additionalConditions) {
        additionalConditions.forEach(field => {
            const key = Object.keys(field)[0];
            const value = field[key];
            returnFields.push({ name: key, value: value });
        });
    }

    return returnFields;
};

describe('AWS S3 POST Object with Policy', function () {
    this.timeout(10000); // Increase timeout for potentially slow operations

    beforeEach(done => {
        bucketName = generateBucketName();
        url = `https://${bucketName}.s3.amazonaws.com/`;
        const filePath = path.join(__dirname, 'test-file.txt');
        const fileContent = 'This is a test file';

        fs.writeFile(filePath, fileContent, err => {
            if (err) {
                return done(err);
            }

            // Create the bucket
            s3.createBucket({ Bucket: bucketName }, (err) => {
                if (err) {
                    return done(err);
                }
                done();
            });
        });
    });

    afterEach(done => {
        const filePath = path.join(__dirname, 'test-file.txt');

        // Delete the file
        fs.unlink(filePath, err => {
            if (err) {
                return done(err);
            }

            // Delete the bucket and its contents
            const deleteBucket = () => {
                s3.deleteBucket({ Bucket: bucketName }, (err) => {
                    if (err && err.code !== 'NoSuchBucket') {
                        return done(err);
                    }
                    done();
                });
            };

            // List objects in the bucket
            s3.listObjects({ Bucket: bucketName }, (err, data) => {
                if (err && err.code === 'NoSuchBucket') {
                    return done(); // Ignore the error if the bucket does not exist
                } else if (err) {
                    return done(err);
                }

                if (data.Contents.length === 0) {
                    // Bucket is already empty
                    return deleteBucket();
                }

                // Delete all objects in the bucket
                const objects = data.Contents.map(item => ({ Key: item.Key }));
                s3.deleteObjects({
                    Bucket: bucketName,
                    Delete: { Objects: objects }
                }, (err) => {
                    if (err) {
                        return done(err);
                    }
                    deleteBucket();
                });
            });
        });
    });

    it('should successfully upload an object to S3 using a POST form', done => {
        const fields = calculateFields();
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(response => {
                    assert.equal(response.status, 204);
                    done();
                })
                .catch(err => {
                    done(err);
                });
        });
    });

    it('should successfully upload a larger file to S3 using a POST form', done => {
        const largeFilePath = path.join(__dirname, 'large-test-file.txt');
        const largeFileContent = 'This is a larger test file'.repeat(10000); // Simulate a larger file

        fs.writeFile(largeFilePath, largeFileContent, err => {
            if (err) {
                return done(err);
            }

            const fields = calculateFields();
            const formData = new FormData();

            fields.forEach(field => {
                formData.append(field.name, field.value);
            });

            formData.append('file', fs.createReadStream(largeFilePath));

            formData.getLength((err, length) => {
                if (err) {
                    return done(err);
                }

                axios.post(url, formData, {
                    headers: {
                        ...formData.getHeaders(),
                        'Content-Length': length,
                    },
                })
                    .then(response => {
                        assert.equal(response.status, 204);
                        fs.unlink(largeFilePath, done); // Clean up the large file
                    })
                    .catch(err => {
                        fs.unlink(largeFilePath, () => done(err)); // Clean up and propagate the error
                    });
            });
        });
    });

    it('should handle error when bucket does not exist', done => {
        bucketName = generateBucketName();
        const tempUrl = `https://${bucketName}.s3.amazonaws.com/`;
        const fields = calculateFields();
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            axios.post(tempUrl, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(() => {
                    done(new Error('Expected error but got success response'));
                })
                .catch(err => {

                    assert.equal(err.response.status, 404);
                    done();
                });
        });
    });

    it('should upload an object with additional metadata', done => {
        const additionalConditions = [
            { 'x-amz-meta-test-meta': 'test-value' },
        ];
        const fields = calculateFields(additionalConditions);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(response => {
                    assert.equal(response.status, 204);

                    // Verify metadata
                    s3.headObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(data.Metadata['test-meta'], 'test-value');
                        done();
                    });
                })
                .catch(err => {
                    done(err);
                });
        });
    });
});
        });

        it('should return InvalidRedirectLocation if putting object ' +
        'with x-amz-website-redirect-location header that does not start ' +
        'with \'http://\', \'https://\' or \'/\'', done => {
            const params = { Bucket: bucket, Key: 'key',
                WebsiteRedirectLocation: 'google.com' };
            s3.putObject(params, err => {
                assert.strictEqual(err.code, 'InvalidRedirectLocation');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });

        describe('Put object with tag set', () => {
            taggingTests.forEach(taggingTest => {
                it(taggingTest.it, done => {
                    const key = encodeURIComponent(taggingTest.tag.key);
                    const value = encodeURIComponent(taggingTest.tag.value);
                    const tagging = `${key}=${value}`;
                    const params = { Bucket: bucket, Key: object,
                        Tagging: tagging };
                    s3.putObject(params, err => {
                        if (taggingTest.error) {
                            _checkError(err, taggingTest.error, 400);
                            return done();
                        }
                        assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        return s3.getObjectTagging({ Bucket: bucket,
                            Key: object }, (err, data) => {
                            assert.equal(err, null, 'Expected success, ' +
                            `got error ${JSON.stringify(err)}`);
                            assert.deepStrictEqual(data.TagSet[0], {
                                Key: taggingTest.tag.key,
                                Value: taggingTest.tag.value });
                            done();
                        });
                    });
                });
            });

            it('should be able to put object with 10 tags',
            done => {
                const taggingConfig = generateMultipleTagQuery(10);
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: taggingConfig }, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${JSON.stringify(err)}`);
                    done();
                });
            });

            it('should be able to put an empty Tag set', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '',
                }, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${JSON.stringify(err)}`);
                    done();
                });
            });

            it('should be able to put object with empty tags',
            done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '&&&&&&&&&&&&&&&&&key1=value1' }, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${JSON.stringify(err)}`);
                    done();
                });
            });

            it('should allow putting 50 tags', done => {
                const taggingConfig = generateMultipleTagQuery(50);
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: taggingConfig }, done);
            });

            it('should return BadRequest if putting more that 50 tags',
            done => {
                const taggingConfig = generateMultipleTagQuery(51);
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: taggingConfig }, err => {
                    _checkError(err, 'BadRequest', 400);
                    done();
                });
            });

            it('should return InvalidArgument if using the same key twice',
            done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: 'key1=value1&key1=value2' }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

            it('should return InvalidArgument if using the same key twice ' +
            'and empty tags', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '&&&&&&&&&&&&&&&&&key1=value1&key1=value2' },
                err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

            it('should return InvalidArgument if tag with no key', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '=value1',
                }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

            it('should return InvalidArgument putting object with ' +
            'bad encoded tags', done => {
                s3.putObject({ Bucket: bucket, Key: object, Tagging:
                'key1==value1' }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

            it('should return InvalidArgument putting object tag with ' +
            'invalid characters: %', done => {
                const value = 'value1%';
                s3.putObject({ Bucket: bucket, Key: object, Tagging:
                `key1=${value}` }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });
        });
    });
});

describe('PUT object with object lock', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({
                Bucket: bucket,
                ObjectLockEnabledForBucket: true,
            }).promise()
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

        it('should put object with valid object lock retention date and ' +
            'mode when object lock is enabled on the bucket', done => {
            const date = new Date(2050, 10, 10);
            const params = {
                Bucket: bucket,
                Key: 'key1',
                ObjectLockRetainUntilDate: date,
                ObjectLockMode: 'COMPLIANCE',
            };
            s3.putObject(params, (err, res) => {
                assert.ifError(err);
                changeObjectLock(
                    [{ bucket, key: 'key1', versionId: res.VersionId }], '', done);
            });
        });

        it('should put object with valid object lock retention date and ' +
            'mode when object lock is enabled on the bucket', done => {
            const date = new Date(2050, 10, 10);
            const params = {
                Bucket: bucket,
                Key: 'key2',
                ObjectLockRetainUntilDate: date,
                ObjectLockMode: 'GOVERNANCE',
            };
            s3.putObject(params, (err, res) => {
                assert.ifError(err);
                changeObjectLock(
                    [{ bucket, key: 'key2', versionId: res.VersionId }], '', done);
            });
        });

        it('should error with invalid object lock mode header', done => {
            const date = new Date(2050, 10, 10);
            const params = {
                Bucket: bucket,
                Key: 'key3',
                ObjectLockMode: 'Governance',
                ObjectLockRetainUntilDate: date,
            };
            s3.putObject(params, err => {
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.message, 'Unknown wormMode directive');
                done();
            });
        });

        it('should put object with valid legal hold status ON', done => {
            const params = {
                Bucket: bucket,
                Key: 'key4',
                ObjectLockLegalHoldStatus: 'ON',
            };
            s3.putObject(params, (err, res) => {
                assert.ifError(err);
                changeObjectLock(
                    [{ bucket, key: 'key4', versionId: res.VersionId }], '', done);
            });
        });

        it('should put object with valid legal hold status OFF', done => {
            const params = {
                Bucket: bucket,
                Key: 'key5',
                ObjectLockLegalHoldStatus: 'OFF',
            };
            s3.putObject(params, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should error with invalid legal hold status', done => {
            const params = {
                Bucket: bucket,
                Key: 'key6',
                ObjectLockLegalHoldStatus: 'on',
            };
            s3.putObject(params, err => {
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.message,
                    'Legal hold status must be one of "ON", "OFF"');
                done();
            });
        });

        it('should return error when object lock retain until date header is ' +
            'provided but object lock mode header is missing', done => {
            const date = new Date(2050, 10, 10);
            const params = {
                Bucket: bucket,
                Key: 'key7',
                ObjectLockRetainUntilDate: date,
            };
            s3.putObject(params, err => {
                const expectedErrMessage
                    = 'x-amz-object-lock-retain-until-date and ' +
                    'x-amz-object-lock-mode must both be supplied';
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.message, expectedErrMessage);
                done();
            });
        });

        it('should return error when object lock mode header is provided ' +
            'but object lock retain until date header is missing', done => {
            const params = {
                Bucket: bucket,
                Key: 'key8',
                ObjectLockMode: 'GOVERNANCE',
            };
            s3.putObject(params, err => {
                const expectedErrMessage
                    = 'x-amz-object-lock-retain-until-date and ' +
                    'x-amz-object-lock-mode must both be supplied';
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.message, expectedErrMessage);
                done();
            });
        });
    });
});
