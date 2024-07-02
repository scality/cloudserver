const xml2js = require('xml2js');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const FormData = require('form-data');
const assert = require('assert');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');

const filename = 'test-file.txt';
const region = 'us-east-1';
let ak;
let sk;
let s3;

const generateBucketName = () => `test-bucket-${crypto.randomBytes(8).toString('hex')}`;

const formatDate = (date) => {
    const year = date.getUTCFullYear();
    const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
    const day = date.getUTCDate().toString().padStart(2, '0');
    return `${year}${month}${day}`;
};

const getSignatureKey = (key, dateStamp, regionName, serviceName) => {
    const kDate = crypto.createHmac('sha256', `AWS4${key}`).update(dateStamp).digest();
    const kRegion = crypto.createHmac('sha256', kDate).update(regionName).digest();
    const kService = crypto.createHmac('sha256', kRegion).update(serviceName).digest();
    const kSigning = crypto.createHmac('sha256', kService).update('aws4_request').digest();
    return kSigning;
};

const calculateFields = (ak, sk, bucketName, additionalConditions) => {
    const service = 's3';

    const now = new Date();
    const formattedDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
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
            const index = conditionsFields.findIndex(condition => condition.hasOwnProperty(key));
            if (index !== -1) {
                conditionsFields[index][key] = value;
            } else {
                conditionsFields.push({ [key]: value });
            }
        });
    }
    const policy = {
        expiration: new Date(new Date().getTime() + 60000).toISOString(),
        conditions: conditionsFields,
    };
    const policyBase64 = Buffer.from(JSON.stringify(policy)).toString('base64');

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
            const index = returnFields.findIndex(f => f.name === key);
            if (index !== -1) {
                returnFields[index].value = value;
            } else {
                returnFields.push({ name: key, value });
            }
        });
    }
    return returnFields;
};


describe('POST object', () => {
    let bucketUtil;
    let config;
    const testContext = {};

    before(() => {
        config = getConfig('default');
        ak = config.credentials.accessKeyId;
        sk = config.credentials.secretAccessKey;
        bucketUtil = new BucketUtility('default');
        s3 = bucketUtil.s3;
    });

    beforeEach(done => {
        const bucketName = generateBucketName();
        const url = `${config.endpoint}/${bucketName}`;
        testContext.bucketName = bucketName;
        testContext.url = url;

        const filePath = path.join(__dirname, filename);
        const fileContent = 'This is a test file';
        fs.writeFile(filePath, fileContent, err => {
            if (err) {
                return done(err);
            }

            // Create the bucket
            return s3.createBucket({ Bucket: bucketName }, async (err) => {
                if (err) {
                    return done(err);
                }
                return done();
            });
        });
    });

    afterEach(() => {
        const { bucketName } = testContext;
        const filePath = path.join(__dirname, filename);

        // Delete the file
        fs.unlink(filePath, err => {
            if (err) {
                throw err;
            }

            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucketName)
                .then(() => {
                    process.stdout.write('Deleting bucket');
                    return bucketUtil.deleteOne(bucketName);
                })
                .catch(err => {
                    if (err.code !== 'NoSuchBucket') {
                        process.stdout.write('Error in afterEach');
                        throw err;
                    }
                });
        });
    });

    it('should successfully upload an object using a POST form', done => {
        const { bucketName, url } = testContext;
        const fields = calculateFields(ak, sk, bucketName);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(path.join(__dirname, filename)));

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            return axios.post(url, formData, {
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

    it('should handle error when bucket does not exist', done => {
        const fakeBucketName = generateBucketName();
        const tempUrl = `${config.endpoint}/${fakeBucketName}`;
        const fields = calculateFields(ak, sk, fakeBucketName);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(path.join(__dirname, filename)));

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            return axios.post(tempUrl, formData, {
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

    it('should successfully upload a larger file to S3 using a POST form', done => {
        const { bucketName, url } = testContext;
        const largeFileName = 'large-test-file.txt';
        const largeFilePath = path.join(__dirname, largeFileName);
        const largeFileContent = 'This is a larger test file'.repeat(10000); // Simulate a larger file

        fs.writeFile(largeFilePath, largeFileContent, err => {
            if (err) {
                return done(err);
            }

            const fields = calculateFields(ak, sk, bucketName, [{ key: largeFileName }]);
            const formData = new FormData();

            fields.forEach(field => {
                formData.append(field.name, field.value);
            });

            formData.append('file', fs.createReadStream(largeFilePath));

            return formData.getLength((err, length) => {
                if (err) {
                    return done(err);
                }

                return axios.post(url, formData, {
                    headers: {
                        ...formData.getHeaders(),
                        'Content-Length': length,
                    },
                })
                    .then(response => {
                        assert.equal(response.status, 204);
                        s3.listObjectsV2({ Bucket: bucketName }, (err, data) => {
                            if (err) {
                                fs.unlink(largeFilePath, () => done(err)); // Clean up and propagate the error
                                return;
                            }

                            const uploadedFile = data.Contents.find(item => item.Key === path.basename(largeFileName));
                            assert(uploadedFile, 'Uploaded file should exist in the bucket');
                            assert.equal(uploadedFile.Size, Buffer.byteLength(largeFileContent),
                                'File size should match');

                            fs.unlink(largeFilePath, done); // Clean up the large file
                        });
                    })
                    .catch(err => {
                        fs.unlink(largeFilePath, () => done(err)); // Clean up and propagate the error
                    });
            });
        });
    });

    it('should be able to post an empty file and verify its existence', done => {
        const { bucketName, url } = testContext;
        const emptyFilePath = path.join(__dirname, 'empty-file.txt');

        // Create an empty file
        fs.writeFile(emptyFilePath, '', err => {
            if (err) {
                return done(err);
            }

            const fields = calculateFields(ak, sk, bucketName);
            const formData = new FormData();

            fields.forEach(field => {
                formData.append(field.name, field.value);
            });

            formData.append('file', fs.createReadStream(emptyFilePath));

            return formData.getLength((err, length) => {
                if (err) {
                    return done(err);
                }

                return axios.post(url, formData, {
                    headers: {
                        ...formData.getHeaders(),
                        'Content-Length': length,
                    },
                })
                    .then(response => {
                        assert.equal(response.status, 204);

                        // Check if the object exists using listObjects
                        return s3.listObjects({ Bucket: bucketName, Prefix: filename }, (err, data) => {
                            if (err) {
                                return done(err);
                            }

                            const fileExists = data.Contents.some(item => item.Key === filename);

                            const file = data.Contents.find(item => item.Key === filename);
                            assert.equal(file.Size, 0);

                            if (!fileExists) {
                                return done(new Error('File does not exist in S3'));
                            }

                            // Clean up: delete the empty file locally and from S3
                            return fs.unlink(emptyFilePath, err => {
                                if (err) {
                                    return done(err);
                                }

                                return s3.deleteObject({ Bucket: bucketName, Key: filename }, err => {
                                    if (err) {
                                        return done(err);
                                    }

                                    return done();
                                });
                            });
                        });
                    })
                    .catch(err => {
                        done(err);
                    });
            });
        });
    });

    it('should handle error when file is missing', done => {
        const { bucketName, url } = testContext;
        const fields = calculateFields(ak, sk, bucketName);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            return axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(() => {
                    done(new Error('Expected error but got success response'));
                })
                .catch(err => {
                    assert.equal(err.response.status, 400);
                    done();
                });
        });
    });

    it('should upload an object with key slash', done => {
        const { bucketName, url } = testContext;
        const slashKey = '/';
        const fields = calculateFields(ak, sk, bucketName, [{ key: slashKey }]);

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(path.join(__dirname, filename)));

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            return axios.post(url, formData, {
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

    it('should return an error if form data (excluding file) exceeds 20KB', done => {
        const { bucketName, url } = testContext;
        const fields = calculateFields(ak, sk, bucketName);

        // Add additional fields to make form data exceed 20KB
        const largeValue = 'A'.repeat(1024); // 1KB value
        for (let i = 0; i < 21; i++) { // Add 21 fields of 1KB each to exceed 20KB
            fields.push({ name: `field${i}`, value: largeValue });
        }

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(path.join(__dirname, filename)));

        return formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            return axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(() => {
                    done(new Error('Request should not succeed with form data exceeding 20KB'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    // Parse the XML error response
                    xml2js.parseString(err.response.data, (err, result) => {
                        if (err) {
                            return done(err);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'MaxPostPreDataLengthExceeded');
                        assert.equal(error.Message[0],
                            'Your POST request fields preceeding the upload file was too large.');
                        return done();
                    });
                });
        });
    });

    it('should successfully upload an object with bucket versioning enabled and verify version ID', done => {
        const { url, bucketName } = testContext;

        // Enable versioning on the bucket
        const versioningParams = {
            Bucket: bucketName,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };

        return s3.putBucketVersioning(versioningParams, (err) => {
            if (err) {
                return done(err);
            }

            const fields = calculateFields(ak, sk, bucketName, [{ bucket: bucketName }]);
            const formData = new FormData();

            fields.forEach(field => {
                formData.append(field.name, field.value);
            });

            formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

            return formData.getLength((err, length) => {
                if (err) {
                    return done(err);
                }

                return axios.post(url, formData, {
                    headers: {
                        ...formData.getHeaders(),
                        'Content-Length': length,
                    },
                })
                    .then(response => {
                        assert.equal(response.status, 204);

                        // Verify version ID is present in the response
                        const versionId = response.headers['x-amz-version-id'];
                        assert.ok(versionId, 'Version ID should be present in the response headers');
                        done();
                    })
                    .catch(err => {
                        done(err);
                    });
            });
        });
    });

    it('should handle error when signature is invalid', done => {
        const { url, bucketName } = testContext;
        const fields = calculateFields(ak, sk, bucketName);
        fields.push({ name: 'X-Amz-Signature', value: 'invalid-signature' });
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
                .then(() => {
                    done(new Error('Expected error but got success response'));
                })
                .catch(err => {
                    assert.equal(err.response.status, 403);
                    done();
                });
        });
    });

    it('should return an error when signature includes invalid data', done => {
        const { url, bucketName } = testContext;
        let fields = calculateFields(ak, sk, bucketName);
        const laterThanNow = new Date(new Date().getTime() + 60000);
        const shortFormattedDate = formatDate(laterThanNow);

        const signingKey = getSignatureKey(sk, shortFormattedDate, 'ap-east-1', 's3');
        const signature = crypto.createHmac('sha256', signingKey).update(fields.find(field =>
            field.name === 'Policy').value).digest('hex');

        // Modify the signature to be invalid
        fields = fields.map(field => {
            if (field.name === 'X-Amz-Signature') {
                return { name: 'X-Amz-Signature', value: signature };
            }
            return field;
        });

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
                .then(() => {
                    done(new Error('Request should not succeed with an invalid signature'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    // Parse the XML error response
                    xml2js.parseString(err.response.data, (parseErr, result) => {
                        if (parseErr) {
                            return done(parseErr);
                        }

                        const error = result.Error;
                        assert.equal(
                            error.Code[0],
                            'SignatureDoesNotMatch',
                            'Expected SignatureDoesNotMatch error code'
                        );
                        done();
                    });
                });
        });
    });

    it('should return an error for invalid keys', done => {
        const { url, bucketName } = testContext;
        const invalidAccessKeyId = 'INVALIDACCESSKEY';
        const invalidSecretAccessKey = 'INVALIDSECRETKEY';
        let fields = calculateFields(invalidAccessKeyId, invalidSecretAccessKey, bucketName);

        // Modify the signature to be invalid
        fields = fields.map(field => {
            if (field.name === 'X-Amz-Signature') {
                return { name: 'X-Amz-Signature', value: 'invalid-signature' };
            }
            return field;
        });

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
                .then(() => {
                    done(new Error('Request should not succeed with an invalid keys'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    // Parse the XML error response
                    xml2js.parseString(err.response.data, (parseErr, result) => {
                        if (parseErr) {
                            return done(parseErr);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'InvalidAccessKeyId', 'Expected InvalidAccessKeyId error code');
                        done();
                    });
                });
        });
    });

    it('should return an error for invalid credential', done => {
        const { url, bucketName } = testContext;
        let fields = calculateFields(ak, sk, bucketName);
        const laterThanNow = new Date(new Date().getTime() + 60000);
        const shortFormattedDate = formatDate(laterThanNow);

        const credential = `${ak}/${shortFormattedDate}/ap-east-1/s3/aws4_request`;

        // Modify the signature to be invalid
        fields = fields.map(field => {
            if (field.name === 'X-Amz-Credential') {
                return { name: 'X-Amz-Credential', value: credential };
            }
            return field;
        });

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
                .then(() => {
                    done(new Error('Request should not succeed with an invalid credential'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    // Parse the XML error response
                    xml2js.parseString(err.response.data, (parseErr, result) => {
                        if (parseErr) {
                            return done(parseErr);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'InvalidArgument', 'Expected InvalidArgument error code');
                        done();
                    });
                });
        });
    });
});

