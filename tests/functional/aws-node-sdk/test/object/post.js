
const xml2js = require('xml2js');
const axios = require('axios');
const crypto = require('crypto');
const FormData = require('form-data');
const assert = require('assert');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');

let bucketName;
const filename = 'test-file.txt';
let fileBuffer;
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

// 'additionalConditions' will also replace existing keys if they are present
const calculateFields = (ak, sk, additionalConditions, bucket = bucketName, key = filename) => {
    const service = 's3';

    const now = new Date();
    const formattedDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
    let shortFormattedDate = formatDate(now);

    const credential = `${ak}/${shortFormattedDate}/${region}/${service}/aws4_request`;
    const conditionsFields = [
        { bucket },
        { key },
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
                if (key === 'x-amz-date') {
                    shortFormattedDate = value.split('T')[0];
                }
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
        { name: 'x-amz-credential', value: credential },
        { name: 'x-amz-algorithm', value: 'AWS4-HMAC-SHA256' },
        { name: 'x-amz-signature', value: signature },
        { name: 'x-amz-date', value: formattedDate },
        { name: 'policy', value: policyBase64 },
        { name: 'bucket', value: bucket },
        { name: 'key', value: key },
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
        bucketName = generateBucketName();
        const url = `${config.endpoint}/${bucketName}`;
        testContext.bucketName = bucketName;
        testContext.url = url;

        const fileContent = 'This is a test file';
        fileBuffer = Buffer.from(fileContent);

        // Create the bucket
        s3.createBucket({ Bucket: bucketName }, err => {
            if (err) {
                return done(err);
            }
            return done();
        });
    });


    afterEach(done => {
        const { bucketName } = testContext;

        process.stdout.write('Emptying bucket\n');
        bucketUtil.empty(bucketName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucketName);
            })
            .then(() => done())
            .catch(err => {
                if (err.code !== 'NoSuchBucket') {
                    process.stdout.write('Error in afterEach\n');
                    return done(err);
                }
                return done();
            });
    });


    it('should successfully upload an object using a POST form', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, { filename });

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
                    assert.equal(response.headers.location, `/${bucketName}/${filename}`);
                    assert.equal(response.headers.key, filename);
                    assert.equal(response.headers.bucket, bucketName);
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
        const fields = calculateFields(ak, sk, [], fakeBucketName);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, { filename });

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
        const { url } = testContext;
        const largeFileName = 'large-test-file.txt';
        const largeFileContent = 'This is a larger test file'.repeat(10000); // Simulate a larger file
        const largeFileBuffer = Buffer.from(largeFileContent);

        const fields = calculateFields(ak, sk, [{ key: largeFileName }]);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', largeFileBuffer, { filename: largeFileName });

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
                    s3.listObjectsV2({ Bucket: bucketName }, (err, data) => {
                        if (err) {
                            return done(err);
                        }

                        const uploadedFile = data.Contents.find(item => item.Key === largeFileName);
                        assert(uploadedFile, 'Uploaded file should exist in the bucket');
                        assert.equal(uploadedFile.Size, Buffer.byteLength(largeFileContent), 'File size should match');

                        return done();
                    });
                })
                .catch(err => {
                    done(err);
                });
        });
    });

    it('should be able to post an empty file and verify its existence', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        const emptyFileBuffer = Buffer.from(''); // Create a buffer for an empty file

        formData.append('file', emptyFileBuffer, filename);

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

                    // Check if the object exists using listObjects
                    s3.listObjectsV2({ Bucket: bucketName, Prefix: filename }, (err, data) => {
                        if (err) {
                            return done(err);
                        }

                        const fileExists = data.Contents.some(item => item.Key === filename);
                        const file = data.Contents.find(item => item.Key === filename);

                        assert(fileExists, 'File should exist in S3');
                        assert.equal(file.Size, 0, 'File size should be 0');

                        // Clean up: delete the empty file from S3
                        return s3.deleteObject({ Bucket: bucketName, Key: filename }, err => {
                            if (err) {
                                return done(err);
                            }

                            return done();
                        });
                    });
                })
                .catch(err => {
                    done(err);
                });
        });
    });

    it('should handle error when file is missing', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk);
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
                    xml2js.parseString(err.response.data, (parseErr, result) => {
                        if (parseErr) {
                            return done(parseErr);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'InvalidArgument');
                        assert.equal(error.Message[0], 'POST requires exactly one file upload per request.');
                        return done();
                    });
                });
        });
    });

    it('should handle error when there are multiple files', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        // Append the same buffer twice to simulate multiple files
        formData.append('file', fileBuffer, { filename });
        formData.append('file', fileBuffer, { filename });

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
                    xml2js.parseString(err.response.data, (parseErr, result) => {
                        if (parseErr) {
                            return done(parseErr);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'InvalidArgument');
                        assert.equal(error.Message[0], 'POST requires exactly one file upload per request.');
                        return done();
                    });
                });
        });
    });


    it('should handle error when key is missing', done => {
        const { url } = testContext;
        // Prep fields then remove the key field
        let fields = calculateFields(ak, sk);
        fields = fields.filter(e => e.name !== 'key');

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        const fileContent = 'This is a test file';
        const fileBuffer = Buffer.from(fileContent);

        formData.append('file', fileBuffer, { filename });

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
                    done(new Error('Request should not succeed without key field'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    xml2js.parseString(err.response.data, (parseErr, result) => {
                        if (parseErr) {
                            return done(parseErr);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'InvalidArgument');
                        assert.equal(error.Message[0],
                            "Bucket POST must contain a field named 'key'.  "
                            + 'If it is specified, please check the order of the fields.');
                        return done();
                    });
                });
        });
    });

    it('should handle error when content-type is incorrect', done => {
        const { url } = testContext;
        // Prep fields then remove the key field
        let fields = calculateFields(ak, sk);
        fields = fields.filter(e => e.name !== 'key');

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            const headers = {
                ...formData.getHeaders(),
                'Content-Length': length,
            };
            headers['content-type'] = 'application/json';
            return axios.post(url, formData, {
                headers,
            })
                .then(() => {
                    done(new Error('Request should not succeed wrong content-type'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    xml2js.parseString(err.response.data, (err, result) => {
                        if (err) {
                            return done(err);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'PreconditionFailed');
                        assert.equal(error.Message[0],
                            'Bucket POST must be of the enclosure-type multipart/form-data');
                        return done();
                    });
                });
        });
    });

    it('should handle error when content-type is missing', done => {
        const { url } = testContext;
        // Prep fields then remove the key field
        let fields = calculateFields(ak, sk);
        fields = fields.filter(e => e.name !== 'key');

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            const headers = {
                ...formData.getHeaders(),
                'Content-Length': length,
            };
            delete headers['content-type'];
            return axios.post(url, formData, {
                headers,
            })
                .then(() => {
                    done(new Error('Request should not succeed without correct content-type'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    xml2js.parseString(err.response.data, (err, result) => {
                        if (err) {
                            return done(err);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'PreconditionFailed');
                        assert.equal(error.Message[0],
                            'Bucket POST must be of the enclosure-type multipart/form-data');
                        return done();
                    });
                });
        });
    });

    it('should upload an object with key slash', done => {
        const { url } = testContext;
        const slashKey = '/';
        const fields = calculateFields(ak, sk, [{ key: slashKey }]);

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

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

    it('should fail to upload an object with key length of 0', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk, [
            { key: '' },
        ]);

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            // Use an incorrect content length (e.g., actual length - 20)

            return axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(() => done(new Error('Request should have failed but succeeded')))
                .catch(err => {
                    // Expecting an error response from the API
                    assert.equal(err.response.status, 400);
                    xml2js.parseString(err.response.data, (err, result) => {
                        if (err) {
                            return done(err);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'InvalidArgument');
                        assert.equal(error.Message[0],
                            'User key must have a length greater than 0.');
                        return done();
                    });
                });
        });
    });

    it('should fail to upload an object with key longer than 1024 bytes', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk, [
            { key: 'a'.repeat(1025) },
        ]);

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            // Use an incorrect content length (e.g., actual length - 20)

            return axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(() => {
                    // The request should fail, so we shouldn't get here
                    done(new Error('Request should have failed but succeeded'));
                })
                .catch(err => {
                    // Expecting an error response from the API
                    assert.equal(err.response.status, 400);
                    xml2js.parseString(err.response.data, (err, result) => {
                        if (err) {
                            return done(err);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'KeyTooLong');
                        assert.equal(error.Message[0],
                            'Your key is too long.');
                        return done();
                    });
                });
        });
    });

    it('should replace ${filename} variable in key with the name of the uploaded file', done => {
        const { url } = testContext;
        const keyTemplate = 'uploads/test/${filename}';
        const fileToUpload = keyTemplate.replace('${filename}', filename);
        const fields = calculateFields(ak, sk, [{ key: fileToUpload }]);
        const formData = new FormData();

        fields.forEach(field => {
            const value = field.name === 'key' ? keyTemplate : field.value;
            formData.append(field.name, value);
        });

        formData.append('file', fileBuffer, filename);

        formData.getLength((err, length) => {
            if (err) return done(err);

            return axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(response => {
                    assert.equal(response.status, 204);
                    const expectedKey = keyTemplate.replace('${filename}', filename);

                    const listParams = { Bucket: bucketName, Prefix: expectedKey };
                    return s3.listObjects(listParams, (err, data) => {
                        if (err) return done(err);
                        const objectExists = data.Contents.some(item => item.Key === expectedKey);
                        assert(objectExists, 'Object was not uploaded with the expected key');
                        return done();
                    });
                })
                .catch(done);
        });
    });

    it('should fail to upload an object with an invalid multipart boundary', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk);

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

        // Generate the form data with a valid boundary
        const validBoundary = formData.getBoundary();

        // Manually create the payload with an invalid boundary
        const invalidBoundary = '----InvalidBoundary';
        const payload = Buffer.concat([
            Buffer.from(`--${invalidBoundary}\r\n`),
            Buffer.from(`Content-Disposition: form-data; name="key"\r\n\r\n${filename}\r\n`),
            Buffer.from(`--${invalidBoundary}\r\n`),
            Buffer.from(`Content-Disposition: form-data; name="file"; filename="${filename}"\r\n`),
            Buffer.from('Content-Type: application/octet-stream\r\n\r\n'),
            fileBuffer,
            Buffer.from(`\r\n--${invalidBoundary}--\r\n`),
        ]);

        // Create an axios instance with invalid headers
        axios.post(url, payload, {
            headers: {
                'Content-Type': `multipart/form-data; boundary=${validBoundary}`,
                'Content-Length': payload.length,
            },
        })
            .then(() => {
                // The request should fail, so we shouldn't get here
                done(new Error('Request should have failed but succeeded'));
            })
            .catch(err => {
                // Expecting an error response from the API
                assert.equal(err.response.status, 400);
                done();
            });
    });

    it('should fail to upload an object with an too small content length header', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk);

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

        formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            // Use an incorrect content length (e.g., actual length - 20)
            const incorrectLength = length - 20;

            return axios.post(url, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': incorrectLength,
                },
            })
                .then(() => done(new Error('Request should have failed but succeeded')))
                .catch(err => {
                    // Expecting an error response from the API
                    assert.equal(err.response.status, 400);
                    done();
                });
        });
    });

    it('should return an error if form data (excluding file) exceeds 20KB', done => {
        const { url } = testContext;
        const fields = calculateFields(ak, sk);

        // Add additional fields to make form data exceed 20KB
        const largeValue = 'A'.repeat(1024); // 1KB value
        for (let i = 0; i < 21; i++) { // Add 21 fields of 1KB each to exceed 20KB
            fields.push({ name: `field${i}`, value: largeValue });
        }

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

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

    it('should return an error if a query parameter is present in the URL', done => {
        const { url } = testContext;
        const queryParam = '?invalidParam=true';
        const invalidUrl = `${url}${queryParam}`;
        const fields = calculateFields(ak, sk);

        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fileBuffer, filename);

        return formData.getLength((err, length) => {
            if (err) {
                return done(err);
            }

            return axios.post(invalidUrl, formData, {
                headers: {
                    ...formData.getHeaders(),
                    'Content-Length': length,
                },
            })
                .then(() => {
                    done(new Error('Request should not succeed with an invalid query parameter'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    xml2js.parseString(err.response.data, (err, result) => {
                        if (err) {
                            return done(err);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'InvalidArgument');
                        assert.equal(error.Message[0], 'Query String Parameters not allowed on POST requests.');
                        return done();
                    });
                });
        });
    });

    it('should successfully upload an object with bucket versioning enabled and verify version ID', done => {
        const { url } = testContext;

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

            const fields = calculateFields(ak, sk, [{ bucket: bucketName }]);
            const formData = new FormData();

            fields.forEach(field => {
                formData.append(field.name, field.value);
            });

            formData.append('file', fileBuffer, filename);

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
});

