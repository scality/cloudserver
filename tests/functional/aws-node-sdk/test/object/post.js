const AWS = require('aws-sdk');

const axios = require('axios');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const FormData = require('form-data');
const assert = require('assert');
const xml2js = require('xml2js');


// const filename = 'test-file.txt';
// const bucketName = 'your-bucket-name';
// const url = `http://localhost:8000/${bucketName}/`;

const generateBucketName = () => `test-bucket-${crypto.randomBytes(8).toString('hex')}`;
const filename = 'test-file.txt';
const region = 'us-east-1';
const ak = process.env.AWS_ACCESS_KEY_ID;
const sk = process.env.AWS_SECRET_ACCESS_KEY;

const s3 = new AWS.S3({
    accessKeyId: ak,
    secretAccessKey: sk,
    region: 'us-east-1',
});

let url;
let bucketName;


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

const calculateFields = (ak, sk, additionalConditions) => {
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
                returnFields.push({ name: key, value: value });
            }
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

            // Function to delete a single bucket and its contents
            const deleteSingleBucket = (bucket, callback) => {
                const deleteBucket = () => {
                    s3.deleteBucket({ Bucket: bucket }, (err) => {
                        if (err && err.code !== 'NoSuchBucket') {
                            return callback(err);
                        }
                        callback();
                    });
                };

                s3.listObjects({ Bucket: bucket }, (err, data) => {
                    if (err && err.code === 'NoSuchBucket') {
                        return callback(); // Ignore the error if the bucket does not exist
                    } else if (err) {
                        return callback(err);
                    }

                    if (data.Contents.length === 0) {
                        // Bucket is already empty
                        return deleteBucket();
                    }

                    // Delete all objects in the bucket, including objects locked with governance
                    const objects = data.Contents.map(item => ({ Key: item.Key }));
                    const deleteParams = {
                        Bucket: bucket,
                        Delete: { Objects: objects },
                        BypassGovernanceRetention: true // Bypass governance mode
                    };
                    s3.deleteObjects(deleteParams, (err) => {
                        if (err) {
                            return callback(err);
                        }
                        deleteBucket();
                    });
                });
            };

            // List all buckets
            s3.listBuckets((err, data) => {
                if (err) {
                    return done(err);
                }

                // Filter buckets that start with the specified prefix
                const bucketsToDelete = data.Buckets.filter(bucket => bucket.Name.startsWith(bucketName));

                // Delete each bucket and its contents
                let completed = 0;
                const total = bucketsToDelete.length;
                const checkDone = (err) => {
                    if (err) {
                        return done(err);
                    }
                    completed += 1;
                    if (completed === total) {
                        done();
                    }
                };

                if (total === 0) {
                    done();
                } else {
                    bucketsToDelete.forEach(bucket => deleteSingleBucket(bucket.Name, checkDone));
                }
            });
        });
    });


    it('should successfully upload an object to S3 using a POST form', done => {
        const fields = calculateFields(ak, sk);
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

            const fields = calculateFields(ak, sk);
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
        const fields = calculateFields(ak, sk);
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
        const fields = calculateFields(ak, sk, additionalConditions);
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

    it('should handle error when policy is invalid', done => {
        const fields = calculateFields(ak, sk);
        fields.push({ name: 'Policy', value: 'invalid-policy' });
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

    it('should handle error when signature is invalid', done => {
        const fields = calculateFields(ak, sk);
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

    it('should be able to post an empty file and verify its existence', done => {
        const emptyFilePath = path.join(__dirname, 'empty-file.txt');

        // Create an empty file
        fs.writeFile(emptyFilePath, '', err => {
            if (err) {
                return done(err);
            }

            const fields = calculateFields(ak, sk);
            const formData = new FormData();

            fields.forEach(field => {
                formData.append(field.name, field.value);
            });

            formData.append('file', fs.createReadStream(emptyFilePath));

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

                        // Initialize AWS SDK with credentials and region
                        const s3 = new AWS.S3({
                            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
                            region: 'us-east-1'
                        });

                        // Check if the object exists
                        s3.headObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                            if (err) {
                                return done(err);
                            }

                            // Verify the object size is 0
                            assert.equal(data.ContentLength, 0);

                            // Clean up: delete the empty file locally and from S3
                            fs.unlink(emptyFilePath, err => {
                                if (err) {
                                    return done(err);
                                }

                                s3.deleteObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                                    if (err) {
                                        return done(err);
                                    }

                                    done();
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
        const fields = calculateFields(ak, sk);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

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
                    assert.equal(err.response.status, 400);
                    done();
                });
        });
    });

    it('should upload an object with key slash', done => {
        const fields = calculateFields(ak, sk, [{ key: 'key', value: '/' }]);

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

    it('should return InvalidRedirectLocation if posting object with x-amz-website-redirect-location header that does not start with "http://", "https://", or "/"', done => {
        const fields = calculateFields(ak, sk, [{ 'x-amz-website-redirect-location': 'invalid-url' }]);
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
                    done(new Error('Expected InvalidRedirectLocation error but got success response'));
                })
                .catch(err => {
                    assert.equal(err.response.status, 400);
                    done();
                });
        });
    });

    it('should successfully upload object with valid x-amz-website-redirect-location header and verify it', done => {
        const validRedirectLocation = 'http://example.com';
        const fields = calculateFields(ak, sk, [{ 'x-amz-website-redirect-location': validRedirectLocation }]);
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

                    // Get the object's website redirect location
                    s3.getObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(data.WebsiteRedirectLocation, validRedirectLocation);
                        done();
                    });
                })
                .catch(err => {
                    done(err);
                });
        });
    });


    it('should be able to post object with 10 tags and verify them', done => {
        const tags = Array.from({ length: 9 }, (_, i) => `<Tag><Key>Tag${i + 1}</Key><Value>Value${i + 1}</Value></Tag>`).join('');
        const taggingXML = `<Tagging><TagSet>${tags}</TagSet></Tagging>`;
        const fields = calculateFields(ak, sk, [{ 'tagging': taggingXML }]);

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

                    // Initialize AWS SDK with credentials and region
                    const s3 = new AWS.S3({
                        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
                        region: 'us-east-1'
                    });

                    // Get the object tags
                    s3.getObjectTagging({ Bucket: bucketName, Key: filename }, (err, data) => {
                        if (err) {
                            return done(err);
                        }

                        const tags = data.TagSet;
                        assert.equal(tags.length, 9);

                        done();
                    });
                })
                .catch(err => {
                    done(err);
                });
        });
    });

    it('should refuse invalid tagging (wrong XML)', done => {
        const invalidTaggingXML = `<Tagging><TagSet><Tag><Key>Tag1</Key><Value>Value1</Value></Tag></Tagging>`; // Missing closing </TagSet>
        const fields = calculateFields(ak, sk, [{ tagging: invalidTaggingXML }]);

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
                    assert.equal(err.response.status, 400); // 400 Bad Request for invalid XML
                    done();
                });
        });
    });

    it('should return the specified success_action_status code', done => {
        const successActionStatus = 201;
        const fields = calculateFields(ak, sk, [{ 'success_action_status': successActionStatus.toString() }]);

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
                    assert.equal(response.status, successActionStatus);
                    done();
                })
                .catch(err => {
                    done(err);
                });
        });
    });

    it('should return an error if form data (excluding file) exceeds 20KB', done => {
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
                        assert.equal(error.Message[0], 'Your POST request fields preceeding the upload file was too large.');
                        done();
                    });
                });
        });
    });


    it('should return an error if there is a discrepancy between policy and form fields', done => {
        let fields = calculateFields(ak, sk);

        // Find and replace the 'key' field with an invalid name
        fields = fields.map(field => {
            if (field.name === 'key') {
                return { name: 'key', value: 'invalid-key-name' };
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
                .then(response => {
                    done(new Error('Request should not succeed with a policy and form field discrepancy'));
                })
                .catch(err => {
                    assert.ok(err.response, 'Error should be returned by axios');

                    // Parse the XML error response
                    xml2js.parseString(err.response.data, (parseErr, result) => {
                        if (parseErr) {
                            return done(parseErr);
                        }

                        const error = result.Error;
                        assert.equal(error.Code[0], 'AccessDenied', 'Expected PolicyConditionFailed error code');
                        assert.ok(
                            error.Message[0].includes('Invalid according to Policy: Policy Condition failed'),
                            'Expected error message to include policy condition failure details'
                        );
                        done();
                    });
                });
        });
    });


    it('should return an error for invalid keys', done => {
        const invalidAccessKeyId = 'INVALIDACCESSKEY';
        const invalidSecretAccessKey = 'INVALIDSECRETKEY';
        let fields = calculateFields(invalidAccessKeyId, invalidSecretAccessKey);

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
                .then(response => {
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


    it('should return an error for invalid signature', done => {
        let fields = calculateFields(ak, sk);
        const laterThanNow = new Date(new Date().getTime() + 60000);
        const shortFormattedDate = formatDate(laterThanNow);

        const signingKey = getSignatureKey(sk, shortFormattedDate, 'ap-east-1', 's3');
        const signature = crypto.createHmac('sha256', signingKey).update(fields.find(field => field.name === 'Policy').value).digest('hex');

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
                .then(response => {
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
                        assert.equal(error.Code[0], 'SignatureDoesNotMatch', 'Expected SignatureDoesNotMatch error code');
                        assert.ok(
                            error.Message[0].includes('The request signature we calculated does not match the signature you provided'),
                            'Expected error message to include signature mismatch details'
                        );
                        done();
                    });
                });
        });
    });

    it('should return an error for invalid credential', done => {
        let fields = calculateFields(ak, sk);
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
                .then(response => {
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

    it('should successfully upload object with valid checksum and verify it', done => {
        const filePath = path.join(__dirname, 'test-file.txt');
        const fileContent = 'This is a test file';

        // Calculate the SHA-256 checksum of the file content
        const validChecksum = crypto.createHash('sha256').update(fileContent).digest('base64');
        const fields = calculateFields(ak, sk, [{ 'x-amz-checksum-sha256': validChecksum }]);
        const formData = new FormData();

        fields.forEach(field => {
            formData.append(field.name, field.value);
        });

        formData.append('file', fs.createReadStream(filePath));

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

                    // Get the object to verify it was uploaded successfully
                    s3.getObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(data.ContentLength, fileContent.length);
                        done();
                    });
                })
                .catch(err => {
                    done(err);
                });
        });
    });

    it('should return error if posting object with invalid checksum', done => {
        const invalidChecksum = 'invalid-checksum'; // Invalid checksum value
        const fields = calculateFields(ak, sk, [{ 'x-amz-checksum-sha256': invalidChecksum }]);
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
                    assert.equal(err.response.status, 400); // Assuming 400 Bad Request for invalid checksum
                    done();
                });
        });
    });

    it('should successfully upload object with valid SSE parameters and verify it', done => {
        const fields = calculateFields(ak, sk, [{ 'x-amz-server-side-encryption': 'AES256' }]);
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

                    // Get the object's SSE configuration
                    s3.headObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                        if (err) {
                            return done(err);
                        }

                        assert.equal(data.ServerSideEncryption, 'AES256');

                        done();
                    });
                })
                .catch(err => {
                    done(err);
                });
        });
    });

    it('should return error if posting object with invalid SSE parameters', done => {
        const fields = calculateFields(ak, sk, [{ 'x-amz-server-side-encryption': 'INVALID' }]);
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
                    assert.equal(err.response.status, 400); // Assuming 400 Bad Request for invalid SSE parameters
                    done();
                });
        });
    });






    /** Tests with different bucket setup, failing right now because of policy...  for unknown reasons **/


    it('should successfully upload object with valid object lock parameters and verify it', done => {
        const retentionDate = new Date();
        retentionDate.setDate(retentionDate.getDate() + 1); // Set retention date one day in the future
        bucketName = `${bucketName}-object-lock`;
        // Create the bucket with Object Lock enabled
        const createBucketParams = {
            Bucket: bucketName,
            ObjectLockEnabledForBucket: true,
        };

        s3.createBucket(createBucketParams, (err) => {
            if (err) {
                return done(err);
            }

            // Enable Object Lock configuration


            const fields = calculateFields(ak, sk, [
                { 'x-amz-object-lock-mode': 'GOVERNANCE' },
                { 'x-amz-object-lock-retain-until-date': retentionDate.toISOString() }
            ]);

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

                        // Get the object's lock configuration
                        s3.headObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                            if (err) {
                                return done(err);
                            }

                            assert.equal(data.ObjectLockMode, 'GOVERNANCE');
                            assert.equal(new Date(data.ObjectLockRetainUntilDate).toISOString(), retentionDate.toISOString());

                            done();
                        });
                    })
                    .catch(err => {
                        console.log(err.response.data)
                        done(err);
                    });
            });
        });
    });


    it('should successfully upload object with valid object lock parameters and verify it', done => {
        const retentionDate = new Date();
        retentionDate.setDate(retentionDate.getDate() + 1); // Set retention date one day in the future
        const fields = calculateFields(ak, sk, [
            { 'x-amz-object-lock-mode': 'GOVERNANCE' },
            { 'x-amz-object-lock-retain-until-date': retentionDate.toISOString() },
        ]);
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

                    // Get the object's lock configuration
                    s3.headObject({ Bucket: bucketName, Key: filename }, (err, data) => {
                        if (err) {
                            return done(err);
                        }

                        assert.equal(data.ObjectLockMode, 'GOVERNANCE');
                        assert.equal(new Date(data.ObjectLockRetainUntilDate).toISOString(), retentionDate.toISOString());

                        done();
                    });
                })
                .catch(err => {
                    console.log(err.response.data)
                    done(err);
                });
        });
    });

    it('should successfully upload an object with bucket versioning enabled and verify version ID', done => {
        bucketName = `${bucketName}-versioning`;

        // Create the bucket
        s3.createBucket({ Bucket: bucketName }, (err) => {
            if (err) {
                return done(err);
            }

            // Enable versioning on the bucket
            const versioningParams = {
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };

            s3.putBucketVersioning(versioningParams, (err) => {
                if (err) {
                    return done(err);
                }

                const fields = calculateFields(ak, sk, [{ bucket: bucketName }]);
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

                            // Verify version ID is present in the response
                            const versionId = response.headers['x-amz-version-id'];
                            assert.ok(versionId, 'Version ID should be present in the response headers');

                            // Verify the object versioning
                            s3.getObject({ Bucket: bucketName, Key: filename, VersionId: versionId }, (err, data) => {
                                if (err) {
                                    return done(err);
                                }

                                assert.equal(data.VersionId, versionId);

                                done();
                            });
                        })
                        .catch(err => {
                            console.log(err);
                            done(err);
                        });
                });
            });
        });
    });

    it('should successfully upload an object with a specified ACL and verify it', done => {

        // Create the bucket
        s3.createBucket({ Bucket: bucketName }, (err) => {
            if (err) {
                return done(err);
            }

            const aclValue = 'public-read'; // Example ACL value
            const fields = calculateFields([{ 'acl': aclValue }]);
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

                        // Get the object's ACL
                        s3.getObjectAcl({ Bucket: bucketName, Key: filename }, (err, data) => {
                            if (err) {
                                return done(err);
                            }

                            const grants = data.Grants;
                            const publicReadGrant = grants.find(grant =>
                                grant.Permission === 'READ' && grant.Grantee.URI === 'http://acs.amazonaws.com/groups/global/AllUsers'
                            );

                            assert.ok(publicReadGrant, 'Expected public-read ACL grant not found');

                            done();
                        });
                    })
                    .catch(err => {
                        done(err);
                    });
            });
        });
    });

    it('should return an error when uploading an object with an invalid ACL', done => {
        // Create the bucket
        s3.createBucket({ Bucket: bucketName }, (err) => {
            if (err) {
                return done(err);
            }

            const invalidAclValue = 'invalid-acl'; // Example invalid ACL value
            const fields = calculateFields([{ 'acl': invalidAclValue }]);
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
                        assert.equal(err.response.status, 400); // Assuming 400 Bad Request for invalid ACL
                        done();
                    });
            });
        });
    });
























    // it('should be able to post an empty Tag set', done => {
    //     const fields = calculateFields([{ 'x-amz-tagging': '' }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(response => {
    //                 assert.equal(response.status, 204);
    //                 done();
    //             })
    //             .catch(err => {
    //                 done(err);
    //             });
    //     });
    // });

    // it('should be able to post object with empty tags', done => {
    //     const fields = calculateFields([{ 'x-amz-tagging': 'Tag1=' }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(response => {
    //                 assert.equal(response.status, 204);
    //                 done();
    //             })
    //             .catch(err => {
    //                 done(err);
    //             });
    //     });
    // });

    // it('should allow posting 50 tags', done => {
    //     const tags = Array.from({ length: 50 }, (_, i) => `Tag${i + 1}=Value${i + 1}`).join('&');
    //     const fields = calculateFields([{ 'x-amz-tagging': tags }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(response => {
    //                 assert.equal(response.status, 204);
    //                 done();
    //             })
    //             .catch(err => {
    //                 done(err);
    //             });
    //     });
    // });

    // it('should return BadRequest if posting more than 50 tags', done => {
    //     const tags = Array.from({ length: 51 }, (_, i) => `Tag${i + 1}=Value${i + 1}`).join('&');
    //     const fields = calculateFields([{ 'x-amz-tagging': tags }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(() => {
    //                 done(new Error('Expected BadRequest error but got success response'));
    //             })
    //             .catch(err => {
    //                 assert.equal(err.response.status, 400);
    //                 done();
    //             });
    //     });
    // });

    // it('should return InvalidArgument if using the same key twice', done => {
    //     const tags = 'Tag1=Value1&Tag1=Value2';
    //     const fields = calculateFields([{ 'x-amz-tagging': tags }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(() => {
    //                 done(new Error('Expected InvalidArgument error but got success response'));
    //             })
    //             .catch(err => {
    //                 assert.equal(err.response.status, 400);
    //                 done();
    //             });
    //     });
    // });

    // it('should return InvalidArgument if using the same key twice and empty tags', done => {
    //     const tags = 'Tag1=&Tag1=Value2';
    //     const fields = calculateFields([{ 'x-amz-tagging': tags }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(() => {
    //                 done(new Error('Expected InvalidArgument error but got success response'));
    //             })
    //             .catch(err => {
    //                 assert.equal(err.response.status, 400);
    //                 done();
    //             });
    //     });
    // });

    // it('should return InvalidArgument if tag with no key', done => {
    //     const tags = '=Value1';
    //     const fields = calculateFields([{ 'x-amz-tagging': tags }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(() => {
    //                 done(new Error('Expected InvalidArgument error but got success response'));
    //             })
    //             .catch(err => {
    //                 assert.equal(err.response.status, 400);
    //                 done();
    //             });
    //     });
    // });

    // it('should return InvalidArgument posting object with bad encoded tags', done => {
    //     const tags = 'Tag1=Value1%2'; // Bad encoding
    //     const fields = calculateFields([{ 'x-amz-tagging': tags }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(() => {
    //                 done(new Error('Expected InvalidArgument error but got success response'));
    //             })
    //             .catch(err => {
    //                 assert.equal(err.response.status, 400);
    //                 done();
    //             });
    //     });
    // });

    // it('should return InvalidArgument posting object tag with invalid characters: %', done => {
    //     const tags = 'Tag1=Value%1';
    //     const fields = calculateFields([{ 'x-amz-tagging': tags }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(() => {
    //                 done(new Error('Expected InvalidArgument error but got success response'));
    //             })
    //             .catch(err => {
    //                 assert.equal(err.response.status, 400);
    //                 done();
    //             });
    //     });
    // });






















    //     it('should handle error when file is too large', done => {
    //         const fields = calculateFields();
    //         const formData = new FormData();

    //         fields.forEach(field => {
    //             formData.append(field.name, field.value);
    //         });

    //         formData.append('file', fs.createReadStream(path.join(__dirname, 'large-test-file.txt')));

    //         formData.getLength((err, length) => {
    //             if (err) {
    //                 return done(err);
    //             }

    //             axios.post(url, formData, {
    //                 headers: {
    //                     ...formData.getHeaders(),
    //                     'Content-Length': length,
    //                 },
    //             })
    //                 .then(() => {
    //                     done(new Error('Expected error but got success response'));
    //                 })
    //                 .catch(err => {
    //                     assert.equal(err.response.status, 400);
    //                     done();
    //                 });
    //         });
    //     });

    //    it('should return error if posting object with > 2KB user-defined metadata', done => {
    //         const largeMetadata = 'a'.repeat(2049); // Metadata larger than 2KB
    //         const fields = calculateFields([{ key: 'x-amz-meta-large', value: largeMetadata }]);
    //         const formData = new FormData();

    //         fields.forEach(field => {
    //             formData.append(field.name, field.value);
    //         });

    //         formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //         formData.getLength((err, length) => {
    //             if (err) {
    //                 return done(err);
    //             }

    //             axios.post(url, formData, {
    //                 headers: {
    //                     ...formData.getHeaders(),
    //                     'Content-Length': length,
    //                 },
    //             })
    //                 .then(() => {
    //                     done(new Error('Expected error but got success response'));
    //                 })
    //                 .catch(err => {
    //                     assert.equal(err.response.status, 400);
    //                     done();
    //                 });
    //         });
    //     });


    //     it('should return InvalidRequest error if posting object with object lock retention date and mode when object lock is not enabled on the bucket', done => {
    //         const fields = calculateFields([
    //             { 'x-amz-object-lock-retain-until-date': new Date().toISOString() },
    //             { 'x-amz-object-lock-mode': 'GOVERNANCE' }
    //         ]);
    //         const formData = new FormData();

    //         fields.forEach(field => {
    //             formData.append(field.name, field.value);
    //         });

    //         formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //         formData.getLength((err, length) => {
    //             if (err) {
    //                 return done(err);
    //             }

    //             axios.post(url, formData, {
    //                 headers: {
    //                     ...formData.getHeaders(),
    //                     'Content-Length': length,
    //                 },
    //             })
    //                 .then(() => {
    //                     done(new Error('Expected InvalidRequest error but got success response'));
    //                 })
    //                 .catch(err => {
    //                     assert.equal(err.response.status, 400);
    //                     done();
    //                 });
    //         });
    //     });

    // it('should return Not Implemented error for object encryption using customer-provided encryption keys', done => {
    //     const fields = calculateFields([{ 'x-amz-server-side-encryption-customer-algorithm': 'AES256' }]);
    //     const formData = new FormData();

    //     fields.forEach(field => {
    //         formData.append(field.name, field.value);
    //     });

    //     formData.append('file', fs.createReadStream(path.join(__dirname, 'test-file.txt')));

    //     formData.getLength((err, length) => {
    //         if (err) {
    //             return done(err);
    //         }

    //         axios.post(url, formData, {
    //             headers: {
    //                 ...formData.getHeaders(),
    //                 'Content-Length': length,
    //             },
    //         })
    //             .then(() => {
    //                 done(new Error('Expected Not Implemented error but got success response'));
    //             })
    //             .catch(err => {
    //                 assert.equal(err.response.status, 501);
    //                 done();
    //             });
    //     });
    // });

});

