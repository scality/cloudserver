const assert = require('assert');
const crypto = require('crypto');
const { errors } = require('arsenal');
const AWS = require('aws-sdk');

const async = require('async');
const azure = require('azure-storage');

const { getRealAwsConfig } = require('../support/awsConfig');
const { config } = require('../../../../../lib/Config');
const authdata = require('../../../../../conf/authdata.json');
const GCP = require('../../../../../lib/data/external/GcpService');

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsLocation = 'awsbackend';
const awsLocation2 = 'awsbackend2';
const awsLocationMismatch = 'awsbackendmismatch';
const awsLocationEncryption = 'awsbackendencryption';
const azureLocation = 'azurebackend';
const azureLocation2 = 'azurebackend2';
const azureLocationMismatch = 'azurebackendmismatch';
const azureLocationNonExistContainer = 'azurenonexistcontainer';
const gcpLocation = 'gcpbackend';
const gcpLocation2 = 'gcpbackend2';
const gcpLocationMismatch = 'gcpbackendmismatch';
const gcpLocationEncryption = 'gcpbackendencryption';
const versioningEnabled = { Status: 'Enabled' };
const versioningSuspended = { Status: 'Suspended' };
const awsFirstTimeout = 10000;
const awsSecondTimeout = 30000;
const s3Backends = ['AWS', 'GCP'];
const s3Locations = {
    AWS: {
        s3Location: awsLocation,
        s3Location2: awsLocation2,
        s3LocationMismatch: awsLocationMismatch,
        s3LocationEncryption: awsLocationEncryption,
    },
    GCP: {
        s3Location: gcpLocation,
        s3Location2: gcpLocation2,
        s3LocationMismatch: gcpLocationMismatch,
        s3LocationEncryption: gcpLocationEncryption,
    },
};
let describeSkipIfNotMultiple = describe.skip;
let awsS3;
let awsBucket;
let gcpS3;
let gcpBucket;

if (config.backends.data === 'multiple') {
    describeSkipIfNotMultiple = describe;
    const awsConfig = getRealAwsConfig(awsLocation);
    awsS3 = new AWS.S3(awsConfig);
    awsBucket = config.locationConstraints[awsLocation].details.bucketName;
    const gcpConfig = getRealAwsConfig(gcpLocation);
    gcpS3 = new GCP(gcpConfig);
    gcpBucket = config.locationConstraints[gcpLocation].details.bucketName;
}

function _assertErrorResult(err, expectedError, desc) {
    if (!expectedError) {
        assert.strictEqual(err, null, `got error for ${desc}: ${err}`);
        return;
    }
    assert(err, `expected ${expectedError} but found no error`);
    assert.strictEqual(err.code, expectedError);
    assert.strictEqual(err.statusCode, errors[expectedError].code);
}

const utils = {
    describeSkipIfNotMultiple,
    awsS3,
    awsBucket,
    gcpS3,
    gcpBucket,
    fileLocation,
    memLocation,
    awsLocation,
    awsLocation2,
    awsLocationMismatch,
    awsLocationEncryption,
    azureLocation,
    azureLocation2,
    azureLocationMismatch,
    azureLocationNonExistContainer,
    gcpLocation,
    gcpLocation2,
    gcpLocationMismatch,
    gcpLocationEncryption,
    s3Locations,
};

utils.withGCP = function withGCP(testFn) {
    function fn() {
        s3Backends.forEach(s3Type => {
            testFn.call(this, s3Type);
        });
    }
    return fn;
};

utils.getOwnerInfo = account => {
    let ownerID;
    let ownerDisplayName;
    if (process.env.S3_END_TO_END) {
        if (account === 'account1') {
            ownerID = process.env.CANONICAL_ID;
            ownerDisplayName = process.env.ACCOUNT_NAME;
        } else {
            ownerID = process.env.ACCOUNT2_CANONICAL_ID;
            ownerDisplayName = process.env.ACCOUNT2_NAME;
        }
    } else {
        if (account === 'account1') {
            ownerID = authdata.accounts[0].canonicalID;
            ownerDisplayName = authdata.accounts[0].name;
        } else {
            ownerID = authdata.accounts[1].canonicalID;
            ownerDisplayName = authdata.accounts[1].name;
        }
    }
    return { ownerID, ownerDisplayName };
};

utils.uniqName = name => `${name}${new Date().getTime()}`;

utils.getAzureClient = () => {
    const params = {};
    const envMap = {
        azureStorageEndpoint: 'AZURE_STORAGE_ENDPOINT',
        azureStorageAccountName: 'AZURE_STORAGE_ACCOUNT_NAME',
        azureStorageAccessKey: 'AZURE_STORAGE_ACCESS_KEY',
    };

    const isTestingAzure = Object.keys(envMap).every(key => {
        const envVariable = process.env[`${azureLocation}_${envMap[key]}`];
        if (envVariable) {
            params[key] = envVariable;
            return true;
        }
        if (config.locationConstraints[azureLocation] &&
            config.locationConstraints[azureLocation].details &&
            config.locationConstraints[azureLocation].details[key]) {
            params[key] =
                config.locationConstraints[azureLocation].details[key];
            return true;
        }
        return false;
    });

    if (!isTestingAzure) {
        return undefined;
    }

    return azure.createBlobService(params.azureStorageAccountName,
        params.azureStorageAccessKey, params.azureStorageEndpoint);
};

utils.getAzureContainerName = azureLocation => {
    let azureContainerName;
    if (config.locationConstraints[azureLocation] &&
    config.locationConstraints[azureLocation].details &&
    config.locationConstraints[azureLocation].details.azureContainerName) {
        azureContainerName =
          config.locationConstraints[azureLocation].details.azureContainerName;
    }
    return azureContainerName;
};

utils.getAzureKeys = () => {
    const keys = [
        {
            describe: 'empty',
            name: `somekey-${Date.now()}`,
            body: '',
            MD5: 'd41d8cd98f00b204e9800998ecf8427e',
        },
        {
            describe: 'normal',
            name: `somekey-${Date.now()}`,
            body: Buffer.from('I am a body', 'utf8'),
            MD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a',
        },
        {
            describe: 'big',
            name: `bigkey-${Date.now()}`,
            body: Buffer.alloc(10485760),
            MD5: 'f1c9645dbc14efddc7d8a322685f26eb',
        },
    ];
    return keys;
};

// For contentMD5, Azure requires base64 but AWS requires hex, so convert
// from base64 to hex
utils.convertMD5 = contentMD5 =>
    Buffer.from(contentMD5, 'base64').toString('hex');

utils.expectedETag = (body, getStringified = true) => {
    const eTagValue = crypto.createHash('md5').update(body).digest('hex');
    if (!getStringified) {
        return eTagValue;
    }
    return `"${eTagValue}"`;
};

function checkArguments(s3Type, cb) {
    if (typeof s3Type === 'function' && cb === undefined) {
        return { type: 'AWS', callback: s3Type };
    }
    const type = s3Type === 'GCP' ? 'GCP' : 'AWS';
    return { type, callback: cb };
}

utils.putToAwsBackend = (s3, bucket, key, body, s3Type, cb) => {
    const { type, callback } = checkArguments(s3Type, cb);
    const { s3Location } = s3Locations[type];
    s3.putObject({ Bucket: bucket, Key: key, Body: body,
    Metadata: { 'scal-location-constraint': s3Location } },
        (err, result) => callback(err, result.VersionId));
};

utils.enableVersioning = (s3, bucket, cb) => {
    s3.putBucketVersioning({ Bucket: bucket,
        VersioningConfiguration: versioningEnabled }, err => {
        assert.strictEqual(err, null, 'Expected success ' +
            `enabling versioning, got error ${err}`);
        cb();
    });
};

utils.suspendVersioning = (s3, bucket, cb) => {
    s3.putBucketVersioning({ Bucket: bucket,
        VersioningConfiguration: versioningSuspended }, err => {
        assert.strictEqual(err, null, 'Expected success ' +
            `enabling versioning, got error ${err}`);
        cb();
    });
};

utils.mapToAwsPuts = (s3, bucket, key, dataArray, s3Type, cb) => {
    const { type, callback } = checkArguments(s3Type, cb);
    async.mapSeries(dataArray, (data, next) => {
        utils.putToAwsBackend(s3, bucket, key, data, type, next);
    }, (err, results) => {
        assert.strictEqual(err, null, 'Expected success ' +
            `putting object, got error ${err}`);
        callback(null, results);
    });
};

utils.putVersionsToAws = (s3, bucket, key, versions, s3Type, cb) => {
    const { type, callback } = checkArguments(s3Type, cb);
    utils.enableVersioning(s3, bucket, () => {
        utils.mapToAwsPuts(s3, bucket, key, versions, type, callback);
    });
};

utils.putNullVersionsToAws = (s3, bucket, key, versions, s3Type, cb) => {
    const { type, callback } = checkArguments(s3Type, cb);
    utils.suspendVersioning(s3, bucket, () => {
        utils.mapToAwsPuts(s3, bucket, key, versions, type, callback);
    });
};

utils.getAndAssertResult = (s3, params, cb) => {
    const { bucket, key, body, versionId, expectedVersionId, expectedTagCount,
    expectedError } = params;
    s3.getObject({ Bucket: bucket, Key: key, VersionId: versionId },
        (err, data) => {
            _assertErrorResult(err, expectedError, 'putting tags');
            if (expectedError) {
                return cb();
            }
            assert.strictEqual(err, null, 'Expected success ' +
                `getting object, got error ${err}`);
            if (body) {
                assert(data.Body, 'expected object body in response');
                const expectedMD5 = utils.expectedETag(body, false);
                const resultMD5 = utils.expectedETag(data.Body, false);
                assert.strictEqual(resultMD5, expectedMD5);
            }
            if (!expectedVersionId) {
                assert.strictEqual(data.VersionId, undefined);
            } else {
                assert.strictEqual(data.VersionId, expectedVersionId);
            }
            if (expectedTagCount && expectedTagCount === '0') {
                assert.strictEqual(data.TagCount, undefined);
            } else if (expectedTagCount) {
                assert.strictEqual(data.TagCount, expectedTagCount);
            }
            return cb();
        });
};

utils.getAwsRetry = (params, retryNumber, assertCb) => {
    const { key, versionId, s3Type } = params;
    const retryTimeout = {
        0: 0,
        1: awsFirstTimeout,
        2: awsSecondTimeout,
    };
    const maxRetries = 2;
    const getObject = s3Type === 'GCP' ?
        gcpS3.getObject.bind(gcpS3) : awsS3.getObject.bind(awsS3);
    const bucket = s3Type === 'GCP' ? gcpBucket : awsBucket;
    assert(getObject, `Unable to retrieve client for ${s3Type}`);
    const timeout = retryTimeout[retryNumber];
    return setTimeout(getObject, timeout, { Bucket: bucket, Key: key,
        VersionId: versionId },
        (err, res) => {
            try {
                // note: this will only catch exceptions thrown before an
                // asynchronous call
                return assertCb(err, res);
            } catch (e) {
                if (retryNumber !== maxRetries) {
                    return utils.getAwsRetry(params, retryNumber + 1,
                        assertCb);
                }
                throw e;
            }
        });
};

utils.awsGetLatestVerId = (key, body, s3Type, cb) => {
    const { type, callback } = checkArguments(s3Type, cb);
    return utils.getAwsRetry({ key, s3Type: type }, 0, (err, result) => {
        assert.strictEqual(err, null, 'Expected success ' +
            `getting object from AWS, got error ${err}`);
        const resultMD5 = utils.expectedETag(result.Body, false);
        const expectedMD5 = utils.expectedETag(body, false);
        assert.strictEqual(resultMD5, expectedMD5, 'expected different body');
        return callback(null, result.VersionId);
    });
};

utils.tagging = {};

function _getTaggingConfig(tags) {
    return {
        // eslint-disable-next-line arrow-body-style
        TagSet: Object.keys(tags).map(key => {
            return {
                Key: key,
                Value: tags[key],
            };
        }),
    };
}

utils.tagging.putTaggingAndAssert = (s3, params, cb) => {
    const { bucket, key, tags, versionId, expectedVersionId,
        expectedError } = params;
    const taggingConfig = _getTaggingConfig(tags);
    return s3.putObjectTagging({ Bucket: bucket, Key: key, VersionId: versionId,
        Tagging: taggingConfig }, (err, data) => {
        _assertErrorResult(err, expectedError, 'putting tags');
        if (expectedError) {
            return cb();
        }
        assert.strictEqual(err, null, `got error for putting tags: ${err}`);
        if (expectedVersionId) {
            assert.strictEqual(data.VersionId, expectedVersionId);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }
        return cb(null, data.VersionId);
    });
};

utils.tagging.getTaggingAndAssert = (s3, params, cb) => {
    const { bucket, key, expectedTags, versionId, expectedVersionId,
        expectedError, getObject } = params;
    s3.getObjectTagging({ Bucket: bucket, Key: key, VersionId: versionId },
        (err, data) => {
            _assertErrorResult(err, expectedError, 'putting tags');
            if (expectedError) {
                return cb();
            }
            const expectedTagResult = _getTaggingConfig(expectedTags);
            const expectedTagCount = `${Object.keys(expectedTags).length}`;
            assert.strictEqual(err, null, `got error for putting tags: ${err}`);
            if (expectedVersionId) {
                assert.strictEqual(data.VersionId, expectedVersionId);
            } else {
                assert.strictEqual(data.VersionId, undefined);
            }
            assert.deepStrictEqual(data.TagSet, expectedTagResult.TagSet);
            if (getObject === false) {
                return process.nextTick(cb, null, data.VersionId);
            }
            return utils.getAndAssertResult(s3, { bucket, key, versionId,
                expectedVersionId, expectedTagCount },
                () => cb(null, data.VersionId));
        });
};

utils.tagging.delTaggingAndAssert = (s3, params, cb) => {
    const { bucket, key, versionId, expectedVersionId, expectedError } = params;
    return s3.deleteObjectTagging({ Bucket: bucket, Key: key,
        VersionId: versionId }, (err, data) => {
        _assertErrorResult(err, expectedError, 'putting tags');
        if (expectedError) {
            return cb();
        }
        assert.strictEqual(err, null, `got error for putting tags: ${err}`);
        if (expectedVersionId) {
            assert.strictEqual(data.VersionId, expectedVersionId);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }
        return utils.tagging.getTaggingAndAssert(s3, { bucket, key, versionId,
            expectedVersionId, expectedTags: {} }, () => cb());
    });
};

utils.tagging.awsGetAssertTags = (params, cb) => {
    const { key, versionId, expectedTags, s3Type } = params;
    const s3Bucket = s3Type === 'GCP' ? gcpBucket : awsBucket;
    const expectedTagResult = _getTaggingConfig(expectedTags);
    awsS3.getObjectTagging({ Bucket: s3Bucket, Key: key,
        VersionId: versionId }, (err, data) => {
        assert.strictEqual(err, null, 'got unexpected error getting ' +
            `tags directly from ${s3Type}: ${err}`);
        assert.deepStrictEqual(data.TagSet, expectedTagResult.TagSet);
        return cb();
    });
};


module.exports = utils;
