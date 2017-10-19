const assert = require('assert');
const crypto = require('crypto');
const { errors } = require('arsenal');
const AWS = require('aws-sdk');

const async = require('async');
const azure = require('azure-storage');

const { getRealAwsConfig } = require('../support/awsConfig');
const { config } = require('../../../../../lib/Config');

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsLocation = 'awsbackend';
const awsLocation2 = 'awsbackend2';
const awsLocationMismatch = 'awsbackendmismatch';
const azureLocation = 'azurebackend';
const azureLocation2 = 'azurebackend2';
const azureLocationMismatch = 'azurebackendmismatch';
const awsLocationEncryption = 'awsbackendencryption';
const versioningEnabled = { Status: 'Enabled' };
const versioningSuspended = { Status: 'Suspended' };
const awsTimeout = 10000;
let describeSkipIfNotMultiple = describe.skip;
let awsS3;
let awsBucket;

if (config.backends.data === 'multiple') {
    describeSkipIfNotMultiple = describe;
    const awsConfig = getRealAwsConfig(awsLocation);
    awsS3 = new AWS.S3(awsConfig);
    awsBucket = config.locationConstraints[awsLocation].details.bucketName;
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
    fileLocation,
    memLocation,
    awsLocation,
    awsLocation2,
    awsLocationMismatch,
    awsLocationEncryption,
    azureLocation,
    azureLocation2,
    azureLocationMismatch,
};

utils.uniqName = name => `${name}${new Date().getTime()}`;

utils.getAzureClient = () => {
    let isTestingAzure;
    let azureBlobEndpoint;
    let azureBlobSAS;
    let azureClient;
    if (process.env[`${azureLocation}_AZURE_BLOB_ENDPOINT`]) {
        isTestingAzure = true;
        azureBlobEndpoint = process.env[`${azureLocation}_AZURE_BLOB_ENDPOINT`];
    } else if (config.locationConstraints[azureLocation] &&
          config.locationConstraints[azureLocation].details &&
          config.locationConstraints[azureLocation].details.azureBlobEndpoint) {
        isTestingAzure = true;
        azureBlobEndpoint =
          config.locationConstraints[azureLocation].details.azureBlobEndpoint;
    } else {
        isTestingAzure = false;
    }

    if (isTestingAzure) {
        if (process.env[`${azureLocation}_AZURE_BLOB_SAS`]) {
            azureBlobSAS = process.env[`${azureLocation}_AZURE_BLOB_SAS`];
            isTestingAzure = true;
        } else if (config.locationConstraints[azureLocation] &&
            config.locationConstraints[azureLocation].details &&
            config.locationConstraints[azureLocation].details.azureBlobSAS
        ) {
            azureBlobSAS = config.locationConstraints[azureLocation].details
              .azureBlobSAS;
            isTestingAzure = true;
        } else {
            isTestingAzure = false;
        }
    }

    if (isTestingAzure) {
        azureClient = azure.createBlobServiceWithSas(azureBlobEndpoint,
          azureBlobSAS);
    }
    return azureClient;
};

utils.getAzureContainerName = () => {
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

utils.putToAwsBackend = (s3, bucket, key, body, cb) => {
    s3.putObject({ Bucket: bucket, Key: key, Body: body,
    Metadata: { 'scal-location-constraint': awsLocation } },
        (err, result) => cb(err, result.VersionId));
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

utils.mapToAwsPuts = (s3, bucket, key, dataArray, cb) => {
    async.mapSeries(dataArray, (data, next) => {
        utils.putToAwsBackend(s3, bucket, key, data, next);
    }, (err, results) => {
        assert.strictEqual(err, null, 'Expected success ' +
            `putting object, got error ${err}`);
        cb(null, results);
    });
};

utils.putVersionsToAws = (s3, bucket, key, versions, cb) => {
    utils.enableVersioning(s3, bucket, () => {
        utils.mapToAwsPuts(s3, bucket, key, versions, cb);
    });
};

utils.putNullVersionsToAws = (s3, bucket, key, versions, cb) => {
    utils.suspendVersioning(s3, bucket, () => {
        utils.mapToAwsPuts(s3, bucket, key, versions, cb);
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

utils.awsGetLatestVerId = (key, body, cb) => {
    const getObject = awsS3.getObject.bind(awsS3);
    return setTimeout(getObject, awsTimeout, { Bucket: awsBucket, Key: key },
        (err, result) => {
            assert.strictEqual(err, null, 'Expected success ' +
                `getting object from AWS, got error ${err}`);
            const resultMD5 = utils.expectedETag(result.Body, false);
            const expectedMD5 = utils.expectedETag(body, false);
            assert.strictEqual(resultMD5, expectedMD5,
                'expected different body');
            return cb(null, result.VersionId);
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
    const { key, versionId, expectedTags } = params;
    const expectedTagResult = _getTaggingConfig(expectedTags);
    awsS3.getObjectTagging({ Bucket: awsBucket, Key: key,
        VersionId: versionId }, (err, data) => {
        assert.strictEqual(err, null, 'got unexpected error getting ' +
            `tags directly from AWS: ${err}`);
        assert.deepStrictEqual(data.TagSet, expectedTagResult.TagSet);
        return cb();
    });
};


module.exports = utils;
