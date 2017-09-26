const assert = require('assert');
const crypto = require('crypto');

const async = require('async');
const azure = require('azure-storage');

const { config } = require('../../../../../lib/Config');

const memLocation = 'mem-test';
const fileLocation = 'file-test';
const awsLocation = 'aws-test';
const awsLocation2 = 'aws-test-2';
const awsLocationMismatch = 'aws-test-mismatch';
const azureLocation = 'azuretest';
const azureLocation2 = 'azuretest2';
const azureLocationMismatch = 'azuretestmismatch';
const versioningEnabled = { Status: 'Enabled' };
const versioningSuspended = { Status: 'Suspended' };

let awsBucket;

if (config.backends.data === 'multiple' && !process.env.S3_END_TO_END) {
    awsBucket = config.locationConstraints[awsLocation].details.bucketName;
}

const utils = {
    awsBucket,
    fileLocation,
    memLocation,
    awsLocation,
    awsLocation2,
    awsLocationMismatch,
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
    const { bucket, key, body, versionId, expectedVersionId } = params;
    s3.getObject({ Bucket: bucket, Key: key, VersionId: versionId },
        (err, data) => {
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
            cb();
        });
};

module.exports = utils;
