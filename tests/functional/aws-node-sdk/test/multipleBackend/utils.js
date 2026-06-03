const assert = require('assert');
const crypto = require('crypto');
const { storage } = require('arsenal');
const {
    S3Client,
    PutObjectCommand,
    GetObjectCommand,
    PutBucketVersioningCommand,
    GetBucketVersioningCommand,
    PutObjectTaggingCommand,
    GetObjectTaggingCommand,
    DeleteObjectTaggingCommand,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');

const azure = require('@azure/storage-blob');

const { GCP } = storage.data.external.GCP;

const { getRealAwsConfig } = require('../support/awsConfig');
const { config } = require('../../../../../lib/Config');
const authdata = require('../../../../../conf/authdata.json');

const { describeSkipIfNotMultiple } = require('../../lib/utility/test-utils');

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
const versioningEnabled = { Status: 'Enabled' };
const versioningSuspended = { Status: 'Suspended' };
const awsFirstTimeout = 10000;
const awsSecondTimeout = 30000;
let awsS3;
let awsBucket;

let gcpClient;
let gcpBucket;
let gcpBucketMPU;

if (config.backends.data === 'multiple') {
    if (config.locationConstraints[awsLocation]) {
        const awsConfig = getRealAwsConfig(awsLocation);
        awsS3 = new S3Client(awsConfig);
        awsBucket = config.locationConstraints[awsLocation].details.bucketName;
    } else {
        process.stdout.write(
            `LocationConstraint for aws '${awsLocation}' not found in ${Object.keys(config.locationConstraints)}\n`,
        );
    }

    if (config.locationConstraints[gcpLocation]) {
        const gcpConfig = getRealAwsConfig(gcpLocation);
        gcpClient = new GCP(gcpConfig);
        gcpBucket = config.locationConstraints[gcpLocation].details.bucketName;
        gcpBucketMPU = config.locationConstraints[gcpLocation].details.mpuBucketName;
    } else {
        process.stdout.write(
            `LocationConstraint for gcp '${gcpLocation}' not found in ${Object.keys(config.locationConstraints)}\n`,
        );
    }
}

const utils = {
    describeSkipIfNotMultiple,
    awsS3,
    awsBucket,
    gcpClient,
    gcpBucket,
    gcpBucketMPU,
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
};

utils.genUniqID = () => uuidv4().replace(/-/g, '');

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

utils.uniqName = name => `${name}-${utils.genUniqID()}`;

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

        if (
            config.locationConstraints[azureLocation] &&
            config.locationConstraints[azureLocation].details &&
            config.locationConstraints[azureLocation].details[key]
        ) {
            params[key] = config.locationConstraints[azureLocation].details[key];
            return true;
        }
        return false;
    });

    if (!isTestingAzure) {
        return undefined;
    }

    const cred = new azure.StorageSharedKeyCredential(params.azureStorageAccountName, params.azureStorageAccessKey);
    return new azure.BlobServiceClient(params.azureStorageEndpoint, cred);
};

utils.getAzureContainerName = azureLocation => {
    let azureContainerName;
    if (
        config.locationConstraints[azureLocation] &&
        config.locationConstraints[azureLocation].details &&
        config.locationConstraints[azureLocation].details.azureContainerName
    ) {
        azureContainerName = config.locationConstraints[azureLocation].details.azureContainerName;
    }
    return azureContainerName;
};

utils.getAzureKeys = () => {
    const keys = [
        {
            describe: 'empty',
            name: `somekey-${utils.genUniqID()}`,
            body: '',
            MD5: 'd41d8cd98f00b204e9800998ecf8427e',
        },
        {
            describe: 'normal',
            name: `somekey-${utils.genUniqID()}`,
            body: Buffer.from('I am a body', 'utf8'),
            MD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a',
        },
        {
            describe: 'big',
            name: `bigkey-${utils.genUniqID()}`,
            body: Buffer.alloc(10485760),
            MD5: 'f1c9645dbc14efddc7d8a322685f26eb',
        },
    ];
    return keys;
};

// For contentMD5, Azure requires base64 but AWS requires hex, so convert
// from base64 to hex
utils.convertMD5 = contentMD5 => Buffer.from(contentMD5, 'base64').toString('hex');

utils.expectedETag = (body, getStringified = true) => {
    const eTagValue = crypto.createHash('md5').update(body).digest('hex');
    if (!getStringified) {
        return eTagValue;
    }
    return `"${eTagValue}"`;
};

utils.waitForVersioningBeforePut = async (s3, bucket, callback) => {
    const MAX_VERSIONING_CHECKS = 10;
    const VERSIONING_CHECK_INTERVAL = 1000;
    const sleep = () => new Promise(resolve => setTimeout(resolve, VERSIONING_CHECK_INTERVAL));

    const waitForVersioning = (async () => {
        for (let attempt = 1; attempt <= MAX_VERSIONING_CHECKS; attempt++) {
            let versioningEnabled = false;
            try {
                const versioningResult = await s3.send(
                    new GetBucketVersioningCommand({
                        Bucket: bucket,
                    }),
                );
                versioningEnabled = versioningResult.Status === 'Enabled';
            } catch {
                if (attempt === MAX_VERSIONING_CHECKS) {
                    break;
                }
                await sleep();
                continue;
            }
            if (versioningEnabled) {
                break;
            }

            if (attempt < MAX_VERSIONING_CHECKS) {
                await sleep();
            }
        }
    })();

    if (callback) {
        waitForVersioning.then(() => callback()).catch(err => callback(err));
        return waitForVersioning;
    }
    return waitForVersioning;
};

utils.putToAwsBackend = (s3, bucket, key, body, callback) => {
    const result = s3.send(
        new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body,
            Metadata: { 'scal-location-constraint': awsLocation },
        }),
    );
    if (callback) {
        return result
            .then(data => {
                callback(null, data.VersionId);
            })
            .catch(err => {
                s3.send(
                    new ListObjectVersionsCommand({
                        Bucket: bucket,
                        Prefix: key,
                    }),
                )
                    .then(data => {
                        callback(err, data.VersionId);
                    })
                    .catch(listErr => {
                        callback(listErr);
                    });
            });
    }
    return result;
};

utils.enableVersioning = (s3, bucket, callback) => {
    const promise = s3.send(
        new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: versioningEnabled,
        }),
    );

    if (callback) {
        return promise.then(() => callback()).catch(err => callback(err));
    }
    return promise;
};

utils.suspendVersioning = (s3, bucket, callback) => {
    const promise = s3.send(
        new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: versioningSuspended,
        }),
    );

    if (callback) {
        return promise.then(() => callback()).catch(err => callback(err));
    }
    return promise;
};

utils.mapToAwsPuts = async (s3, bucket, key, dataArray, callback) => {
    try {
        const results = [];
        for (const data of dataArray) {
            const result = await utils.putToAwsBackend(s3, bucket, key, data);
            const versionId = result.VersionId;
            results.push(versionId);
        }
        if (callback) {
            callback(null, results);
            return undefined;
        }
        return results;
    } catch (err) {
        if (callback) {
            callback(err);
            return undefined;
        }
        throw err;
    }
};

utils.putVersionsToAws = async (s3, bucket, key, versions, callback) => {
    try {
        await utils.enableVersioning(s3, bucket);
        // Wait for versioning to be enabled before PUT to ensure VersionId is available
        await utils.waitForVersioningBeforePut(s3, bucket);
        const results = await utils.mapToAwsPuts(s3, bucket, key, versions);
        if (callback) {
            callback(null, results);
            return undefined;
        }
        return results;
    } catch (err) {
        if (callback) {
            callback(err);
            return undefined;
        }
        throw err;
    }
};

utils.putNullVersionsToAws = async (s3, bucket, key, versions, callback) => {
    try {
        await utils.suspendVersioning(s3, bucket);
        // Note: When versioning is suspended, we don't need to wait for "Enabled" status
        // The wait is only needed when enabling versioning
        const results = await utils.mapToAwsPuts(s3, bucket, key, versions);
        if (callback) {
            callback(null, results);
            return undefined;
        }
        return results;
    } catch (err) {
        if (callback) {
            callback(err);
            return undefined;
        }
        throw err;
    }
};

utils.getAndAssertResult = (s3, params, callback) => {
    const run = async () => {
        const { bucket, key, body, versionId, expectedVersionId, expectedTagCount, expectedError } = params;
        const getParams = {
            Bucket: bucket,
            Key: key,
        };
        if (versionId) {
            getParams.VersionId = versionId;
        }

        try {
            const data = await s3.send(new GetObjectCommand(getParams));
            if (expectedError) {
                throw new Error(`Expected error ${expectedError} but got success`);
            }
            if (body) {
                assert(data.Body, 'expected object body in response');

                // SDK v3 returns a stream; buffer it before running assertions
                const chunks = [];
                for await (const chunk of data.Body) {
                    chunks.push(chunk);
                }
                const bodyBuffer = Buffer.concat(chunks);
                assert.equal(
                    bodyBuffer.length,
                    data.ContentLength,
                    `received data of length ${bodyBuffer.length} does not ` +
                        'equal expected based on ' +
                        `content length header of ${data.ContentLength}`,
                );
                const expectedMD5 = utils.expectedETag(body, false);
                const resultMD5 = utils.expectedETag(bodyBuffer, false);
                assert.strictEqual(resultMD5, expectedMD5);
            }
            if (!expectedVersionId) {
                assert.strictEqual(data.VersionId, undefined, `Expected undefined VersionId but got ${data.VersionId}`);
            } else {
                assert.strictEqual(
                    data.VersionId,
                    expectedVersionId,
                    `Expected VersionId ${expectedVersionId} but got ${data.VersionId}`,
                );
            }
            if (expectedTagCount && expectedTagCount === '0') {
                assert.strictEqual(data.TagCount, undefined);
            } else if (expectedTagCount) {
                assert.strictEqual(data.TagCount, parseInt(expectedTagCount, 10));
            }
        } catch (err) {
            if (expectedError) {
                assert.strictEqual(err.name, expectedError);
                return;
            }
            throw err;
        }
    };

    const promise = run();
    if (callback) {
        promise.then(() => callback()).catch(err => callback(err));
        return undefined;
    }
    return promise;
};

utils.getAwsRetry = (params, retryNumber, assertCb) => {
    const { key, versionId } = params;
    const retryTimeout = {
        0: 0,
        1: awsFirstTimeout,
        2: awsSecondTimeout,
    };
    const maxRetries = 2;
    const timeout = retryTimeout[retryNumber];

    const executeGet = async () => {
        try {
            const params = {
                Bucket: awsBucket,
                Key: key,
                VersionId: versionId,
            };
            const res = await awsS3.send(new GetObjectCommand(params));
            return { success: true, data: res };
        } catch (err) {
            return { success: false, error: err };
        }
    };

    return setTimeout(() => {
        executeGet()
            .then(result => {
                if (result.success) {
                    return assertCb(null, result.data);
                }
                const err = result.error;
                if (err.$metadata?.httpStatusCode === 404) {
                    return assertCb(err);
                }
                if (retryNumber < maxRetries) {
                    return utils.getAwsRetry(params, retryNumber + 1, assertCb);
                }
                return assertCb(err);
            })
            .catch(e => assertCb(e));
    }, timeout);
};

utils.awsGetLatestVerId = (key, body, cb) =>
    utils.getAwsRetry({ key }, 0, async (err, result) => {
        assert.strictEqual(err, null, 'Expected success ' + `getting object from AWS, got error ${err}`);

        const chunks = [];
        for await (const chunk of result.Body) {
            chunks.push(chunk);
        }
        const bodyBuffer = Buffer.concat(chunks);
        const resultMD5 = utils.expectedETag(bodyBuffer, false);
        const expectedMD5 = utils.expectedETag(body, false);
        assert.strictEqual(resultMD5, expectedMD5, 'expected different body');
        return cb(null, result.VersionId);
    });

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

utils.tagging.putTaggingAndAssert = async (s3, params) => {
    const { bucket, key, tags, versionId, expectedVersionId, expectedError } = params;
    const taggingConfig = _getTaggingConfig(tags);

    try {
        const data = await s3.send(
            new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                Tagging: taggingConfig,
            }),
        );

        if (expectedError) {
            throw new Error(`Expected error ${expectedError} but got success`);
        }

        if (expectedVersionId) {
            assert.strictEqual(data.VersionId, expectedVersionId);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }
        return data.VersionId;
    } catch (err) {
        if (expectedError) {
            assert.strictEqual(err.name, expectedError);
            return undefined;
        }
        throw err;
    }
};

utils.tagging.getTaggingAndAssert = async (s3, params) => {
    const { bucket, key, expectedTags, versionId, expectedVersionId, expectedError, getObject } = params;

    try {
        const data = await s3.send(
            new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }),
        );

        if (expectedError) {
            throw new Error(`Expected error ${expectedError} but got success`);
        }

        const expectedTagResult = _getTaggingConfig(expectedTags);
        const expectedTagCount = `${Object.keys(expectedTags).length}`;

        if (expectedVersionId) {
            assert.strictEqual(data.VersionId, expectedVersionId);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }
        assert.deepStrictEqual(data.TagSet, expectedTagResult.TagSet);

        if (getObject !== false) {
            await utils.getAndAssertResult(s3, { bucket, key, versionId, expectedVersionId, expectedTagCount });
        }

        return data.VersionId;
    } catch (err) {
        if (expectedError) {
            assert.strictEqual(err.name, expectedError);
            return undefined;
        }
        throw err;
    }
};

utils.tagging.delTaggingAndAssert = async (s3, params) => {
    const { bucket, key, versionId, expectedVersionId, expectedError } = params;

    try {
        const data = await s3.send(
            new DeleteObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }),
        );

        if (expectedError) {
            throw new Error(`Expected error ${expectedError} but got success`);
        }

        if (expectedVersionId) {
            assert.strictEqual(data.VersionId, expectedVersionId);
        } else {
            assert.strictEqual(data.VersionId, undefined);
        }

        await utils.tagging.getTaggingAndAssert(s3, {
            bucket,
            key,
            versionId,
            expectedVersionId,
            expectedTags: {},
        });
        return undefined;
    } catch (err) {
        if (expectedError) {
            assert.strictEqual(err.name, expectedError);
            return undefined;
        }
        throw err;
    }
};

utils.tagging.awsGetAssertTags = async params => {
    const { key, versionId, expectedTags } = params;
    const expectedTagResult = _getTaggingConfig(expectedTags);

    const data = await awsS3.send(
        new GetObjectTaggingCommand({
            Bucket: awsBucket,
            Key: key,
            VersionId: versionId,
        }),
    );

    assert.deepStrictEqual(data.TagSet, expectedTagResult.TagSet);
};

module.exports = utils;
