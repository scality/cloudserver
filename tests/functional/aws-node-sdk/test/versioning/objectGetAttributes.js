const assert = require('assert');
const { promisify } = require('util');
const { S3 } = require('aws-sdk');
const getConfig = require('../support/config');
const { removeAllVersions, versioningEnabled } = require('../../lib/utility/versioning-util.js');

const removeAllVersionsPromise = promisify(removeAllVersions);

const bucket = 'testbucket';
const key = 'testobject';
const body = 'hello world!';
const expectedMD5 = 'fc3ff98e8c6a0d3087d515c0473f8677';

describe('Test get object attributes with versioning', () => {
  let s3;

  before(() => {
    const config = getConfig('default', { signatureVersion: 'v4' });
    s3 = new S3(config);
  });

  beforeEach(async () => {
    await s3.createBucket({ Bucket: bucket }).promise();
    await s3
      .putBucketVersioning({
        Bucket: bucket,
        VersioningConfiguration: versioningEnabled,
      })
      .promise();
  });

  afterEach(async () => {
    await removeAllVersionsPromise({ Bucket: bucket });
    await s3.deleteBucket({ Bucket: bucket }).promise();
  });

  it('should return NoSuchVersion for non-existent versionId', async () => {
    await s3
      .putObject({
        Bucket: bucket,
        Key: key,
        Body: body,
      })
      .promise();

    // Use a properly formatted but non-existent version ID
    const fakeVersionId = '111111111111111111111111111111111111111175636f7270';

    try {
      await s3
        .getObjectAttributes({
          Bucket: bucket,
          Key: key,
          VersionId: fakeVersionId,
          ObjectAttributes: ['ETag'],
        })
        .promise();
      assert.fail('Expected NoSuchVersion error');
    } catch (err) {
      assert.strictEqual(err.code, 'NoSuchVersion');
      assert.strictEqual(
          err.message,
          'Indicates that the version ID specified in the request does not match an existing version.',
      );
    }
  });

  it('should return MethodNotAllowed for delete marker', async () => {
    await s3
      .putObject({
        Bucket: bucket,
        Key: key,
        Body: body,
      })
      .promise();

    // Delete creates a delete marker
    await s3
      .deleteObject({
        Bucket: bucket,
        Key: key,
      })
      .promise();

    // Request without versionId targets the delete marker
    try {
      await s3
        .getObjectAttributes({
          Bucket: bucket,
          Key: key,
          ObjectAttributes: ['ETag'],
        })
        .promise();
      assert.fail('Expected MethodNotAllowed error');
    } catch (err) {
      assert.strictEqual(err.code, 'MethodNotAllowed');
      assert.strictEqual(err.message, 'The specified method is not allowed against this resource.');
    }
  });

  it('should return attributes for specific version', async () => {
    const putResult = await s3
      .putObject({
        Bucket: bucket,
        Key: key,
        Body: body,
      })
      .promise();
    const versionId = putResult.VersionId;

    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        VersionId: versionId,
        ObjectAttributes: ['ETag', 'ObjectSize'],
      })
      .promise();

    assert.strictEqual(data.ETag, expectedMD5);
    assert.strictEqual(data.ObjectSize, body.length);
    assert(data.LastModified, 'LastModified should be present');
  });

  it('should return VersionId for versioned object', async () => {
    const putResult = await s3
      .putObject({
        Bucket: bucket,
        Key: key,
        Body: body,
      })
      .promise();
    const versionId = putResult.VersionId;

    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['ETag'],
      })
      .promise();

    assert.strictEqual(data.VersionId, versionId);
  });
});
