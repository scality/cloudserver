const assert = require('assert');
const { S3 } = require('aws-sdk');
const getConfig = require('../support/config');

const bucket = 'testbucket';
const key = 'testobject';
const body = 'hello world!';
const expectedMD5 = 'fc3ff98e8c6a0d3087d515c0473f8677';

describe('objectGetAttributes', () => {
  let s3;

  before(() => {
    const config = getConfig('default', { signatureVersion: 'v4' });
    s3 = new S3(config);
  });

  beforeEach(async () => {
    await s3.createBucket({ Bucket: bucket }).promise();
    await s3.putObject({ Bucket: bucket, Key: key, Body: body }).promise();
  });

  afterEach(async () => {
    await s3.deleteObject({ Bucket: bucket, Key: key }).promise();
    await s3.deleteBucket({ Bucket: bucket }).promise();
  });

  it('should fail with a wrong bucket owner header', async () => {
    try {
      await s3
        .getObjectAttributes({
          Bucket: bucket,
          Key: key,
          ObjectAttributes: ['ETag'],
          ExpectedBucketOwner: 'wrongAccountId',
        })
        .promise();
      assert.fail('Expected AccessDenied error');
    } catch (err) {
      assert.strictEqual(err.code, 'AccessDenied');
      assert.strictEqual(err.message, 'Access Denied');
    }
  });

  it('should fail because attributes header is missing', async () => {
    try {
      await s3
        .getObjectAttributes({
          Bucket: bucket,
          Key: key,
          ObjectAttributes: [],
        })
        .promise();
      assert.fail('Expected InvalidRequest error');
    } catch (err) {
      assert.strictEqual(err.code, 'InvalidRequest');
      assert.strictEqual(
          err.message,
          'The x-amz-object-attributes header specifying the attributes to be retrieved is either missing or empty',
      );
    }
  });

  it('should fail because attribute name is invalid', async () => {
    try {
      await s3
        .getObjectAttributes({
          Bucket: bucket,
          Key: key,
          ObjectAttributes: ['InvalidAttribute'],
        })
        .promise();
      assert.fail('Expected InvalidArgument error');
    } catch (err) {
      assert.strictEqual(err.code, 'InvalidArgument');
      assert.strictEqual(err.message, 'Invalid attribute name specified.');
    }
  });

  it('should return NoSuchKey for non-existent object', async () => {
    try {
      await s3
        .getObjectAttributes({
          Bucket: bucket,
          Key: 'nonexistent',
          ObjectAttributes: ['ETag'],
        })
        .promise();
      assert.fail('Expected NoSuchKey error');
    } catch (err) {
      assert.strictEqual(err.code, 'NoSuchKey');
      assert.strictEqual(err.message, 'The specified key does not exist.');
    }
  });

  it('should return all attributes', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize'],
      })
      .promise();

    assert.strictEqual(data.ETag, expectedMD5);
    assert.strictEqual(data.StorageClass, 'STANDARD');
    assert.strictEqual(data.ObjectSize, body.length);
    assert.deepStrictEqual(data.Checksum, {}, 'Checksum should be present');
    assert.strictEqual(data.ObjectParts, undefined, "ObjectParts shouldn't be present for non-MPU object");
    assert(data.LastModified, 'LastModified should be present');
  });

  it('should return ETag', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['ETag'],
      })
      .promise();

    assert.strictEqual(data.ETag, expectedMD5);
  });

  it('should return Checksum', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['Checksum'],
      })
      .promise();

    assert.deepStrictEqual(data.Checksum, {}, 'Checksum should be present');
  });

  it("shouldn't return ObjectParts for non-MPU objects", async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['ObjectParts'],
      })
      .promise();

    assert.strictEqual(data.ObjectParts, undefined, "ObjectParts shouldn't be present");
  });

  it('should return StorageClass', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['StorageClass'],
      })
      .promise();

    assert.strictEqual(data.StorageClass, 'STANDARD');
  });

  it('should return ObjectSize', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['ObjectSize'],
      })
      .promise();

    assert.strictEqual(data.ObjectSize, body.length);
  });

  it('should return LastModified', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: key,
        ObjectAttributes: ['ETag'],
      })
      .promise();

    assert(data.LastModified, 'LastModified should be present');
    assert(data.LastModified instanceof Date, 'LastModified should be a Date');
    assert(!isNaN(data.LastModified.getTime()), 'LastModified should be a valid date');
  });
});

describe('Test get object attributes with multipart upload', () => {
  let s3;
  const mpuKey = 'mpuObject';
  const partSize = 5 * 1024 * 1024; // Minimum part size is 5MB
  const partCount = 3;

  before(async () => {
    const config = getConfig('default', { signatureVersion: 'v4' });
    s3 = new S3(config);

    await s3.createBucket({ Bucket: bucket }).promise();

    const createResult = await s3
      .createMultipartUpload({
        Bucket: bucket,
        Key: mpuKey,
      })
      .promise();
    const uploadId = createResult.UploadId;

    const partData = Buffer.alloc(partSize, 'a');
    const parts = [];
    for (let i = 1; i <= partCount; i++) {
      const uploadResult = await s3
        .uploadPart({
          Bucket: bucket,
          Key: mpuKey,
          PartNumber: i,
          UploadId: uploadId,
          Body: partData,
        })
        .promise();
      parts.push({ PartNumber: i, ETag: uploadResult.ETag });
    }

    await s3
      .completeMultipartUpload({
        Bucket: bucket,
        Key: mpuKey,
        UploadId: uploadId,
        MultipartUpload: { Parts: parts },
      })
      .promise();
  });

  after(async () => {
    await s3.deleteObject({ Bucket: bucket, Key: mpuKey }).promise();
    await s3.deleteBucket({ Bucket: bucket }).promise();
  });

  it('should return TotalPartsCount for MPU object', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: mpuKey,
        ObjectAttributes: ['ObjectParts'],
      })
      .promise();

    assert(data.ObjectParts, 'ObjectParts should be present');
    assert.strictEqual(data.ObjectParts.TotalPartsCount, partCount);
  });

  it('should return TotalPartsCount along with other attributes for MPU object', async () => {
    const data = await s3
      .getObjectAttributes({
        Bucket: bucket,
        Key: mpuKey,
        ObjectAttributes: ['ETag', 'ObjectParts', 'ObjectSize', 'StorageClass'],
      })
      .promise();

    assert(data.ETag, 'ETag should be present');
    assert(data.ETag.includes(`-${partCount}`), `ETag should indicate MPU with ${partCount} parts`);
    assert(data.ObjectParts, 'ObjectParts should be present');
    assert.strictEqual(data.ObjectParts.TotalPartsCount, partCount);
    assert.strictEqual(data.ObjectSize, partSize * partCount);
    assert.strictEqual(data.StorageClass, 'STANDARD');
  });
});
