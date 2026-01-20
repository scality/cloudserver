const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils } = require('../helpers');
const completeMultipartUpload = require('../../../lib/api/completeMultipartUpload');
const DummyRequest = require('../DummyRequest');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const objectPut = require('../../../lib/api/objectPut');
const { objectDelete } = require('../../../lib/api/objectDelete');
const objectGetAttributes = require('../../../lib/api/objectGetAttributes');
const objectPutPart = require('../../../lib/api/objectPutPart');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const body = 'hello world!';
const postBody = Buffer.from(body, 'utf8');
const expectedMD5 = 'fc3ff98e8c6a0d3087d515c0473f8677';

describe('objectGetAttributes API', () => {
  let testPutObjectRequest;

  const testPutBucketRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${bucketName}`,
    actionImplicitDenies: false,
  };

  beforeEach(() => {
    cleanup();
    testPutObjectRequest = new DummyRequest(
      {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
          'content-length': `${postBody.length}`,
        },
        parsedContentLength: postBody.length,
        url: `/${bucketName}/${objectName}`,
      },
      postBody,
    );
  });

  const createGetAttributesRequest = (attributes, options = {}) => ({
    bucketName,
    namespace,
    objectKey: options.objectKey || objectName,
    headers: {
      'x-amz-object-attributes': attributes.join(','),
      ...options.headers,
    },
    url: `/${bucketName}/${options.objectKey || objectName}`,
    query: options.query || {},
    actionImplicitDenies: false,
  });

  it('should fail because attributes header is missing', done => {
    const testGetRequest = {
      bucketName,
      namespace,
      objectKey: objectName,
      headers: {},
      url: `/${bucketName}/${objectName}`,
      query: {},
      actionImplicitDenies: false,
    };

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, err => {
          assert.strictEqual(err.is.InvalidRequest, true);
          assert.strictEqual(
              err.description,
              'The x-amz-object-attributes header specifying the attributes ' +
              'to be retrieved is either missing or empty',
          );
          done();
        });
      });
    });
  });

  it('should fail because attributes header is empty', done => {
    const testGetRequest = {
      bucketName,
      namespace,
      objectKey: objectName,
      headers: {
        'x-amz-object-attributes': '',
      },
      url: `/${bucketName}/${objectName}`,
      query: {},
      actionImplicitDenies: false,
    };

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, err => {
          assert.strictEqual(err.is.InvalidRequest, true);
          assert.strictEqual(
              err.description,
              'The x-amz-object-attributes header specifying the attributes ' +
              'to be retrieved is either missing or empty',
          );
          done();
        });
      });
    });
  });

  it('should fail because attribute name is invalid', done => {
    const testGetRequest = createGetAttributesRequest(['InvalidAttribute']);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, err => {
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          done();
        });
      });
    });
  });

  it('should return NoSuchKey for non-existent object', done => {
    const testGetRequest = createGetAttributesRequest(['ETag'], {
      objectKey: 'nonexistent',
    });

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectGetAttributes(authInfo, testGetRequest, log, err => {
        assert.strictEqual(err.is.NoSuchKey, true);
        assert.strictEqual(err.description, 'The specified key does not exist.');
        done();
      });
    });
  });

  it('should fail because of bad bucket owner', done => {
    const testGetRequest = createGetAttributesRequest(['ETag'], {
      headers: {
        'x-amz-expected-bucket-owner': 'wrongAccountId',
      },
    });

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, err => {
          assert.strictEqual(err.is.AccessDenied, true);
          assert.strictEqual(err.description, 'Access Denied');
          done();
        });
      });
    });
  });

  it('should return all attributes', done => {
    const testGetRequest = createGetAttributesRequest([
        'ETag',
        'Checksum',
        'ObjectParts',
        'StorageClass',
        'ObjectSize',
    ]);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, (err, xml, headers) => {
          assert.ifError(err);
          assert(xml, 'Response XML should be present');
          assert(headers['Last-Modified'], 'Last-Modified header should be present');

          parseString(xml, (err, result) => {
            const response = result.GetObjectAttributesResponse;

            assert.ifError(err);
            assert.strictEqual(response.ETag[0], expectedMD5);
            assert.strictEqual(response.StorageClass[0], 'STANDARD');
            assert.strictEqual(response.ObjectSize[0], String(body.length));
            assert(response.Checksum, 'Checksum should be present');
            assert(!response.ObjectParts, 'ObjectParts should not be present for non-MPU object');
            assert(headers['Last-Modified'], 'LastModified should be present');
            done();
          });
        });
      });
    });
  });

  it('should return ETag', done => {
    const testGetRequest = createGetAttributesRequest(['ETag']);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, (err, xml) => {
          assert.ifError(err);
          parseString(xml, (err, result) => {
            assert.ifError(err);
            assert.strictEqual(result.GetObjectAttributesResponse.ETag[0], expectedMD5);
            done();
          });
        });
      });
    });
  });

  it('should return Checksum', done => {
    const testGetRequest = createGetAttributesRequest(['Checksum']);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, (err, xml) => {
          assert.ifError(err);
          parseString(xml, (err, result) => {
            assert.ifError(err);
            assert(result.GetObjectAttributesResponse.Checksum, 'Checksum should be present');
            done();
          });
        });
      });
    });
  });

  it('should not return ObjectParts for non-MPU object', done => {
    const testGetRequest = createGetAttributesRequest(['ObjectParts']);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, (err, xml) => {
          assert.ifError(err);
          parseString(xml, (err, result) => {
            assert.ifError(err);
            assert(!result.GetObjectAttributesResponse.ObjectParts, 'ObjectParts should not be present');
            done();
          });
        });
      });
    });
  });

  it('should return StorageClass', done => {
    const testGetRequest = createGetAttributesRequest(['StorageClass']);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, (err, xml) => {
          assert.ifError(err);
          parseString(xml, (err, result) => {
            assert.ifError(err);
            assert.strictEqual(result.GetObjectAttributesResponse.StorageClass[0], 'STANDARD');
            done();
          });
        });
      });
    });
  });

  it('should return ObjectSize', done => {
    const testGetRequest = createGetAttributesRequest(['ObjectSize']);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, (err, xml) => {
          assert.ifError(err);
          parseString(xml, (err, result) => {
            assert.ifError(err);
            assert.strictEqual(result.GetObjectAttributesResponse.ObjectSize[0], String(body.length));
            done();
          });
        });
      });
    });
  });

  it('should return LastModified in response headers', done => {
    const testGetRequest = createGetAttributesRequest(['ETag']);

    bucketPut(authInfo, testPutBucketRequest, log, () => {
      objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
        assert.ifError(err);
        objectGetAttributes(authInfo, testGetRequest, log, (err, _xml, headers) => {
          assert.ifError(err);
          assert(headers['Last-Modified'], 'Last-Modified should be present');
          assert(!isNaN(new Date(headers['Last-Modified']).getTime()), 'Last-Modified should be a valid date');
          done();
        });
      });
    });
  });
});

describe('objectGetAttributes API with multipart upload', () => {
  const mpuObjectName = 'mpuObject';
  const partCount = 2;
  const partBody = Buffer.from('I am a part\n', 'utf8');

  const testPutBucketRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${bucketName}`,
    actionImplicitDenies: false,
  };

  beforeEach(done => {
    cleanup();
    bucketPut(authInfo, testPutBucketRequest, log, done);
  });

  const createMpuObject = callback => {
    const initiateRequest = {
      bucketName,
      namespace,
      objectKey: mpuObjectName,
      headers: { host: `${bucketName}.s3.amazonaws.com` },
      url: `/${mpuObjectName}?uploads`,
      actionImplicitDenies: false,
    };

    async.waterfall(
      [
        next => initiateMultipartUpload(authInfo, initiateRequest, log, next),
        (result, _corsHeaders, next) => parseString(result, next),
        (json, next) => {
          const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
          const partHash = crypto.createHash('md5').update(partBody).digest('hex');

          // Upload first part (minimum 5MB for non-last part)
          const part1Request = new DummyRequest(
            {
              bucketName,
              namespace,
              objectKey: mpuObjectName,
              headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'content-length': '5242880',
              },
              parsedContentLength: 5242880,
              url: `/${mpuObjectName}?partNumber=1&uploadId=${testUploadId}`,
              query: {
                partNumber: '1',
                uploadId: testUploadId,
              },
              partHash,
            },
            partBody,
          );

          objectPutPart(authInfo, part1Request, undefined, log, () => {
            next(null, testUploadId, partHash);
          });
        },
        (testUploadId, partHash, next) => {
          // Upload second part
          const part2Request = new DummyRequest(
            {
              bucketName,
              namespace,
              objectKey: mpuObjectName,
              headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'content-length': `${partBody.length}`,
              },
              parsedContentLength: partBody.length,
              url: `/${mpuObjectName}?partNumber=2&uploadId=${testUploadId}`,
              query: {
                partNumber: '2',
                uploadId: testUploadId,
              },
              partHash,
            },
            partBody,
          );

          objectPutPart(authInfo, part2Request, undefined, log, () => {
            next(null, testUploadId, partHash);
          });
        },
        (testUploadId, partHash, next) => {
          // Complete the multipart upload
          const completeBody =
            '<CompleteMultipartUpload>' +
            '<Part>' +
            '<PartNumber>1</PartNumber>' +
            `<ETag>"${partHash}"</ETag>` +
            '</Part>' +
            '<Part>' +
            '<PartNumber>2</PartNumber>' +
            `<ETag>"${partHash}"</ETag>` +
            '</Part>' +
            '</CompleteMultipartUpload>';

          const completeRequest = {
            bucketName,
            namespace,
            objectKey: mpuObjectName,
            parsedHost: 's3.amazonaws.com',
            url: `/${mpuObjectName}?uploadId=${testUploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: { uploadId: testUploadId },
            post: completeBody,
            actionImplicitDenies: false,
          };

          completeMultipartUpload(authInfo, completeRequest, log, err => {
            next(err);
          });
        },
      ],
      callback,
    );
  };

  const createGetAttributesRequest = attributes => ({
    bucketName,
    namespace,
    objectKey: mpuObjectName,
    headers: {
      'x-amz-object-attributes': attributes.join(','),
    },
    url: `/${bucketName}/${mpuObjectName}`,
    query: {},
    actionImplicitDenies: false,
  });

  it('should return TotalPartsCount for MPU object', done => {
    createMpuObject(err => {
      assert.ifError(err);
      const testGetRequest = createGetAttributesRequest(['ObjectParts']);

      objectGetAttributes(authInfo, testGetRequest, log, (err, xml) => {
        assert.ifError(err);
        parseString(xml, (err, result) => {
          const response = result.GetObjectAttributesResponse;

          assert.ifError(err);
          assert(response.ObjectParts, 'ObjectParts should be present');
          assert.strictEqual(response.ObjectParts[0].PartsCount[0], String(partCount));
          done();
        });
      });
    });
  });

  it('should return TotalPartsCount along with other attributes for MPU object', done => {
    createMpuObject(err => {
      assert.ifError(err);
      const testGetRequest = createGetAttributesRequest(['ETag', 'ObjectParts', 'ObjectSize', 'StorageClass']);

      objectGetAttributes(authInfo, testGetRequest, log, (err, xml) => {
        assert.ifError(err);
        parseString(xml, (err, result) => {
          const response = result.GetObjectAttributesResponse;

          assert.ifError(err);
          assert(response.ETag, 'ETag should be present');
          assert(response.ETag[0].includes(`-${partCount}`), `ETag should indicate MPU with ${partCount} parts`);
          assert(response.ObjectParts, 'ObjectParts should be present');
          assert.strictEqual(response.ObjectParts[0].PartsCount[0], String(partCount));
          assert(response.ObjectSize, 'ObjectSize should be present');
          assert.strictEqual(response.StorageClass[0], 'STANDARD');
          done();
        });
      });
    });
  });
});

describe('objectGetAttributes API with versioning', () => {
  const enableVersioningRequest = versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');

  const testPutBucketRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${bucketName}`,
    actionImplicitDenies: false,
  };

  beforeEach(done => {
    cleanup();
    async.series(
      [
          next => bucketPut(authInfo, testPutBucketRequest, log, next),
          next => bucketPutVersioning(authInfo, enableVersioningRequest, log, next),
      ],
      done,
    );
  });

  const createGetAttributesRequest = (attributes, options = {}) => ({
    bucketName,
    namespace,
    objectKey: options.objectKey || objectName,
    headers: {
      'x-amz-object-attributes': attributes.join(','),
      ...options.headers,
    },
    url: `/${bucketName}/${options.objectKey || objectName}`,
    query: options.query || {},
    actionImplicitDenies: false,
  });

  it('should return NoSuchVersion for non-existent versionId', done => {
    const testPutObjectRequest = new DummyRequest(
      {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
          'content-length': `${postBody.length}`,
        },
        parsedContentLength: postBody.length,
        url: `/${bucketName}/${objectName}`,
      },
      postBody,
    );

    // Use a properly formatted but non-existent version ID
    const fakeVersionId = '111111111111111111111111111111111111111175636f7270';

    objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
      assert.ifError(err);
      const testGetRequest = createGetAttributesRequest(['ETag'], {
        query: { versionId: fakeVersionId },
      });

      objectGetAttributes(authInfo, testGetRequest, log, err => {
        assert.strictEqual(err.is.NoSuchVersion, true);
        assert.strictEqual(
            err.description,
            'Indicates that the version ID specified in the request does not match an existing version.',
        );
        done();
      });
    });
  });

  it('should return MethodNotAllowed for delete marker', done => {
    const testPutObjectRequest = new DummyRequest(
      {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
          'content-length': `${postBody.length}`,
        },
        parsedContentLength: postBody.length,
        url: `/${bucketName}/${objectName}`,
      },
      postBody,
    );

    const testDeleteRequest = {
      bucketName,
      namespace,
      objectKey: objectName,
      headers: {},
      url: `/${bucketName}/${objectName}`,
      actionImplicitDenies: false,
    };

    async.series(
      [
          next => objectPut(authInfo, testPutObjectRequest, undefined, log, next),
          next => objectDelete(authInfo, testDeleteRequest, log, next),
      ],
      err => {
        assert.ifError(err);
        // Request without versionId targets the delete marker
        const testGetRequest = createGetAttributesRequest(['ETag']);

        objectGetAttributes(authInfo, testGetRequest, log, (err, _xml, headers) => {
          assert.strictEqual(err.is.MethodNotAllowed, true);
          assert.strictEqual(err.description, 'The specified method is not allowed against this resource.');
          assert.strictEqual(headers['x-amz-delete-marker'], true);
          done();
        });
      },
    );
  });

  it('should return attributes for specific version', done => {
    const testPutObjectRequest = new DummyRequest(
      {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
          'content-length': `${postBody.length}`,
        },
        parsedContentLength: postBody.length,
        url: `/${bucketName}/${objectName}`,
      },
      postBody,
    );

    objectPut(authInfo, testPutObjectRequest, undefined, log, (err, resHeaders) => {
      assert.ifError(err);
      const versionId = resHeaders['x-amz-version-id'];
      assert(versionId, 'Version ID should be present');

      const testGetRequest = createGetAttributesRequest(['ETag', 'ObjectSize'], {
        query: { versionId },
      });

      objectGetAttributes(authInfo, testGetRequest, log, (err, xml, headers) => {
        assert.ifError(err);
        assert(headers['Last-Modified'], 'Last-Modified should be present');

        parseString(xml, (err, result) => {
          const response = result.GetObjectAttributesResponse;

          assert.ifError(err);
          assert.strictEqual(response.ETag[0], expectedMD5);
          assert.strictEqual(response.ObjectSize[0], String(body.length));
          done();
        });
      });
    });
  });

  it('should return VersionId in response headers for versioned object', done => {
    const testPutObjectRequest = new DummyRequest(
      {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
          'content-length': `${postBody.length}`,
        },
        parsedContentLength: postBody.length,
        url: `/${bucketName}/${objectName}`,
      },
      postBody,
    );

    objectPut(authInfo, testPutObjectRequest, undefined, log, (err, resHeaders) => {
      assert.ifError(err);
      const versionId = resHeaders['x-amz-version-id'];
      assert(versionId, 'Version ID should be present from PUT');

      const testGetRequest = createGetAttributesRequest(['ETag']);

      objectGetAttributes(authInfo, testGetRequest, log, (err, _xml, headers) => {
        assert.ifError(err);
        assert.strictEqual(headers['x-amz-version-id'], versionId);
        done();
      });
    });
  });
});
