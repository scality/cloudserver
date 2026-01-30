const assert = require('assert');
const crypto = require('crypto');
const { parseStringPromise } = require('xml2js');

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

// Promisify helper for functions with non-standard callback signatures
const promisify = fn => (...args) => new Promise((resolve, reject) => {
    fn(...args, (err, ...results) => {
        if (err) {
            reject(err);
        } else {
            resolve(results);
        }
    });
});

const bucketPutAsync = promisify(bucketPut);
const bucketPutVersioningAsync = promisify(bucketPutVersioning);
const objectPutAsync = promisify(objectPut);
const objectDeleteAsync = promisify(objectDelete);
const objectGetAttributesAsync = promisify(objectGetAttributes);
const initiateMultipartUploadAsync = promisify(initiateMultipartUpload);
const objectPutPartAsync = promisify(objectPutPart);
const completeMultipartUploadAsync = promisify(completeMultipartUpload);

const testPutBucketRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${bucketName}`,
    actionImplicitDenies: false,
};

const createGetAttributesRequest = (attributes, options = {}) => {
    const key = options.objectKey || objectName;
    return {
        bucketName,
        namespace,
        objectKey: key,
        headers: {
            'x-amz-object-attributes': attributes.join(','),
            ...options.headers,
        },
        url: `/${bucketName}/${key}`,
        query: options.query || {},
        actionImplicitDenies: false,
    };
};

describe('objectGetAttributes API', () => {
    beforeEach(async () => {
        cleanup();
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
        await bucketPutAsync(authInfo, testPutBucketRequest, log);
        await objectPutAsync(authInfo, testPutObjectRequest, undefined, log);
    });

    it('should fail because attributes header is missing', async () => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            query: {},
            actionImplicitDenies: false,
        };

        try {
            await objectGetAttributesAsync(authInfo, testGetRequest, log);
            assert.fail('Expected error was not thrown');
        } catch (err) {
            assert.strictEqual(err.is.InvalidRequest, true);
            assert.strictEqual(
                err.description,
                'The x-amz-object-attributes header specifying the attributes ' +
                'to be retrieved is either missing or empty',
            );
        }
    });

    it('should fail because attributes header is empty', async () => {
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

        try {
            await objectGetAttributesAsync(authInfo, testGetRequest, log);
            assert.fail('Expected error was not thrown');
        } catch (err) {
            assert.strictEqual(err.is.InvalidArgument, true);
            assert.strictEqual(err.description, 'Invalid attribute name specified.');
        }
    });

    it('should fail because attribute name is invalid', async () => {
        const testGetRequest = createGetAttributesRequest(['InvalidAttribute']);

        try {
            await objectGetAttributesAsync(authInfo, testGetRequest, log);
            assert.fail('Expected error was not thrown');
        } catch (err) {
            assert.strictEqual(err.is.InvalidArgument, true);
            assert.strictEqual(err.description, 'Invalid attribute name specified.');
        }
    });

    it('should return NoSuchKey for non-existent object', async () => {
        const testGetRequest = createGetAttributesRequest(['ETag'], {
            objectKey: 'nonexistent',
        });

        try {
            await objectGetAttributesAsync(authInfo, testGetRequest, log);
            assert.fail('Expected error was not thrown');
        } catch (err) {
            assert.strictEqual(err.is.NoSuchKey, true);
            assert.strictEqual(err.description, 'The specified key does not exist.');
        }
    });

    it('should fail because of bad bucket owner', async () => {
        const testGetRequest = createGetAttributesRequest(['ETag'], {
            headers: {
                'x-amz-expected-bucket-owner': 'wrongAccountId',
            },
        });

        try {
            await objectGetAttributesAsync(authInfo, testGetRequest, log);
            assert.fail('Expected error was not thrown');
        } catch (err) {
            assert.strictEqual(err.is.AccessDenied, true);
            assert.strictEqual(err.description, 'Access Denied');
        }
    });

    it('should return all attributes', async () => {
        const testGetRequest = createGetAttributesRequest([
            'ETag',
            'Checksum',
            'ObjectParts',
            'StorageClass',
            'ObjectSize',
        ]);

        const [xml, headers] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        assert(xml, 'Response XML should be present');
        assert(headers['Last-Modified'], 'Last-Modified header should be present');

        const result = await parseStringPromise(xml);
        const response = result.GetObjectAttributesResponse;

        assert.strictEqual(response.ETag[0], expectedMD5);
        assert.strictEqual(response.StorageClass[0], 'STANDARD');
        assert.strictEqual(response.ObjectSize[0], String(body.length));
        assert.deepStrictEqual(response.Checksum[0], '', 'Checksum should be empty');
        assert.strictEqual(response.ObjectParts, undefined, "ObjectParts shouldn't be present for non-MPU object");
        assert(headers['Last-Modified'], 'LastModified should be present');
    });

    it('should return ETag', async () => {
        const testGetRequest = createGetAttributesRequest(['ETag']);

        const [xml] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        const result = await parseStringPromise(xml);
        assert.strictEqual(result.GetObjectAttributesResponse.ETag[0], expectedMD5);
    });

    it('should return Checksum', async () => {
        const testGetRequest = createGetAttributesRequest(['Checksum']);

        const [xml] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        const result = await parseStringPromise(xml);
        assert.deepStrictEqual(result.GetObjectAttributesResponse.Checksum[0], '', 'Checksum should be empty');
    });

    it("shouldn't return ObjectParts for non-MPU object", async () => {
        const testGetRequest = createGetAttributesRequest(['ObjectParts']);

        const [xml] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        const result = await parseStringPromise(xml);
        assert.strictEqual(
            result.GetObjectAttributesResponse.ObjectParts,
            undefined,
            "ObjectParts shouldn't be present",
        );
    });

    it('should return StorageClass', async () => {
        const testGetRequest = createGetAttributesRequest(['StorageClass']);

        const [xml] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        const result = await parseStringPromise(xml);
        assert.strictEqual(result.GetObjectAttributesResponse.StorageClass[0], 'STANDARD');
    });

    it('should return ObjectSize', async () => {
        const testGetRequest = createGetAttributesRequest(['ObjectSize']);

        const [xml] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        const result = await parseStringPromise(xml);
        assert.strictEqual(result.GetObjectAttributesResponse.ObjectSize[0], String(body.length));
    });

    it('should return LastModified in response headers', async () => {
        const testGetRequest = createGetAttributesRequest(['ETag']);

        const [, headers] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        assert(headers['Last-Modified'], 'Last-Modified should be present');
        assert(!isNaN(new Date(headers['Last-Modified']).getTime()), 'Last-Modified should be a valid date');
    });
});

describe('objectGetAttributes API with multipart upload', () => {
    const partCount = 2;
    const partBody = Buffer.from('I am a part\n', 'utf8');

    const createMpuObject = async () => {
        const initiateRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: `/${objectName}?uploads`,
            actionImplicitDenies: false,
        };

        const [result] = await initiateMultipartUploadAsync(authInfo, initiateRequest, log);
        const json = await parseStringPromise(result);
        const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
        const partHash = crypto.createHash('md5').update(partBody).digest('hex');

        const completeParts = [];
        for (let i = 1; i <= partCount; i++) {
            const partRequest = new DummyRequest(
                {
                    bucketName,
                    namespace,
                    objectKey: objectName,
                    headers: {
                        host: `${bucketName}.s3.amazonaws.com`,
                        'content-length': '5242880',
                    },
                    parsedContentLength: 5242880,
                    url: `/${objectName}?partNumber=${i}&uploadId=${testUploadId}`,
                    query: {
                        partNumber: String(i),
                        uploadId: testUploadId,
                    },
                    partHash,
                },
                partBody,
            );
            await objectPutPartAsync(authInfo, partRequest, undefined, log);
            completeParts.push(`<Part><PartNumber>${i}</PartNumber><ETag>"${partHash}"</ETag></Part>`);
        }

        const completeBody =
            `<CompleteMultipartUpload>${completeParts.join('')}</CompleteMultipartUpload>`;

        const completeRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            parsedHost: 's3.amazonaws.com',
            url: `/${objectName}?uploadId=${testUploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: { uploadId: testUploadId },
            post: completeBody,
            actionImplicitDenies: false,
        };

        await completeMultipartUploadAsync(authInfo, completeRequest, log);
    };

    beforeEach(async () => {
        cleanup();
        await bucketPutAsync(authInfo, testPutBucketRequest, log);
        await createMpuObject();
    });

    it('should return TotalPartsCount for MPU object', async () => {
        const testGetRequest = createGetAttributesRequest(['ObjectParts']);

        const [xml] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        const result = await parseStringPromise(xml);
        const response = result.GetObjectAttributesResponse;

        assert(response.ObjectParts, 'ObjectParts should be present');
        assert.strictEqual(response.ObjectParts[0].PartsCount[0], String(partCount));
    });

    it('should return TotalPartsCount along with other attributes for MPU object', async () => {
        const testGetRequest = createGetAttributesRequest(['ETag', 'ObjectParts', 'ObjectSize', 'StorageClass']);

        const [xml] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        const result = await parseStringPromise(xml);
        const response = result.GetObjectAttributesResponse;

        assert(response.ETag, 'ETag should be present');
        assert(response.ETag[0].includes(`-${partCount}`), `ETag should indicate MPU with ${partCount} parts`);
        assert(response.ObjectParts, 'ObjectParts should be present');
        assert.strictEqual(response.ObjectParts[0].PartsCount[0], String(partCount));
        assert(response.ObjectSize, 'ObjectSize should be present');
        assert.strictEqual(response.StorageClass[0], 'STANDARD');
    });
});

describe('objectGetAttributes API with versioning', () => {
    const enableVersioningRequest = versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');

    beforeEach(async () => {
        cleanup();
        await bucketPutAsync(authInfo, testPutBucketRequest, log);
        await bucketPutVersioningAsync(authInfo, enableVersioningRequest, log);
    });

    it('should return NoSuchVersion for non-existent versionId', async () => {
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

        const fakeVersionId = '111111111111111111111111111111111111111175636f7270';

        await objectPutAsync(authInfo, testPutObjectRequest, undefined, log);
        const testGetRequest = createGetAttributesRequest(['ETag'], {
            query: { versionId: fakeVersionId },
        });

        try {
            await objectGetAttributesAsync(authInfo, testGetRequest, log);
            assert.fail('Expected error was not thrown');
        } catch (err) {
            assert.strictEqual(err.is.NoSuchVersion, true);
            assert.strictEqual(
                err.description,
                'Indicates that the version ID specified in the request does not match an existing version.',
            );
        }
    });

    it('should return MethodNotAllowed for delete marker', async () => {
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

        await objectPutAsync(authInfo, testPutObjectRequest, undefined, log);
        await objectDeleteAsync(authInfo, testDeleteRequest, log);

        const testGetRequest = createGetAttributesRequest(['ETag']);

        try {
            await objectGetAttributesAsync(authInfo, testGetRequest, log);
            assert.fail('Expected error was not thrown');
        } catch (err) {
            assert.strictEqual(err.is.MethodNotAllowed, true);
            assert.strictEqual(err.description, 'The specified method is not allowed against this resource.');
            assert.strictEqual(err.responseHeaders['x-amz-delete-marker'], true);
        }
    });

    it('should return attributes for specific version', async () => {
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

        const [resHeaders] = await objectPutAsync(authInfo, testPutObjectRequest, undefined, log);
        const versionId = resHeaders['x-amz-version-id'];
        assert(versionId, 'Version ID should be present');

        const testGetRequest = createGetAttributesRequest(['ETag', 'ObjectSize'], {
            query: { versionId },
        });

        const [xml, headers] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        assert(headers['Last-Modified'], 'Last-Modified should be present');

        const result = await parseStringPromise(xml);
        const response = result.GetObjectAttributesResponse;

        assert.strictEqual(response.ETag[0], expectedMD5);
        assert.strictEqual(response.ObjectSize[0], String(body.length));
    });

    it('should return VersionId in response headers for versioned object', async () => {
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

        const [resHeaders] = await objectPutAsync(authInfo, testPutObjectRequest, undefined, log);
        const versionId = resHeaders['x-amz-version-id'];
        assert(versionId, 'Version ID should be present from PUT');

        const testGetRequest = createGetAttributesRequest(['ETag']);

        const [, headers] = await objectGetAttributesAsync(authInfo, testGetRequest, log);
        assert.strictEqual(headers['x-amz-version-id'], versionId);
    });
});
