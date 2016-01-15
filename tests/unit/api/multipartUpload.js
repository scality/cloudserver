import assert from 'assert';
import crypto from 'crypto';

import async from 'async';
import { expect } from 'chai';
import { parseString } from 'xml2js';

import bucketPut from '../../../lib/api/bucketPut';
import completeMultipartUpload from '../../../lib/api/completeMultipartUpload';
import constants from '../../../constants';
import initiateMultipartUpload from '../../../lib/api/initiateMultipartUpload';
import metadata from '../metadataswitch';
import multipartDelete from '../../../lib/api/multipartDelete';
import objectPutPart from '../../../lib/api/objectPutPart';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();

const splitter = constants.splitter;
const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const mpuBucket = `mpu...${bucketName}`;
const postBody = [ new Buffer('I am a body'), ];


describe('Multipart Upload API', () => {
    let metastore;

    beforeEach((done) => {
        metastore = {
            "users": {
                "accessKey1": {
                    "buckets": []
                },
                "accessKey2": {
                    "buckets": []
                }
            },
            "buckets": {}
        };

        // Must delete real bucket and shadow mpu bucket
        metadata.deleteBucket(bucketName, log, () => {
            metadata.deleteBucket(mpuBucket, log, () => {
                done();
            });
        });
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => {
            metadata.deleteBucket(mpuBucket, log, () => {
                done();
            });
        });
    });


    it('should initiate a multipart upload', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: { host: `${bucketName}.s3.amazonaws.com` },
        };
        const initiateRequest = {
            lowerCaseHeaders: { host: `${bucketName}.s3.amazonaws.com` },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
        };

        bucketPut(accessKey, metastore, putRequest, log, () => {
            initiateMultipartUpload(accessKey, metastore, initiateRequest,
                log, (err, result) => {
                    assert.strictEqual(err, undefined);
                    parseString(result, (err, json) => {
                        expect(json.InitiateMultipartUploadResult
                            .Bucket[0]).to.equal(bucketName);
                        expect(json.InitiateMultipartUploadResult
                            .Key[0]).to.equal(objectKey);
                        assert(json.InitiateMultipartUploadResult.UploadId[0]);
                        metadata.getBucket(mpuBucket, log, (err, md) => {
                            assert.strictEqual(Object.keys(md.keyMap).length,
                                               1);
                            assert(Object.keys(md.keyMap)[0]
                            .startsWith(`overview${splitter}${objectKey}`));
                            done();
                        });
                    });
                });
        });
    });

    it('should upload a part', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object
                        .keys(md.keyMap).length, 1);
                    assert(Object.keys(md.keyMap)[0]
                        .startsWith(
                        `overview${splitter}${objectKey}`));
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log, (err) => {
                assert.strictEqual(err, null);
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    const keysInMPUkeyMap = Object.keys(md.keyMap);
                    const sortedKeyMap = keysInMPUkeyMap.sort((a) => {
                        if (a.slice(0, 8) === 'overview') {
                            return -1;
                        }
                    });
                    const overviewEntry = sortedKeyMap[0];
                    const partEntryArray = sortedKeyMap[1].split(splitter);
                    const partUploadId = partEntryArray[0];
                    const firstPartNumber = partEntryArray[1];
                    const partETag = partEntryArray[3];
                    expect(keysInMPUkeyMap).to.have.length(2);
                    expect(md.keyMap[overviewEntry].key)
                        .to.equal(objectKey);
                    assert.strictEqual(partUploadId, testUploadId);
                    expect(firstPartNumber).to.equal('1');
                    expect(partETag).to.equal(calculatedMD5);
                    done();
                });
            });
        });
    });

    it('should upload a part even if the client sent ' +
    'a base 64 ETag (and the stored ETag ' +
    'in metadata should be hex)', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(accessKey, metastore, initiateRequest,
                    log, next);
            },
            function waterfall3(result, next) {
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            const calculatedMD5 = md5Hash.update(bufferBody).digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log, (err) => {
                expect(err).to.be.null;
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    const keysInMPUkeyMap = Object.keys(md.keyMap);
                    const sortedKeyMap = keysInMPUkeyMap.sort((a) => {
                        if (a.slice(0, 8) === 'overview') {
                            return -1;
                        }
                    });
                    const partEntryArray = sortedKeyMap[1].split(splitter);
                    const partETag = partEntryArray[3];
                    expect(keysInMPUkeyMap).to.have.length(2);
                    expect(partETag).to.equal(calculatedMD5);
                    done();
                });
            });
        });
    });

    it('should return an error if too many parts', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '10001',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log,
                (err, result) => {
                    expect(err).to.equal('TooManyParts');
                    expect(result).to.be.undefined;
                    done();
                });
        });
    });

    it('should return an error if part number is not an integer', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: 'I am not an integer',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log,
                (err, result) => {
                    expect(err).to.equal('InvalidArgument');
                    expect(result).to.be.undefined;
                    done();
                });
        });
    });

    it('should return an error if content-length is too large', (done) => {
        // Note this is only faking a large file
        // by setting a large content-length.  It is not actually putting a
        // large file.  Functional tests will test actual large data.
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 5368709121,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log,
                (err, result) => {
                    expect(err).to.equal('EntityTooLarge');
                    expect(result).to.be.undefined;
                    done();
                });
        });
    });

    it('should upload two parts', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                        .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest1 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, log, () => {
                const postBody2 = [ new Buffer('I am a second part')];
                const md5Hash2 = crypto.createHash('md5');
                const bufferBody2 =
                    new Buffer(postBody2, 'binary');
                md5Hash2.update(bufferBody2);
                const secondCalculatedMD5 = md5Hash2.digest('hex');
                const partRequest2 = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?partNumber=` +
                        `1&uploadId=${testUploadId}`,
                    namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        partNumber: '2',
                        uploadId: testUploadId,
                    },
                    post: postBody2,
                    calculatedMD5: secondCalculatedMD5,
                };
                objectPutPart(accessKey, metastore, partRequest2, log,
                    (err) => {
                        expect(err).to.be.null;
                        metadata.getBucket(mpuBucket, log, (err, md) => {
                            const keysInMPUkeyMap = Object.keys(md.keyMap);
                            const sortedKeyMap = keysInMPUkeyMap.sort((a) => {
                                if (a.slice(0, 8) === 'overview') {
                                    return -1;
                                }
                            });
                            const overviewEntry = sortedKeyMap[0];
                            const secondPartEntryArray =
                                sortedKeyMap[2].split(splitter);
                            const partUploadId = secondPartEntryArray[0];
                            const secondPartNumber =
                                secondPartEntryArray[1];
                            const secondPartETag =
                                secondPartEntryArray[3];
                            expect(keysInMPUkeyMap).to.have.length(3);
                            expect(md.keyMap[overviewEntry].key)
                                .to.equal(objectKey);
                            expect(partUploadId).to.equal(testUploadId);
                            expect(secondPartNumber).to.equal('2');
                            expect(secondPartETag)
                                .to.equal(secondCalculatedMD5);
                            done();
                        });
                    });
            });
        });
    });

    it('should complete a multipart upload', (done) => {
        const objectKey = 'testObject';
        const partBody = [ new Buffer('I am a part\n') ];
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md.keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5').update(partBody[0]);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                // Note that the body of the post set in the request here does
                // not really matter in this test.
                // The put is not going through the route so the md5 is being
                // calculated above and manually being set in the request below.
                // What is being tested is that the calculatedMD5 being sent
                // to the API for the part is stored and then used to
                // calculate the final ETag upon completion
                // of the multipart upload.
                post: partBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log, () => {
                const completeBody = `<CompleteMultipartUpload>` +
                    `<Part>` +
                    `<PartNumber>1</PartNumber>` +
                    `<ETag>"${calculatedMD5}"</ETag>` +
                    `</Part>` +
                    `</CompleteMultipartUpload>`;
                const completeRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`,
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: {
                        uploadId: testUploadId,
                    },
                    post: completeBody,
                };
                const awsVerifiedETag =
                    '953e9e776f285afc0bfcf1ab4668299d-1';
                completeMultipartUpload(accessKey, metastore,
                    completeRequest, log, (err, result) => {
                        parseString(result, (err, json) => {
                            expect(json.CompleteMultipartUploadResult
                                .Location[0]).to.
                                equal(`http://${bucketName}.` +
                                `s3.amazonaws.com/${objectKey}`);
                            expect(json.CompleteMultipartUploadResult
                                .Bucket[0]).to.equal(bucketName);
                            expect(json.CompleteMultipartUploadResult
                                .Key[0]).to.equal(objectKey);
                            expect(json.CompleteMultipartUploadResult
                                .ETag[0]).to.equal(awsVerifiedETag);
                            metadata.getBucket(bucketName, log, (err, md) => {
                                assert(md.keyMap[objectKey]);
                                const MD = md.keyMap[objectKey];
                                assert.strictEqual(MD['x-amz-meta-stuff'],
                                                   'I am some user metadata');
                                done();
                            });
                        });
                    });
            });
        });
    });

    it('should return an error if a complete multipart upload' +
    ' request contains malformed xml', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                            .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log, () => {
                const completeBody = `Malformed xml`;
                const completeRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: testUploadId,
                    },
                    post: completeBody,
                    calculatedMD5,
                };
                completeMultipartUpload(accessKey, metastore,
                    completeRequest, log, (err) => {
                        expect(err).to.equal('MalformedXML');
                        metadata.getBucket(mpuBucket, log, (err, md) => {
                            assert.strictEqual(Object.keys(md
                                .keyMap).length, 2);
                            done();
                        });
                    });
            });
        });
    });

    it('should return an error if the complete ' +
    'multipart upload request contains xml that ' +
    'does not conform to the AWS spec', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                            .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log, () => {
                // XML is missing any part listing so does
                // not conform to the AWS spec
                const completeBody = `<CompleteMultipartUpload>` +
                    `</CompleteMultipartUpload>`;
                const completeRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: testUploadId,
                    },
                    post: completeBody,
                    calculatedMD5,
                };
                completeMultipartUpload(
                    accessKey, metastore,
                    completeRequest, log, (err) => {
                        expect(err).to.equal('MalformedPOSTRequest');
                        done();
                    });
            });
        });
    });

    it('should return an error if the complete ' +
    'multipart upload request contains xml with ' +
    'a part list that is not in numerical order', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                            .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest1 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, log, () => {
                objectPutPart(accessKey, metastore, partRequest2, log, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, log, (err) => {
                            expect(err).to.equal('InvalidPartOrder');
                            metadata.getBucket(mpuBucket, log, (err, md) => {
                                assert.strictEqual(Object.keys(md
                                    .keyMap).length, 3);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should return an error if the complete ' +
    'multipart upload request contains xml with ' +
    'a part ETag that does not match the md5 for ' +
    'the part that was actually sent', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                            .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const wrongMD5 = '3858f62230ac3c915f300c664312c11f-9';
            const partRequest1 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, log, () => {
                objectPutPart(accessKey, metastore, partRequest2, log, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>${wrongMD5}</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, log, (err) => {
                            expect(err).to.equal('InvalidPart');
                            metadata.getBucket(mpuBucket, log, (err, md) => {
                                assert.strictEqual(Object.keys(md
                                    .keyMap).length, 3);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should return an error if there is a part ' +
    'other than the last part that is less than 5MB ' +
    'in size', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                            .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest1 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 200,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, log, () => {
                objectPutPart(accessKey, metastore, partRequest2, log, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, log, (err) => {
                            expect(err).to.equal('EntityTooSmall');
                            metadata.getBucket(mpuBucket, log, (err, md) => {
                                assert.strictEqual(Object.keys(md
                                    .keyMap).length, 3);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should aggregate the sizes of the parts', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                            .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until her
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest1 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 6000000,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, log, () => {
                objectPutPart(accessKey, metastore, partRequest2, log, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, log, (err, result) => {
                            assert.strictEqual(err, null);
                            parseString(result, (err) => {
                                assert.strictEqual(err, null);
                                metadata.getBucket(bucketName, log,
                                    (err, md) => {
                                        const MD = md.keyMap[objectKey];
                                        assert.strictEqual(MD['content-length'],
                                        6000100);
                                        done();
                                    });
                            });
                        });
                });
            });
        });
    });

    it('should set a canned ACL for a multipart upload', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
                'x-amz-acl': 'authenticated-read',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md
                            .keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest1 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 6000000,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, log, () => {
                objectPutPart(accessKey, metastore, partRequest2, log, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, log, (err, result) => {
                            assert.strictEqual(err, null);
                            parseString(result, (err) => {
                                assert.strictEqual(err, null);
                                metadata.getBucket(bucketName, log,
                                    (err, md) => {
                                        const MD = md.keyMap[objectKey];
                                        assert.strictEqual(MD.acl.Canned,
                                            'authenticated-read');
                                        done();
                                    });
                            });
                        });
                });
            });
        });
    });

    it('should set specific ACL grants for a multipart upload', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const granteeId = '79a59df900b949e55d96a1e698fbace' +
            'dfd6e09d98eacf8f8d5218e7cd47ef2be';
        const granteeEmail = 'sampleAccount1@sampling.com';
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
                'x-amz-grant-read': `emailAddress="${granteeEmail}"`,
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md.keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const calculatedMD5 = md5Hash.digest('hex');
            const partRequest1 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 6000000,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, log, () => {
                objectPutPart(accessKey, metastore, partRequest2, log, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>"${calculatedMD5}"</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, log, (err, result) => {
                            assert.strictEqual(err, null);
                            parseString(result, (err) => {
                                assert.strictEqual(err, null);
                                metadata.getBucket(bucketName, log,
                                    (err, md) => {
                                        const MD = md.keyMap[objectKey];
                                        assert.strictEqual(MD.acl.READ[0],
                                            granteeId);
                                        done();
                                    });
                            });
                        });
                });
            });
        });
    });

    it('should abort/delete a multipart upload', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md.keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const bufferMD5 =
                new Buffer(postBody, 'base64');
            const calculatedMD5 = bufferMD5.toString('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log, () => {
                const deleteRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: testUploadId,
                    },
                };
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    expect(Object.keys(md.keyMap))
                    .to.have.length.of(2);
                    multipartDelete(
                    accessKey, metastore,
                    deleteRequest, log, (err) => {
                        assert.strictEqual(err, null);
                        metadata.getBucket(mpuBucket, log, (err, md) => {
                            expect(Object.keys(md.keyMap))
                            .to.have.length.of(0);
                            done();
                        });
                    });
                });
            });
        });
    });

    it('should return an error if attempt to abort/delete ' +
        'a multipart upload that does not exist', (done) => {
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, log, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, log, next);
            },
            function waterfall3(result, next) {
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md.keyMap).length, 1);
                    parseString(result, next);
                });
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const bufferMD5 =
                new Buffer(postBody, 'base64');
            const calculatedMD5 = bufferMD5.toString('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, log, () => {
                const deleteRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: {
                        uploadId: 'non-existent-upload-id',
                    },
                };
                metadata.getBucket(mpuBucket, log, (err, md) => {
                    assert.strictEqual(Object.keys(md.keyMap).length, 2);
                    multipartDelete(accessKey, metastore, deleteRequest,
                        log, err => {
                            assert.strictEqual(err, 'NoSuchUpload');
                            done();
                        });
                });
            });
        });
    });
});
