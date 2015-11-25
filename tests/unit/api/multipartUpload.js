import async from 'async';
import crypto from 'crypto';
import { expect } from 'chai';
import { parseString } from 'xml2js';

import bucketPut from '../../../lib/api/bucketPut';
import initiateMultipartUpload from '../../../lib/api/initiateMultipartUpload';
import objectPutPart from '../../../lib/api/objectPutPart';
import completeMultipartUpload from '../../../lib/api/completeMultipartUpload';
import multipartDelete from '../../../lib/api/multipartDelete';
import config from '../../../config';
const splitter = config.splitter;

const accessKey = 'accessKey1';
const namespace = 'default';


describe('Multipart Upload API', () => {
    let metastore;

    beforeEach(() => {
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
    });


    it('should initiate a multipart upload', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        bucketPut(accessKey, metastore, putRequest, () => {
            initiateMultipartUpload(accessKey, metastore, initiateRequest,
                    (err, result) => {
                        expect(err).to.be.undefined;
                        expect(Object.keys(metastore.buckets[mpuBucket]
                            .keyMap)).to.have.length.of(1);
                        parseString(result, (err, json) => {
                            expect(json.InitiateMultipartUploadResult
                                .Bucket[0]).to.equal(bucketName);
                            expect(json.InitiateMultipartUploadResult
                                .Key[0]).to.equal(objectKey);
                            expect(json.InitiateMultipartUploadResult
                                .UploadId[0]).to.exist;
                            done();
                        });
                    });
        });
    });

    it('should upload a part', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, (err) => {
                expect(err).to.be.null;
                const keysInMPUkeyMap = Object.keys(metastore
                    .buckets[mpuBucket].keyMap);
                const sortedKeyMap = keysInMPUkeyMap.sort((a) => {
                    if (a.slice(0, 8) === 'overview') {
                        return -1;
                    }
                });
                const overviewEntry = sortedKeyMap[0];
                const partEntryArray = sortedKeyMap[1].split(splitter);
                const partUploadId = partEntryArray[0];
                const firstPartNumber = partEntryArray[1];
                const partEtag = partEntryArray[3];
                expect(keysInMPUkeyMap).to.have.length(2);
                expect(metastore.buckets[mpuBucket]
                    .keyMap[overviewEntry].key)
                    .to.equal(objectKey);
                expect(partUploadId).to.equal(testUploadId);
                expect(firstPartNumber).to.equal('1');
                expect(partEtag).to.equal(calculatedMD5);
                done();
            });
        });
    });

    it('should upload a part even if the client sent ' +
    'a base 64 etag (and the stored etag ' +
    'in metadata should be hex)', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(accessKey, metastore,
                                        initiateRequest, next);
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
            const postBody = 'I am a part';
            const md5Hash = crypto.createHash('md5');
            const bufferBody =
                new Buffer(postBody, 'binary');
            md5Hash.update(bufferBody);
            const base64MD5 = md5Hash.digest('base64');
            let hexMD5 = new Buffer(base64MD5, 'base64');
            hexMD5 = hexMD5.toString('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: base64MD5,
            };
            objectPutPart(accessKey, metastore, partRequest, (err) => {
                expect(err).to.be.null;
                expect(err).to.be.null;
                const keysInMPUkeyMap = Object.keys(metastore
                    .buckets[mpuBucket].keyMap);
                const sortedKeyMap = keysInMPUkeyMap.sort((a) => {
                    if (a.slice(0, 8) === 'overview') {
                        return -1;
                    }
                });
                const partEntryArray = sortedKeyMap[1].split(splitter);
                const partEtag = partEntryArray[3];
                expect(keysInMPUkeyMap).to.have.length(2);
                expect(partEtag).to.equal(hexMD5);
                done();
            });
        });
    });

    it('should return an error if too many parts', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
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
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '10001',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, (err, result) => {
                expect(err).to.equal('TooManyParts');
                expect(result).to.be.undefined;
                done();
            });
        });
    });

    it('should return an error if part number is not an integer', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
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
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: 'I am not an integer',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, (err, result) => {
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
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
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
            const postBody = 'I am faking a big post';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, (err, result) => {
                expect(err).to.equal('EntityTooLarge');
                expect(result).to.be.undefined;
                done();
            });
        });
    });

    it('should upload two parts', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a first part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, () => {
                const postBody2 = 'I am a second part';
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
                    namespace: namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        partNumber: '2',
                        uploadId: testUploadId,
                    },
                    post: postBody2,
                    calculatedMD5: secondCalculatedMD5,
                };
                objectPutPart(accessKey, metastore,
                    partRequest2, (err) => {
                        expect(err).to.be.null;
                        const keysInMPUkeyMap = Object.keys(metastore
                            .buckets[mpuBucket].keyMap);
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
                        const secondPartEtag =
                            secondPartEntryArray[3];
                        expect(keysInMPUkeyMap).to.have.length(3);
                        expect(metastore.buckets[mpuBucket]
                            .keyMap[overviewEntry].key)
                            .to.equal(objectKey);
                        expect(partUploadId).to.equal(testUploadId);
                        expect(secondPartNumber).to.equal('2');
                        expect(secondPartEtag)
                            .to.equal(secondCalculatedMD5);
                        done();
                    });
            });
        });
    });

    it('should complete a multipart upload', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part\n';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, () => {
                const completeBody = `<CompleteMultipartUpload>` +
                    `<Part>` +
                    `<PartNumber>1</PartNumber>` +
                    `<ETag>${calculatedMD5}</ETag>` +
                    `</Part>` +
                    `</CompleteMultipartUpload>`;
                const completeRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace: namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: testUploadId,
                    },
                    post: completeBody,
                    calculatedMD5: calculatedMD5,
                };
                const awsVerifiedEtag =
                    '953e9e776f285afc0bfcf1ab4668299d-1';
                completeMultipartUpload(
                    accessKey, metastore,
                    completeRequest, (err, result) => {
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
                                .ETag[0]).to.equal(awsVerifiedEtag);
                            expect(metastore.buckets[bucketUID]
                                .keyMap[objectKey]).to.exist;
                            expect(metastore.buckets[bucketUID]
                                .keyMap[objectKey]['x-amz-meta-stuff'])
                                .to.equal('I am some user metadata');
                            done();
                        });
                    });
            });
        });
    });

    it('should return an error if a complete multipart upload' +
    ' request contains malformed xml', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, () => {
                const completeBody = `Malformed xml`;
                const completeRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace: namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: testUploadId,
                    },
                    post: completeBody,
                    calculatedMD5: calculatedMD5,
                };
                completeMultipartUpload(accessKey, metastore,
                    completeRequest, (err) => {
                        expect(err).to.equal('MalformedXML');
                        done();
                    });
            });
        });
    });

    it('should return an error if the complete ' +
    'multipart upload request contains xml that ' +
    'does not conform to the AWS spec', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, () => {
                // XML is missing any part listing so does
                // not conform to the AWS spec
                const completeBody = `<CompleteMultipartUpload>` +
                    `</CompleteMultipartUpload>`;
                const completeRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace: namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: testUploadId,
                    },
                    post: completeBody,
                    calculatedMD5: calculatedMD5,
                };
                completeMultipartUpload(
                    accessKey, metastore,
                    completeRequest, (err) => {
                        expect(err).to.equal('MalformedPOSTRequest');
                        done();
                    });
            });
        });
    });

    it('should return an error if the complete ' +
    'multipart upload request contains xml with ' +
    'a part list that is not in numerical order', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, () => {
                objectPutPart(accessKey, metastore, partRequest2, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace: namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5: calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, (err) => {
                            expect(err).to.equal('InvalidPartOrder');
                            expect(Object.keys(metastore
                                .buckets[mpuBucket]
                                .keyMap)).to.have.length.of(3);
                            done();
                        });
                });
            });
        });
    });

    it('should return an error if the complete ' +
    'multipart upload request contains xml with ' +
    'a part etag that does not match the md5 for ' +
    'the part that was actually sent', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, () => {
                objectPutPart(accessKey, metastore, partRequest2, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
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
                        namespace: namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5: calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, (err) => {
                            expect(err).to.equal('InvalidPart');
                            expect(Object.keys(metastore
                                .buckets[mpuBucket]
                                .keyMap)).to.have.length.of(3);
                            done();
                        });
                });
            });
        });
    });

    it('should return an error if there is a part ' +
    'other than the last part that is less than 5MB ' +
    'in size', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 200,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, () => {
                objectPutPart(accessKey, metastore, partRequest2, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace: namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5: calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, (err) => {
                            expect(err).to.equal('EntityTooSmall');
                            expect(Object.keys(metastore
                                .buckets[mpuBucket]
                                .keyMap)).to.have.length.of(3);
                            done();
                        });
                });
            });
        });
    });

    it('should aggregate the sizes of the parts', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until her
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, () => {
                objectPutPart(accessKey, metastore, partRequest2, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace: namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5: calculatedMD5,
                    };
                    completeMultipartUpload(
                        accessKey, metastore,
                        completeRequest, (err, result) => {
                            expect(err).to.equal(null);
                            parseString(result, (err) => {
                                expect(err).to.equal(null);
                                expect(metastore.buckets[bucketUID]
                                    .keyMap[objectKey]
                                    ['content-length'])
                                    .to.equal(6000100);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should set a canned ACL for a multipart upload', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
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
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, () => {
                objectPutPart(accessKey, metastore, partRequest2, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace: namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5: calculatedMD5,
                    };
                    completeMultipartUpload(
                        accessKey, metastore,
                        completeRequest, (err, result) => {
                            expect(err).to.equal(null);
                            parseString(result, (err) => {
                                expect(err).to.equal(null);
                                expect(metastore.buckets[bucketUID]
                                    .keyMap[objectKey].acl.Canned)
                                    .to.equal('authenticated-read');
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should set specific ACL grants for a multipart upload', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
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
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
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
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            const partRequest2 = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest1, () => {
                objectPutPart(accessKey, metastore, partRequest2, () => {
                    const completeBody = `<CompleteMultipartUpload>` +
                        `<Part>` +
                        `<PartNumber>1</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `<Part>` +
                        `<PartNumber>2</PartNumber>` +
                        `<ETag>${calculatedMD5}</ETag>` +
                        `</Part>` +
                        `</CompleteMultipartUpload>`;
                    const completeRequest = {
                        lowerCaseHeaders: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        namespace: namespace,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`
                        },
                        query: {
                            uploadId: testUploadId,
                        },
                        post: completeBody,
                        calculatedMD5: calculatedMD5,
                    };
                    completeMultipartUpload(accessKey, metastore,
                        completeRequest, (err, result) => {
                            expect(err).to.equal(null);
                            parseString(result, (err) => {
                                expect(err).to.equal(null);
                                expect(metastore.buckets[bucketUID]
                                    .keyMap[objectKey].acl.READ[0])
                                    .to.equal(granteeId);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should abort/delete a multipart upload', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
            const bufferMD5 =
                new Buffer(postBody, 'base64');
            const calculatedMD5 = bufferMD5.toString('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, () => {
                const deleteRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace: namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: testUploadId,
                    },
                };
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap))
                    .to.have.length.of(2);
                multipartDelete(
                    accessKey, metastore,
                    deleteRequest, (err) => {
                        expect(err).to.be.null;
                        expect(Object.keys(metastore.buckets[mpuBucket]
                            .keyMap))
                            .to.have.length.of(0);
                        done();
                    });
            });
        });
    });

    it('should return an error if attempt to abort/delete a multipart upload ' +
        'that does not exist', (done) => {
        const bucketName = 'bucketname';
        const objectKey = 'testObject';
        const putRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const initiateRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            },
            url: `/${objectKey}?uploads`,
            namespace: namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
            }
        };
        const bucketUID = "911b9ca7dbfbe2b280a70ef0d2c2fb22";
        const mpuBucket = `mpu...${bucketUID}`;

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a part';
            const bufferMD5 =
                new Buffer(postBody, 'base64');
            const calculatedMD5 = bufferMD5.toString('hex');
            const partRequest = {
                lowerCaseHeaders: {
                    host: `${bucketName}.s3.amazonaws.com`
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                namespace: namespace,
                headers: {host: `${bucketName}.s3.amazonaws.com`},
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedMD5: calculatedMD5,
            };
            objectPutPart(accessKey, metastore, partRequest, () => {
                const deleteRequest = {
                    lowerCaseHeaders: {
                        host: `${bucketName}.s3.amazonaws.com`
                    },
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    namespace: namespace,
                    headers: {host: `${bucketName}.s3.amazonaws.com`},
                    query: {
                        uploadId: 'non-existent-upload-id',
                    },
                };
                expect(Object.keys(metastore.buckets[mpuBucket]
                    .keyMap))
                    .to.have.length.of(2);
                multipartDelete(
                    accessKey, metastore,
                    deleteRequest, (err) => {
                        expect(err).to.equal('NoSuchUpload');
                        done();
                    });
            });
        });
    });
});
