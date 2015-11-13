import { expect } from 'chai';
import bucketPut from '../../../lib/api/bucketPut';
import initiateMultipartUpload from '../../../lib/api/initiateMultipartUpload';
import objectPutPart from '../../../lib/api/objectPutPart';
import completeMultipartUpload from '../../../lib/api/completeMultipartUpload';
import { parseString } from 'xml2js';
import async from 'async';

const accessKey = 'accessKey1';
const namespace = 'default';


describe('Multipart Upload API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
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

        bucketPut(accessKey, metastore, putRequest, () => {
            initiateMultipartUpload(accessKey, metastore, initiateRequest,
                    (err, result) => {
                        expect(err).to.be.undefined;
                        expect(Object.keys(metastore.buckets[bucketUID]
                            .multiPartObjectKeyMap)).to.have.length.of(1);
                        parseString(result, (err, json) => {
                            expect(json.InitiateMultipartUploadResult
                                .Bucket[0]).to.equal(bucketName);
                            expect(json.InitiateMultipartUploadResult
                                .Key[0]).to.equal(objectKey);
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

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[bucketUID]
                    .multiPartObjectKeyMap)).to.have.length.of(1);
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
            objectPutPart(accessKey, datastore,
                metastore, partRequest, (err, result) => {
                    expect(err).to.be.null;
                    const dataLocation = Object.keys(datastore)[0];
                    expect(metastore.buckets[bucketUID]
                        .multiPartObjectKeyMap[testUploadId].partLocations[1]
                        .location).to.equal(dataLocation);
                    expect(metastore.buckets[bucketUID]
                        .multiPartObjectKeyMap[testUploadId]
                        .partLocations[1].etag).to.equal(calculatedMD5);
                    expect(datastore[dataLocation]).to.equal(postBody);
                    expect(result).to.equal(calculatedMD5);
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

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[bucketUID]
                    .multiPartObjectKeyMap)).to.have.length.of(1);
                parseString(result, next);
            },
        ],
        function waterfallFinal(err, json) {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const postBody = 'I am a first part';
            const bufferMD5 =
                new Buffer(postBody, 'base64');
            const calculatedMD5 = bufferMD5.toString('hex');
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
            objectPutPart(accessKey, datastore,
                metastore, partRequest1, () => {
                    const postBody2 = 'I am a second part';
                    const secondBufferMD5 =
                        new Buffer(postBody, 'base64');
                    const secondCalculatedMD5 = secondBufferMD5.toString('hex');
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
                    objectPutPart(accessKey, datastore, metastore,
                        partRequest2, (err, result) => {
                            expect(err).to.be.null;
                            const dataLocation = Object.keys(datastore)[1];
                            expect(metastore.buckets[bucketUID]
                                .multiPartObjectKeyMap[testUploadId]
                                .partLocations[2]
                                .location).to.equal(dataLocation);
                            expect(metastore.buckets[bucketUID]
                                .multiPartObjectKeyMap[testUploadId]
                                .partLocations[2].etag).to.equal(calculatedMD5);
                            expect(datastore[dataLocation]).to.equal(postBody2);
                            expect(result).to.equal(calculatedMD5);
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

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, putRequest, next);
            },
            function waterfall2(success, next) {
                initiateMultipartUpload(
                    accessKey, metastore, initiateRequest, next);
            },
            function waterfall3(result, next) {
                expect(Object.keys(metastore.buckets[bucketUID]
                    .multiPartObjectKeyMap)).to.have.length.of(1);
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
            objectPutPart(accessKey, datastore,
                metastore, partRequest, () => {
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
});
