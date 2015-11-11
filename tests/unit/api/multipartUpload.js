import { expect } from 'chai';
import bucketPut from '../../../lib/api/bucketPut';
import initiateMultipartUpload from '../../../lib/api/initiateMultipartUpload';
import objectPutPart from '../../../lib/api/objectPutPart';
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
});
