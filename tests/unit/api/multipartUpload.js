import { expect } from 'chai';
import bucketPut from '../../../lib/api/bucketPut';
import initiateMultipartUpload from '../../../lib/api/initiateMultipartUpload';
import { parseString } from 'xml2js';

const accessKey = 'accessKey1';
const namespace = 'default';


describe('initiateMultipartUpload API', () => {
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
});
