import { expect } from 'chai';
import crypto from 'crypto';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import objectGet from '../../../lib/api/objectGet';
import { Writable } from 'stream';

const memStore = {};

// Create in memory writable stream
class WMStrm extends Writable {
    constructor(key) {
        super();
        this.key = key;
        memStore[key] = new Buffer('');
    }
    _write(chunk, enc, cb) {
        // our memory store stores things in buffers
        const buffer = (Buffer.isBuffer(chunk)) ?
        chunk :  // already is Buffer use it
        new Buffer(chunk, enc);  // string, convert

      // concat to the buffer already there
        memStore[this.key] = Buffer.concat([memStore[this.key], buffer]);
        cb();
    }
}

const accessKey = 'accessKey1';
const namespace = 'default';

describe('objectGet API', () => {
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

    const bucketName = 'bucketname';
    const postBody = 'I am a body';
    const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
    const objectName = 'objectName';
    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testPutObjectRequest = {
        lowerCaseHeaders: {
            'x-amz-meta-test': 'some metadata'
        },
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
    };

    it("should get the object metadata", (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectGet(accessKey, datastore,
                            metastore, testGetRequest,
                            (err, result, responseMetaHeaders) => {
                                expect(responseMetaHeaders[userMetadataKey])
                                    .to.equal(userMetadataValue);
                                expect(responseMetaHeaders.Etag)
                                    .to.equal(correctMD5);
                                done();
                            });
                    });
            });
    });

    it('should get the object data', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectGet(accessKey, datastore, metastore,
                            testGetRequest, (err, readStream) => {
                                // Create new in memory writestream
                                const wstream = new WMStrm('smallObject');
                                readStream.pipe(wstream, {end: false});
                                readStream.on('end', function readStreamRes() {
                                    wstream.end();
                                    expect(memStore.smallObject
                                        .toString()).to.equal(postBody);
                                    done();
                                });
                            });
                    });
            });
    });

    it('should get the object data for large objects', (done) => {
        const testBigData = crypto.randomBytes(1000000);
        const correctBigMD5 =
            crypto.createHash('md5').update(testBigData).digest('base64');

        const testPutBigObjectRequest = {
            lowerCaseHeaders: {
                'x-amz-meta-test': 'some metadata'
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: testBigData,
            calculatedMD5: correctBigMD5
        };

        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutBigObjectRequest, (err, result) => {
                        expect(result).to.equal(correctBigMD5);
                        objectGet(accessKey, datastore,
                            metastore, testGetRequest, (err, readable) => {
                                const md5Hash = crypto.createHash('md5');
                                const chunks = [];
                                readable.on('data', function chunkRcvd(chunk) {
                                    const cBuffer = new Buffer(chunk, "binary");
                                    chunks.push(cBuffer);
                                    md5Hash.update(cBuffer);
                                });
                                readable.on('end', function combineChunks() {
                                    const resultmd5Hash = md5Hash.digest('hex');
                                    expect(resultmd5Hash)
                                        .to.equal(correctBigMD5);
                                    done();
                                });
                            });
                    });
            });
    });
});
