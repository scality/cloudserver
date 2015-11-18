import { expect } from 'chai';
import objectPut from '../../../lib/api/objectPut';
import bucketPut from '../../../lib/api/bucketPut';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('objectPut API', () => {
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


    it('should return an error if the bucket does not exist', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const testRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
            post: postBody
        };

        objectPut(accessKey, datastore, metastore, testRequest,
            (err) => {
                expect(err).to.equal('NoSuchBucket');
                done();
            });
    });

    it('should return an error if user is not authorized', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const putAccessKey = 'accessKey2';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
            post: postBody
        };

        bucketPut(putAccessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                    (err) => {
                        expect(err).to.equal('AccessDenied');
                        done();
                    });
            });
    });

    it.skip('should return an error if datastore ' +
            'reports an error back', () => {
        // TODO: Test to be written once services.putDataStore
        // includes an actual call to
        // datastore rather than just the in
        // memory adding of a key/value pair to the datastore
        // object
    });

    it.skip('should return an error if metastore ' +
            'reports an error back', () => {
        // TODO: Test to be written once
        // services.metadataStoreObject includes an actual call to
        // datastore rather than just the in
        // memory adding of a key/value pair to the datastore
        // object
    });

    it('should successfully put an object with bucket' +
    ' and object in pathname', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const objectUID = '31b0c936d4b4c712e2ea1a927b387fd3';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        expect(
                            metastore.buckets[bucketUID].keyMap[objectName])
                            .to.exist;
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['content-md5'])
                                .to.equal(correctMD5);
                        expect(datastore[objectUID]).to.equal('I am a body');
                        done();
                    });
            });
    });

    it('should successfully put an object with object ' +
    'in pathname and bucket in hostname', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const objectUID = '31b0c936d4b4c712e2ea1a927b387fd3';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: `/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        expect(
                            metastore.buckets[bucketUID].keyMap[objectName])
                            .to.exist;
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['content-md5'])
                                .to.equal(correctMD5);
                        expect(datastore[objectUID]).to.equal('I am a body');
                        done();
                    });
            });
    });

    it('should successfully put an object with user metadata', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {
                // Note that Node will collapse common headers into one
                // (e.g. "x-amz-meta-test: hi" and "x-amz-meta-test:
                // there" becomes "x-amz-meta-test: hi, there")
                // Here we are not going through an actual http
                // request so will not collapse properly.
                'x-amz-meta-test': 'some metadata',
                'x-amz-meta-test2': 'some more metadata',
                'x-amz-meta-test3': 'even more metadata',
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]).to.exist;
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['x-amz-meta-test'])
                                    .to.equal('some metadata');
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['x-amz-meta-test2'])
                                    .to.equal('some more metadata');
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['x-amz-meta-test3'])
                                    .to.equal('even more metadata');
                        done();
                    });
            });
    });
});
