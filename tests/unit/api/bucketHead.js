import { expect } from 'chai';
import bucketHead from '../../../lib/api/bucketHead';
import bucketPut from '../../../lib/api/bucketPut';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('bucketHead API', () => {
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

    it('should return an error if the bucket does not exist', (done) => {
        const bucketName = 'bucketname';
        const testRequest = {
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };

        bucketHead(accessKey, metastore, testRequest, (err) => {
            expect(err).to.equal('NoSuchBucket');
            done();
        });
    });

    it('should return an error if user is not authorized', (done) => {
        const bucketName = 'bucketname';
        const putAccessKey = 'accessKey2';
        const testRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };

        bucketPut(putAccessKey, metastore, testRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketHead(accessKey, metastore, testRequest,
                    (err) => {
                        expect(err).to.equal('AccessDenied');
                        done();
                    });
            });
    });

    it('should return a success message if ' +
       'bucket exists and user is authorized', (done) => {
        const bucketName = 'bucketname';
        const testRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketHead(accessKey, metastore, testRequest,
                    (err, result) => {
                        expect(result).to.equal(
                            'Bucket exists and user authorized -- 200');
                        done();
                    });
            });
    });
});
