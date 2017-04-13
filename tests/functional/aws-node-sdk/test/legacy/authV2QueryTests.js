import assert from 'assert';
import process from 'process';
import cp from 'child_process';
import { S3 } from 'aws-sdk';
import getConfig from '../support/config';
import provideRawOutput from '../../lib/utility/provideRawOutput';

const random = Math.round(Math.random() * 100).toString();
const bucket = `mybucket-${random}`;
const almostOutsideTime = 99990;
const itSkipAWS = process.env.AWS_ON_AIR
    ? it.skip
    : it;


function diff(putFile, receivedFile, done) {
    process.stdout.write(`diff ${putFile} ${receivedFile}\n`);
    cp.spawn('diff', [putFile, receivedFile]).on('exit', code => {
        assert.strictEqual(code, 0);
        done();
    });
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    cp.spawn('rm', [file]).on('exit', () => {
        callback();
    });
}

describe('aws-node-sdk v2auth query tests', function testSuite() {
    this.timeout(60000);
    let s3;

    before(() => {
        const config = getConfig('default');

        s3 = new S3(config);
    });

    // AWS allows an expiry further in the future
    // 100010 seconds is higher that the Expires time limit: 100000 seconds
    itSkipAWS('should return an error code if expires header is too far ' +
        'in the future', done => {
        const params = { Bucket: bucket, Expires: 100010 };
        const url = s3.getSignedUrl('createBucket', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url], httpCode => {
            assert.strictEqual(httpCode, '403 FORBIDDEN');
            done();
        });
    });

    it('should return an error code if request occurs after expiry',
        done => {
            const params = { Bucket: bucket, Expires: 1 };
            const url = s3.getSignedUrl('createBucket', params);
            setTimeout(() => {
                provideRawOutput(['-verbose', '-X', 'PUT', url], httpCode => {
                    assert.strictEqual(httpCode, '403 FORBIDDEN');
                    done();
                });
            }, 1500);
        });

    it('should create a bucket', done => {
        const params = { Bucket: bucket, Expires: almostOutsideTime };
        const url = s3.getSignedUrl('createBucket', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });


    it('should put an object', done => {
        const params = { Bucket: bucket, Key: 'key', Expires:
        almostOutsideTime };
        const url = s3.getSignedUrl('putObject', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'uploadFile'], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should put an object with an acl setting and a storage class setting',
         done => {
             // This will test that upper case query parameters and lowercase
             // query parameters (i.e., 'x-amz-acl') are being sorted properly.
             // This will also test that query params that contain "x-amz-"
             // are being added to the canonical headers list in our string
             // to sign.
             const params = { Bucket: bucket, Key: 'key',
             ACL: 'public-read', StorageClass: 'STANDARD' };
             const url = s3.getSignedUrl('putObject', params);
             provideRawOutput(['-verbose', '-X', 'PUT', url,
                 '--upload-file', 'uploadFile'], httpCode => {
                 assert.strictEqual(httpCode, '200 OK');
                 done();
             });
         });


    it('should get an object', done => {
        const params = { Bucket: bucket, Key: 'key', Expires:
        almostOutsideTime };
        const url = s3.getSignedUrl('getObject', params);
        provideRawOutput(['-verbose', '-o', 'download', url], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('downloaded file should equal file that was put', done => {
        diff('uploadFile', 'download', () => {
            deleteFile('download', done);
        });
    });

    it('should delete an object', done => {
        const params = { Bucket: bucket, Key: 'key', Expires:
        almostOutsideTime };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });


    it('should delete a bucket', done => {
        const params = { Bucket: bucket, Expires: almostOutsideTime };
        const url = s3.getSignedUrl('deleteBucket', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });
});
