import assert from 'assert';
import process from 'process';
import cp from 'child_process';
import { parseString } from 'xml2js';
import { S3 } from 'aws-sdk';
import getConfig from '../support/config';
import provideRawOutput from '../../lib/utility/provideRawOutput';

const random = Math.round(Math.random() * 100).toString();
const bucket = `mybucket-${random}`;


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

describe('aws-node-sdk v4auth query tests', function testSuite() {
    this.timeout(60000);
    let s3;

    before(function setup() {
        const config = getConfig('default', { signatureVersion: 'v4' });

        s3 = new S3(config);
    });

    it('should do an empty bucket listing', function emptyListing(done) {
        const url = s3.getSignedUrl('listBuckets');
        provideRawOutput(['-verbose', url], (httpCode) => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should create a bucket', function createBucket(done) {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('createBucket', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url], (httpCode) => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should do a bucket listing with result', function fullListing(done) {
        const url = s3.getSignedUrl('listBuckets');
        provideRawOutput(['-verbose', url], (httpCode, rawOutput) => {
            assert.strictEqual(httpCode, '200 OK');
            parseString(rawOutput.stdout, (err, xml) => {
                if (err) {
                    assert.ifError(err);
                }
                const bucketNames = xml.ListAllMyBucketsResult
                    .Buckets[0].Bucket.map((item) => {
                        return item.Name[0];
                    });
                const whereIsMyBucket = bucketNames.indexOf(bucket);
                assert(whereIsMyBucket > -1);
                done();
            });
        });
    });

    it('should put an object', function putObject(done) {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('putObject', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'package.json'], (httpCode) => {
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
            ACL: 'public-read', StorageClass: 'STANDARD',
            ContentType: 'text/plain' };
            const url = s3.getSignedUrl('putObject', params);
            provideRawOutput(['-verbose', '-X', 'PUT', url,
                '--upload-file', 'package.json'], httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                done();
            });
        });

    it('should put an object with native characters', done => {
        const Key = 'key-pâtisserie-中文-español-English-हिन्दी-العربية-' +
        'português-বাংলা-русский-日本語-ਪੰਜਾਬੀ-한국어-தமிழ்';
        const params = { Bucket: bucket, Key };
        const url = s3.getSignedUrl('putObject', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'package.json'], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should list objects in bucket', function listObjects(done) {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('listObjects', params);
        provideRawOutput(['-verbose', url], (httpCode, rawOutput) => {
            assert.strictEqual(httpCode, '200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                assert.strictEqual(result.ListBucketResult
                    .Contents[0].Key[0], 'key');
                done();
            });
        });
    });

    it('should get an object', function getObject(done) {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('getObject', params);
        provideRawOutput(['-verbose', '-o', 'download', url], (httpCode) => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('downloaded file should equal file that was put', (done) => {
        diff('package.json', 'download', () => {
            deleteFile('download', done);
        });
    });

    it('should delete an object', function deleteObject(done) {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            (httpCode) => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });

    it('should return a 204 on delete of an already deleted object', done => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });

    it('should return 204 on delete of non-existing object', done => {
        const params = { Bucket: bucket, Key: 'randomObject' };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });

    it('should delete an object with native characters', done => {
        const Key = 'key-pâtisserie-中文-español-English-हिन्दी-العربية-' +
        'português-বাংলা-русский-日本語-ਪੰਜਾਬੀ-한국어-தமிழ்';
        const params = { Bucket: bucket, Key };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url], httpCode => {
            assert.strictEqual(httpCode, '204 NO CONTENT');
            done();
        });
    });

    it('should delete a bucket', function deleteBucket(done) {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('deleteBucket', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            (httpCode) => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });
});
