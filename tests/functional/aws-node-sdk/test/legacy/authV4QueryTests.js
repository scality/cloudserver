const assert = require('assert');
const process = require('process');
const cp = require('child_process');
const { parseString } = require('xml2js');

const { S3 } = require('aws-sdk');
const getConfig = require('../support/config');
const provideRawOutput = require('../../lib/utility/provideRawOutput');

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

    // setup test
    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });

        s3 = new S3(config);
    });

    // emptyListing test
    it('should do an empty bucket listing', done => {
        const url = s3.getSignedUrl('listBuckets');
        provideRawOutput(['-verbose', url], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    // createBucket test
    it('should create a bucket', done => {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('createBucket', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    // fullListing test
    it('should do a bucket listing with result', done => {
        const url = s3.getSignedUrl('listBuckets');
        provideRawOutput(['-verbose', url], (httpCode, rawOutput) => {
            assert.strictEqual(httpCode, '200 OK');
            parseString(rawOutput.stdout, (err, xml) => {
                if (err) {
                    assert.ifError(err);
                }
                const bucketNames = xml.ListAllMyBucketsResult
                    .Buckets[0].Bucket.map(item => item.Name[0]);
                const whereIsMyBucket = bucketNames.indexOf(bucket);
                assert(whereIsMyBucket > -1);
                done();
            });
        });
    });

    // putObject test
    it('should put an object', done => {
        const params = { Bucket: bucket, Key: 'key' };
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
                ACL: 'public-read', StorageClass: 'STANDARD',
                ContentType: 'text/plain' };
            const url = s3.getSignedUrl('putObject', params);
            provideRawOutput(['-verbose', '-X', 'PUT', url,
                '--upload-file', 'uploadFile'], httpCode => {
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
            '--upload-file', 'uploadFile'], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    // listObjects test
    it('should list objects in bucket', done => {
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

    // getObject test
    it('should get an object', done => {
        const params = { Bucket: bucket, Key: 'key' };
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

    // deleteObject test
    it('should delete an object', done => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
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

    // deleteBucket test
    it('should delete a bucket', done => {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('deleteBucket', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });
});
