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
        expect(code).toBe(0);
        done();
    });
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    cp.spawn('rm', [file]).on('exit', () => {
        callback();
    });
}

describe('aws-node-sdk v4auth query tests', () => {
    this.timeout(60000);
    let s3;

    // setup test
    beforeAll(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });

        s3 = new S3(config);
    });

    // emptyListing test
    test('should do an empty bucket listing', done => {
        const url = s3.getSignedUrl('listBuckets');
        provideRawOutput(['-verbose', url], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    // createBucket test
    test('should create a bucket', done => {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('createBucket', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    // fullListing test
    test('should do a bucket listing with result', done => {
        const url = s3.getSignedUrl('listBuckets');
        provideRawOutput(['-verbose', url], (httpCode, rawOutput) => {
            expect(httpCode).toBe('200 OK');
            parseString(rawOutput.stdout, (err, xml) => {
                if (err) {
                    assert.ifError(err);
                }
                const bucketNames = xml.ListAllMyBucketsResult
                    .Buckets[0].Bucket.map(item => item.Name[0]);
                const whereIsMyBucket = bucketNames.indexOf(bucket);
                expect(whereIsMyBucket > -1).toBeTruthy();
                done();
            });
        });
    });

    // putObject test
    test('should put an object', done => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('putObject', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'uploadFile'], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test(
        'should put an object with an acl setting and a storage class setting',
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
                expect(httpCode).toBe('200 OK');
                done();
            });
        }
    );

    test('should put an object with native characters', done => {
        const Key = 'key-pâtisserie-中文-español-English-हिन्दी-العربية-' +
        'português-বাংলা-русский-日本語-ਪੰਜਾਬੀ-한국어-தமிழ்';
        const params = { Bucket: bucket, Key };
        const url = s3.getSignedUrl('putObject', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'uploadFile'], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    // listObjects test
    test('should list objects in bucket', done => {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('listObjects', params);
        provideRawOutput(['-verbose', url], (httpCode, rawOutput) => {
            expect(httpCode).toBe('200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                expect(result.ListBucketResult
                    .Contents[0].Key[0]).toBe('key');
                done();
            });
        });
    });

    // getObject test
    test('should get an object', done => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('getObject', params);
        provideRawOutput(['-verbose', '-o', 'download', url], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test('downloaded file should equal file that was put', done => {
        diff('uploadFile', 'download', () => {
            deleteFile('download', done);
        });
    });

    // deleteObject test
    test('should delete an object', done => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                expect(httpCode).toBe('204 NO CONTENT');
                done();
            });
    });

    test('should return a 204 on delete of an already deleted object', done => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                expect(httpCode).toBe('204 NO CONTENT');
                done();
            });
    });

    test('should return 204 on delete of non-existing object', done => {
        const params = { Bucket: bucket, Key: 'randomObject' };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                expect(httpCode).toBe('204 NO CONTENT');
                done();
            });
    });

    test('should delete an object with native characters', done => {
        const Key = 'key-pâtisserie-中文-español-English-हिन्दी-العربية-' +
        'português-বাংলা-русский-日本語-ਪੰਜਾਬੀ-한국어-தமிழ்';
        const params = { Bucket: bucket, Key };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url], httpCode => {
            expect(httpCode).toBe('204 NO CONTENT');
            done();
        });
    });

    // deleteBucket test
    test('should delete a bucket', done => {
        const params = { Bucket: bucket };
        const url = s3.getSignedUrl('deleteBucket', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                expect(httpCode).toBe('204 NO CONTENT');
                done();
            });
    });
});
