'use strict'; // eslint-disable-line strict

const assert = require('assert');
const parseString = require('xml2js').parseString;
const proc = require('child_process');
const process = require('process');

const awssdk = require('aws-sdk');
const config = awssdk.config;
const S3 = awssdk.S3;

const bucket = 'mybucket';


// Get stdout and stderr stringified
function provideRawOutput(args, cb) {
    process.stdout.write(`curl ${args}\n`);
    const child = proc.spawn('curl', args);
    const procData = {
        stdout: '',
        stderr: '',
    };
    child.stdout.on('data', data => {
        procData.stdout += data.toString();
    });
    child.on('close', () => {
        let httpCode;
        if (procData.stderr !== '') {
            const lines = procData.stderr.replace(/[<>]/g, '').split(/[\r\n]/);
            httpCode = lines.find((line) => {
                const trimmed = line.trim().toUpperCase();
                // ignore 100 Continue HTTP code
                if (trimmed.startsWith('HTTP/1.1 ') &&
                    !trimmed.includes('100 CONTINUE')) {
                    return true;
                }
            });
            if (httpCode) {
                httpCode = httpCode.trim().replace('HTTP/1.1 ', '')
                    .toUpperCase();
            }
        }
        return cb(httpCode, procData);
    });
    child.stderr.on('data', (data) => {
        procData.stderr += data.toString();
    });
}


function diff(putFile, receivedFile, done) {
    process.stdout.write(`diff ${putFile} ${receivedFile}\n`);
    proc.spawn('diff', [putFile, receivedFile]).on('exit', code => {
        assert.strictEqual(code, 0);
        done();
    });
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    proc.spawn('rm', [file]).on('exit', () => {
        callback();
    });
}

describe('aws-node-sdk v4auth query tests', function testSuite() {
    this.timeout(60000);
    let s3;

    before(function setup(done) {
        config.accessKeyId = 'accessKey1';
        config.secretAccessKey = 'verySecretKey1';
        if (process.env.IP !== undefined) {
            config.endpoint = `http://${process.env.IP}:8000`;
        } else {
            config.endpoint = 'http://localhost:8000';
        }
        config.sslEnabled = false;
        config.s3ForcePathStyle = true;
        config.apiVersions = { s3: '2006-03-01' };
        config.logger = process.stdout;
        config.signatureVersion = 'v4';
        s3 = new S3();
        done();
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
