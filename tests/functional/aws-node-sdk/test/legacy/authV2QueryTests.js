import assert from 'assert';
import process from 'process';
import cp from 'child_process';
import { S3 } from 'aws-sdk';
import getConfig from '../support/config';
import conf from '../../../../../lib/Config';

const random = Math.round(Math.random() * 100).toString();
const bucket = `mybucket-${random}`;
const ssl = conf.https;
let transportArgs = ['-s'];
if (ssl && ssl.ca) {
    transportArgs = ['-s', '--cacert', conf.httpsPath.ca];
}
const itSkipAWS = process.env.AWS_ON_AIR
    ? it.skip
    : it;

// Get stdout and stderr stringified
function provideRawOutput(args, cb) {
    process.stdout.write(`curl ${args}\n`);
    const child = cp.spawn('curl', transportArgs.concat(args));
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
            httpCode = lines.find(line => {
                const trimmed = line.trim().toUpperCase();
                // ignore 100 Continue HTTP code
                if (trimmed.startsWith('HTTP/1.1 ') &&
                    !trimmed.includes('100 CONTINUE')) {
                    return true;
                }
                return false;
            });
            if (httpCode) {
                httpCode = httpCode.trim().replace('HTTP/1.1 ', '')
                    .toUpperCase();
            }
        }
        return cb(httpCode, procData);
    });
    child.stderr.on('data', data => {
        procData.stderr += data.toString();
    });
}


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
    itSkipAWS('should return an error code if expires header is too far ' +
        'in the future', done => {
        const params = { Bucket: bucket, Expires: 3602 };
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
        const params = { Bucket: bucket, Expires: 3601 };
        const url = s3.getSignedUrl('createBucket', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });


    it('should put an object', done => {
        const params = { Bucket: bucket, Key: 'key', Expires: 3601 };
        const url = s3.getSignedUrl('putObject', params);
        provideRawOutput(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'package.json'], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });


    it('should get an object', done => {
        const params = { Bucket: bucket, Key: 'key', Expires: 3601 };
        const url = s3.getSignedUrl('getObject', params);
        provideRawOutput(['-verbose', '-o', 'download', url], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('downloaded file should equal file that was put', done => {
        diff('package.json', 'download', () => {
            deleteFile('download', done);
        });
    });

    it('should delete an object', done => {
        const params = { Bucket: bucket, Key: 'key', Expires: 3601 };
        const url = s3.getSignedUrl('deleteObject', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });


    it('should delete a bucket', done => {
        const params = { Bucket: bucket, Expires: 3601 };
        const url = s3.getSignedUrl('deleteBucket', params);
        provideRawOutput(['-verbose', '-X', 'DELETE', url],
            httpCode => {
                assert.strictEqual(httpCode, '204 NO CONTENT');
                done();
            });
    });
});
