'use strict'; // eslint-disable-line strict

const assert = require('assert');
const proc = require('child_process');
const process = require('process');

const parseString = require('xml2js').parseString;

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const program = `${__dirname}/s3curl.pl`;
const upload = `test1MB`;
const aclUpload = `test500KB`;
const download = `tmpfile`;
const bucket = 's3universe';
const aclBucket = 'acluniverse';
const nonexist = 'VOID';
const prefix = 'topLevel';
const delimiter = '/';
const ownerCanonicalId = 'accessKey1';

function diff(putFile, receivedFile, done) {
    process.stdout.write(`diff ${putFile} ${receivedFile}\n`);
    proc.spawn('diff', [ putFile, receivedFile, ]).on('exit', code => {
        assert.strictEqual(code, 0);
        done();
    });
}


function createFile(name, bytes, callback) {
    process.stdout.write(`dd if=/dev/urandom of=${name} bs=${bytes} count=1\n`);
    proc.spawn('dd', [ 'if=/dev/urandom', `of=${name}`,
        `bs=${bytes}`, 'count=1'], { stdio: 'inherit' }).on('exit', code => {
            assert.strictEqual(code, 0);
            process.stdout.write(`chmod ugoa+rw ${name}\n`);
            proc.spawn(`chmod`, [`ugo+rw`, `${name}`], { stdio: 'inherit'})
                .on('exit', code => {
                    assert.strictEqual(code, 0);
                    callback();
                });
        });
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    proc.spawn('rm', [ `${file}`, ]).on('exit', () => {
        callback();
    });
}

// Test whether the proper xml error response is received
function testErrorResponse(args, done, expectedOutput) {
    const av = args;
    process.stdout.write(`${program} ${av}\n`);
    const child = proc.spawn(program, av);
    child.stdout.on('data', data => {
        const stringifiedData = data.toString();
        process.stdout.write('stdout: ' + stringifiedData);
        parseString(data.toString(), (err, result) => {
            assert.strictEqual(result.Error.Code[0], expectedOutput);
        });
    });
    child.stderr.on('data', (data) => {
        process.stdout.write('stderr: ' + data.toString());
    });
    child.on('close', () => {
        done();
    });
}


// Get content xml response and parse it
// to pass to callback
function provideXmlOutputInJSON(args, cb) {
    const av = args;
    process.stdout.write(`${program} ${av}\n`);
    const child = proc.spawn(program, av);
    child.stdout.on('data', data => {
        parseString(data.toString(), (err, result) => {
            cb(err, result);
        });
    });
}

// Get stdout stringified
function provideRawOutput(args, cb) {
    const av = args;
    process.stdout.write(`${program} ${av}\n`);
    const child = proc.spawn(program, av);
    child.stdout.on('data', data => {
        cb(data.toString());
    });
    child.stderr.on('data', (data) => {
        process.stdout.write('stderr: ' + data.toString());
    });
}

function justExec(args, cb, exitCode) {
    let exit = exitCode;
    if (exit === undefined) {
        exit = 0;
    }
    process.stdout.write(`${program} ${args}\n`);
    const child = proc.spawn(program, args, { stdio: 'inherit'});
    child.on('exit', code => {
        assert.strictEqual(code, exit);
        cb();
    });
}

describe('s3curl put and delete buckets', () => {
    it('should put a valid bucket', (done) => {
        justExec(['--createBucket', '--',
        `http://${ipAddress}:8000/${bucket}`], done);
    });

    it('should not be able to put a bucket with a name ' +
        'already being used', (done) => {
        testErrorResponse(['--createBucket', '--',
        `http://${ipAddress}:8000/${bucket}`],
        done, 'BucketAlreadyExists');
    });

    it('should not be able to put a bucket with an invalid name',
        (done) => {
            testErrorResponse(['--createBucket', '--',
            `http://${ipAddress}:8000/2`],
            done, 'InvalidBucketName');
        });

    it('should be able to delete a bucket', (done) => {
        justExec(['--delete', '--',
        `http://${ipAddress}:8000/${bucket}`], done);
    });

    it('should not be able to get a bucket that was deleted',
        (done) => {
            testErrorResponse(['--',
            `http://${ipAddress}:8000/${bucket}`],
            done, 'NoSuchBucket');
        });

    it('should be able to create a bucket with a name' +
        'of a bucket that has previously been deleted', (done) => {
        justExec(['--createBucket', '--',
        `http://${ipAddress}:8000/${bucket}`], done);
    });
});

describe('s3curl put and get bucket ACLs', () => {
    it('should be able to create a bucket with a canned ACL', (done) => {
        justExec(['--createBucket', '--', '-H',
        'x-amz-acl:public-read',
        `http://${ipAddress}:8000/${aclBucket}`], done);
    });

    it('should be able to get a canned ACL', (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${aclBucket}?acl`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            assert.strictEqual(result.AccessControlPolicy
                .Owner[0].ID[0], ownerCanonicalId);
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Grantee[0].ID[0], ownerCanonicalId);
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Permission[0], 'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[1]
                .Grantee[0].URI[0],
                'http://acs.amazonaws.com/groups/global/AllUsers');
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[1]
                .Permission[0], 'READ');
            done();
        });
    });

    it('should be able to create a bucket with a specific ACL', (done) => {
        justExec(['--createBucket', '--', '-H',
        'x-amz-grant-read:uri=http://acs.amazonaws.com/groups/global/AllUsers',
        `http://${ipAddress}:8000/${aclBucket}2`], done);
    });

    it('should be able to get a specifically set ACL', (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${aclBucket}2?acl`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            assert.strictEqual(result.AccessControlPolicy
                .Owner[0].ID[0], ownerCanonicalId);
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Grantee[0].URI[0],
                'http://acs.amazonaws.com/groups/global/AllUsers');
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Permission[0], 'READ');
            done();
        });
    });
});

describe('s3curl getService', () => {
    it('should get a list of all buckets created by user account', (done) => {
        provideXmlOutputInJSON(['--', `http://${ipAddress}:8000`],
        (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            const bucketNames = result.ListAllMyBucketsResult
                .Buckets[0].Bucket.map((item) => {
                    return item.Name[0];
                });
            const whereIsMyBucket = bucketNames.indexOf(bucket);
            assert(whereIsMyBucket > -1);
            const whereIsMyAclBucket = bucketNames.indexOf(aclBucket);
            assert(whereIsMyAclBucket > -1);
            done();
        });
    });
});

describe('s3curl putObject', () => {
    before('create file to put', (done) => {
        createFile(upload, 1048576, done);
    });

    it('should put first object in existing bucket with prefix ' +
    'and delimiter', (done) => {
        justExec(['--debug', `--put=${upload}`, `--`,
            `http://${ipAddress}:8000/${bucket}/` +
            `${prefix}${delimiter}${upload}1`],
            done);
    });

    it('should put second object in existing bucket with prefix ' +
    'and delimiter', (done) => {
        justExec([`--put=${upload}`, `--`,
            `http://${ipAddress}:8000/${bucket}/` +
            `${prefix}${delimiter}${upload}2`],
            done);
    });

    it('should put third object in existing bucket with prefix ' +
    'and delimiter', (done) => {
        justExec([`--put=${upload}`, `--`,
            `http://${ipAddress}:8000/${bucket}/` +
            `${prefix}${delimiter}${upload}3`],
            done);
    });
});

describe('s3curl getBucket', () => {
    it('should list all objects if no prefix or delimiter ' +
    'specified', (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${bucket}`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            assert.strictEqual(result.ListBucketResult
                .Contents[0].Key[0], 'topLevel/test1MB1');
            assert.strictEqual(result.ListBucketResult
                .Contents[1].Key[0], 'topLevel/test1MB2');
            assert.strictEqual(result.ListBucketResult
                .Contents[2].Key[0], 'topLevel/test1MB3');
            done();
        });
    });

    it('should list a common prefix if a common prefix and delimiter are ' +
    'specified', (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${bucket}?delimiter=${delimiter}` +
        `&prefix=${prefix}`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            assert.strictEqual(result.ListBucketResult
                .CommonPrefixes[0].Prefix[0], 'topLevel/');
            done();
        });
    });

    it('should not list a common prefix if no delimiter is ' +
    'specified', (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${bucket}?` +
        `&prefix=${prefix}`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            const keys = Object.keys(result.ListBucketResult);
            const location = keys.indexOf('CommonPrefixes');
            assert.strictEqual(location, -1);
            assert.strictEqual(result.ListBucketResult
                .Contents[0].Key[0], 'topLevel/test1MB1');
            done();
        });
    });

    it('should provide a next marker if maxs keys exceeded ' +
        'and delimiter specified', (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${bucket}?` +
        `delimiter=x&max-keys=2`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            assert.strictEqual(result.ListBucketResult
                .NextMarker[0], 'topLevel/test1MB3');
            assert.strictEqual(result.ListBucketResult
                .IsTruncated[0], 'true');
            done();
        });
    });
});

describe('s3curl head bucket', () => {
    it('should return a 404 response if bucket does not exist', (done) => {
        provideRawOutput(['--head', '--',
        `http://${ipAddress}:8000/${nonexist}`], (output) => {
            const lines = output.split('\n');
            const httpCode = lines[0].split(' ')[1];
            assert.strictEqual(httpCode, '404');
            done();
        });
    });

    it('should return a 200 response if bucket exists' +
        ' and user is authorized', (done) => {
        provideRawOutput(['--head', '--',
        `http://${ipAddress}:8000/${bucket}`], (output) => {
            const lines = output.split('\n');
            const httpCode = lines[0].split(' ')[1];
            assert.strictEqual(httpCode, '200');
            done();
        });
    });
});

describe('s3curl getObject', () => {
    after('delete created file and downloaded file', (done) => {
        deleteFile(upload, () => {
            deleteFile(download, done);
        });
    });

    it('should put object with metadata', (done) => {
        testErrorResponse(['--debug', `--put=${upload}`, `--`,
            '-H', 'x-amz-meta-mine:BestestObjectEver',
            `http://${ipAddress}:8000/${bucket}/getter`],
            done, '');
    });

    it('should get an existing file in an existing bucket', (done) => {
        justExec(['--', '-o', download,
            `http://${ipAddress}:8000/${bucket}/getter`],
            done);
    });

    it('downloaded file should equal uploaded file', (done) => {
        diff(upload, download, done);
    });
});

describe('s3curl head object', () => {
    it(`should get object's metadata`, (done) => {
        provideRawOutput(['--head', '--',
            `http://${ipAddress}:8000/${bucket}/getter`], (output) => {
            const lines = output.split('\n');
            const userMetadata = `x-amz-meta-mine: BestestObjectEver\r`;
            assert(lines.indexOf(userMetadata) > -1);
            done();
        });
    });
});

describe('s3curl object ACLs', () => {
    before('create file to put', (done) => {
        createFile(aclUpload, 512000, done);
    });
    after('delete created file', (done) => {
        deleteFile(aclUpload, done);
    });

    it('should put an object with a canned ACL', (done) => {
        justExec([`--put=${aclUpload}`, `--`,
            '-H', 'x-amz-acl:public-read',
            `http://${ipAddress}:8000/${bucket}/` +
            `${aclUpload}withcannedacl`],
            done);
    });

    it(`should get an object's canned ACL`, (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${bucket}/` +
        `${aclUpload}withcannedacl?acl`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            assert.strictEqual(result.AccessControlPolicy
                .Owner[0].ID[0], ownerCanonicalId);
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Grantee[0].ID[0], ownerCanonicalId);
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Permission[0], 'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[1]
                .Grantee[0].URI[0],
                'http://acs.amazonaws.com/groups/global/AllUsers');
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[1]
                .Permission[0], 'READ');
            done();
        });
    });

    it('should put an object with a specific ACL', (done) => {
        justExec([`--put=${aclUpload}`, `--`,
            '-H', 'x-amz-grant-read:uri=' +
            'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
            `http://${ipAddress}:8000/${bucket}/` +
            `${aclUpload}withspecificacl`],
            done);
    });

    it(`should get an object's specific ACL`, (done) => {
        provideXmlOutputInJSON(['--',
        `http://${ipAddress}:8000/${bucket}/` +
        `${aclUpload}withspecificacl?acl`], (err, result) => {
            if (err) {
                assert.ifError(err);
            }
            assert.strictEqual(result.AccessControlPolicy
                .Owner[0].ID[0], ownerCanonicalId);
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Grantee[0].URI[0],
                'http://acs.amazonaws.com/groups/global/AuthenticatedUsers');
            assert.strictEqual(result.AccessControlPolicy
                .AccessControlList[0].Grant[0]
                .Permission[0], 'READ');
            done();
        });
    });

    it('should return a NoSuchKey error if try to get an object' +
        'ACL for an object that does not exist', (done) => {
        testErrorResponse(['--',
            `http://${ipAddress}:8000/${bucket}/` +
            `keydoesnotexist?acl`],
                done, 'NoSuchKey');
    });
});
