'use strict'; // eslint-disable-line strict

const proc = require('child_process');
const process = require('process');
const assert = require('assert');

const program = 's3cmd';
const upload = 'test1MB';
const emptyUpload = 'Utest0B';
const emptyDownload = 'Dtest0B';
const download = 'tmpfile';
const MPUpload = 'test16MB';
const MPDownload = 'MPtmpfile';
const bucket = 'universe';
const nonexist = 'nonexist';
const invalidName = 'VOID';
const emailAccount = 'sampleAccount1@sampling.com';
const lowerCaseEmail = emailAccount.toLowerCase();

const isIronman = process.env.CI ? ['-c', `${__dirname}/s3cfg`] : null;

function diff(putFile, receivedFile, done) {
    process.stdout.write(`diff ${putFile} ${receivedFile}\n`);
    proc.spawn('diff', [putFile, receivedFile]).on('exit', code => {
        assert.strictEqual(code, 0);
        done();
    });
}

function createFile(name, bytes, callback) {
    process.stdout.write(`dd if=/dev/urandom of=${name} bs=${bytes} count=1\n`);
    proc.spawn('dd', ['if=/dev/urandom', `of=${name}`,
        `bs=${bytes}`, 'count=1'], { stdio: 'inherit' }).on('exit', code => {
            assert.strictEqual(code, 0);
            callback();
        });
}

function createEmptyFile(name, callback) {
    process.stdout.write(`touch ${name}\n`);
    proc.spawn('touch', [name], { stdio: 'inherit' }).on('exit', code => {
        assert.strictEqual(code, 0);
        callback();
    });
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    proc.spawn('rm', [`${file}`]).on('exit', () => {
        callback();
    });
}

function exec(args, done, exitCode) {
    let exit = exitCode;
    if (exit === undefined) {
        exit = 0;
    }
    let av = args;
    if (isIronman) {
        av = args.concat(isIronman);
    }
    process.stdout.write(`${program} ${av}\n`);
    proc.spawn(program, av, { stdio: 'inherit' })
        .on('exit', code => {
            assert.strictEqual(code, exit,
                               's3cmd did not yield expected exit status.');
            done();
        });
}

// Test stdout against expected output
function checkRawOutput(args, lineFinder, testString, cb) {
    let av = args;
    if (isIronman) {
        av = args.concat(isIronman);
    }
    process.stdout.write(`${program} ${av}\n`);
    const allData = [];
    const child = proc.spawn(program, av);
    child.stdout.on('data', (data) => {
        allData.push(data.toString().trim());
        process.stdout.write(data.toString());
    });
    child.on('close', () => {
        const lineOfInterest = allData.find((item) => {
            return item.indexOf(lineFinder) > -1;
        });
        const foundIt = lineOfInterest.indexOf(testString) > -1;
        return cb(foundIt);
    });
}

// Pull line of interest from stderr (to get debug output)
function provideLineOfInterest(args, lineFinder, cb) {
    const av = isIronman ? args.concat(isIronman) : args;
    process.stdout.write(`${program} ${av}\n`);
    const allData = [];
    const child = proc.spawn(program, av);
    child.stderr.on('data', (data) => {
        allData.push(data.toString().trim());
        process.stdout.write(data.toString());
    });
    child.on('close', () => {
        const lineOfInterest = allData.find((item) => {
            return item.indexOf(lineFinder) > -1;
        });
        return cb(lineOfInterest);
    });
}

describe('s3cmd putBucket', () => {
    it('should put a valid bucket', (done) => {
        exec(['mb', `s3://${bucket}`], done);
    });

    it('put the same bucket, should fail', (done) => {
        exec(['mb', `s3://${bucket}`], done, 13);
    });

    it('put an invalid bucket, should fail', (done) => {
        exec(['mb', `s3://${invalidName}`], done, 11);
    });
});

describe(`s3cmd put and get bucket ACL's`, function aclBuck() {
    this.timeout(60000);
    // Note that s3cmd first gets the current ACL and then
    // sets the new one so by running setacl you are running a
    // get and a put
    it('should set a canned ACL', (done) => {
        exec(['setacl', `s3://${bucket}`, '--acl-public'], done);
    });

    it('should get canned ACL that was set', (done) => {
        checkRawOutput(['info', `s3://${bucket}`], 'ACL', `*anon*: READ`,
        (foundIt) => {
            assert(foundIt);
            done();
        });
    });

    it('should set a specific ACL', (done) => {
        exec(['setacl', `s3://${bucket}`,
        `--acl-grant=write:${emailAccount}`], done);
    });

    it('should get specific ACL that was set', (done) => {
        checkRawOutput(['info', `s3://${bucket}`], 'ACL',
        `${lowerCaseEmail}: WRITE`, (foundIt) => {
            assert(foundIt);
            done();
        });
    });
});

describe('s3cmd getBucket', () => {
    it('should list existing bucket', (done) => {
        exec(['ls', `s3://${bucket}`], done);
    });

    it('list non existing bucket, should fail', (done) => {
        exec(['ls', `s3://${nonexist}`], done, 12);
    });
});

describe('s3cmd getService', () => {
    it(`should get a list of a user's buckets`, (done) => {
        checkRawOutput(['ls'], 's3://',
        `${bucket}`, (foundIt) => {
            assert(foundIt);
            done();
        });
    });

    it(`should have response headers matching AWS's response headers`,
        (done) => {
            provideLineOfInterest(['ls', '--debug'], 'DEBUG: Response:',
            (lineOfInterest) => {
                const openingBracket = lineOfInterest.indexOf('{');
                const resObject = lineOfInterest.slice(openingBracket)
                    .replace(/"/g, '\\"').replace(/'/g, '"');
                const parsedObject = JSON.parse(resObject);
                assert(parsedObject.headers['x-amz-id-2']);
                assert.strictEqual(parsedObject.headers.server, 'AmazonS3');
                assert(parsedObject.headers['transfer-encoding']);
                assert(parsedObject.headers['x-amz-request-id']);
                const gmtDate = new Date(parsedObject.headers.date)
                    .toUTCString();
                assert.strictEqual(parsedObject.headers.date, gmtDate);
                assert.strictEqual(parsedObject
                    .headers['content-type'], 'application/xml');
                assert.strictEqual(parsedObject
                    .headers['set-cookie'], undefined);
                done();
            });
        });
});

describe('s3cmd putObject', () => {
    before('create file to put', (done) => {
        createFile(upload, 1048576, done);
    });

    it('should put file in existing bucket', (done) => {
        exec(['put', upload, `s3://${bucket}`], done);
    });

    it('should put file with the same name in existing bucket', (done) => {
        exec(['put', upload, `s3://${bucket}`], done);
    });

    it('put file in non existing bucket, should fail', (done) => {
        exec(['put', upload, `s3://${nonexist}`], done, 12);
    });
});

describe('s3cmd getObject', function toto() {
    this.timeout(0);
    after('delete downloaded file', done => {
        deleteFile(download, done);
    });

    it('should get existing file in existing bucket', (done) => {
        exec(['get', `s3://${bucket}/${upload}`, download], done);
    });

    it('downloaded file should equal uploaded file', (done) => {
        diff(upload, download, done);
    });

    it('get non existing file in existing bucket, should fail', (done) => {
        exec(['get', `s3://${bucket}/${nonexist}`, 'fail'], done, 12);
    });

    it('get file in non existing bucket, should fail', (done) => {
        exec(['get', `s3://${nonexist}/${nonexist}`, 'fail2'], done, 12);
    });
});

describe(`s3cmd put and get object ACL's`, function aclObj() {
    this.timeout(60000);
    // Note that s3cmd first gets the current ACL and then
    // sets the new one so by running setacl you are running a
    // get and a put
    it('should set a canned ACL', (done) => {
        exec(['setacl', `s3://${bucket}/${upload}`, '--acl-public'], done);
    });

    it('should get canned ACL that was set', (done) => {
        checkRawOutput(['info', `s3://${bucket}/${upload}`], 'ACL',
        `*anon*: READ`, (foundIt) => {
            assert(foundIt);
            done();
        });
    });

    it('should set a specific ACL', (done) => {
        exec(['setacl', `s3://${bucket}/${upload}`,
            `--acl-grant=read:${emailAccount}`], done);
    });

    it('should get specific ACL that was set', (done) => {
        checkRawOutput(['info', `s3://${bucket}/${upload}`], 'ACL',
        `${lowerCaseEmail}: READ`, (foundIt) => {
            assert(foundIt);
            done();
        });
    });

    it('should return error if set acl for ' +
        'nonexistent object', (done) => {
        exec(['setacl', `s3://${bucket}/${nonexist}`,
            '--acl-public'], done, 12);
    });
});

describe('s3cmd delObject', () => {
    it('should delete existing object', (done) => {
        exec(['rm', `s3://${bucket}/${upload}`], done);
    });

    it('delete non existing object, should fail', (done) => {
        exec(['rm', `s3://${bucket}/${nonexist}`], done, 12);
    });

    it('try to get the deleted object, should fail', (done) => {
        exec(['get', `s3://${bucket}/${upload}`, download], done, 12);
    });
});

describe('connector edge cases', function tata() {
    this.timeout(0);
    before('create file to put', (done) => {
        createEmptyFile(emptyUpload, done);
    });
    after('delete uploaded and downloaded file', (done) => {
        deleteFile(upload, () => {
            deleteFile(download, () => {
                deleteFile(emptyUpload, () => {
                    deleteFile(emptyDownload, done);
                });
            });
        });
    });

    it('should put previous file in existing bucket', (done) => {
        exec(['put', upload, `s3://${bucket}`], done);
    });

    it('should get existing file in existing bucket', (done) => {
        exec(['get', `s3://${bucket}/${upload}`, download], done);
    });

    it('should put a 0 Bytes file', (done) => {
        exec(['put', emptyUpload, `s3://${bucket}`], done);
    });

    it('should get a 0 Bytes file', (done) => {
        exec(['get', `s3://${bucket}/${emptyUpload}`, emptyDownload], done);
    });

    it('should delete a 0 Bytes file', (done) => {
        exec(['del', `s3://${bucket}/${emptyUpload}`], done);
    });
});

describe('s3cmd multipart upload', function titi() {
    this.timeout(0);
    before('create the multipart file', function createMPUFile(done) {
        this.timeout(60000);
        createFile(MPUpload, 16777216, done);
    });

    after('delete the multipart and the downloaded file', done => {
        deleteFile(MPUpload, () => {
            deleteFile(MPDownload, done);
        });
    });

    it('should put an object via a multipart upload', (done) => {
        exec(['put', MPUpload, `s3://${bucket}`], done);
    });

    it('should list multipart uploads', (done) => {
        exec(['multipart', `s3://${bucket}`], done);
    });

    it('should get an object that was put via multipart upload', (done) => {
        exec(['get', `s3://${bucket}/${MPUpload}`, MPDownload], done);
    });

    it('downloaded file should equal uploaded file', (done) => {
        diff(MPUpload, MPDownload, done);
    });

    it('should delete multipart uploaded object', (done) => {
        exec(['rm', `s3://${bucket}/${MPUpload}`], done);
    });

    it('should not be able to get deleted object', (done) => {
        exec(['get', `s3://${bucket}/${MPUpload}`, download], done, 12);
    });
});

describe('s3cmd delBucket', () => {
    it('delete non-empty bucket, should fail', (done) => {
        exec(['rb', `s3://${bucket}`], done, 13);
    });

    it('should delete remaining object', (done) => {
        exec(['rm', `s3://${bucket}/${upload}`], done);
    });

    it('should delete empty bucket', (done) => {
        exec(['rb', `s3://${bucket}`], done);
    });

    it('try to get the deleted bucket, should fail', (done) => {
        exec(['ls', `s3://${bucket}`], done, 12);
    });
});
