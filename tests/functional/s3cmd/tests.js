'use strict'; // eslint-disable-line strict

const proc = require('child_process');
const process = require('process');
const assert = require('assert');
const fs = require('fs');
const async = require('async');
const conf = require('../../../lib/Config').config;

const configCfg = conf.https ? 's3cfg_ssl' : 's3cfg';
const program = 's3cmd';
const upload = 'test1MB';
const emptyUpload = 'Utest0B';
const emptyDownload = 'Dtest0B';
const download = 'tmpfile';
const MPUpload = 'test60MB';
const MPUploadSplitter = [
    'test60..|..MB',
    '..|..test60MB',
    'test60MB..|..',
];
const MPDownload = 'MPtmpfile';
const MPDownloadCopy = 'MPtmpfile2';
const downloadCopy = 'tmpfile2';
const bucket = 'universe';
const nonexist = 'nonexist';
const invalidName = 'VOID';
const emailAccount = 'sampleAccount1@sampling.com';
const lowerCaseEmail = emailAccount.toLowerCase();
const describeSkipIfE2E = process.env.S3_END_TO_END ? describe.skip : describe;

function safeJSONParse(s) {
    let res;
    try {
        res = JSON.parse(s);
    } catch (e) {
        return e;
    }
    return res;
}

const isScality = process.env.CI ? ['-c', `${__dirname}/${configCfg}`] : null;

function diff(putFile, receivedFile, done) {
    process.stdout.write(`diff ${putFile} ${receivedFile}\n`);
    proc.spawn('diff', [putFile, receivedFile]).on('exit', code => {
        expect(code).toBe(0);
        done();
    });
}

function createFile(name, bytes, callback) {
    process.stdout.write(`dd if=/dev/urandom of=${name} bs=${bytes} count=1\n`);
    const ret = proc.spawnSync('dd', ['if=/dev/urandom', `of=${name}`,
        `bs=${bytes}`, 'count=1'], { stdio: 'inherit' });
    expect(ret.status).toBe(0);
    callback();
}

function createEmptyFile(name, callback) {
    process.stdout.write(`touch ${name}\n`);
    const ret = proc.spawnSync('touch', [name], { stdio: 'inherit' });
    expect(ret.status).toBe(0);
    callback();
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    proc.spawnSync('rm', [`${file}`]);
    callback();
}

function exec(args, done, exitCode) {
    let exit = exitCode;
    if (exit === undefined) {
        exit = 0;
    }
    let av = ['-c', configCfg].concat(args);
    if (isScality) {
        av = av.concat(isScality);
    }
    process.stdout.write(`${program} ${av}\n`);
    const ret = proc.spawnSync(program, av, { stdio: 'inherit' });
    expect(ret.status).toBe(exit);
    done();
}

// Test stdout or stderr against expected output
function checkRawOutput(args, lineFinder, testString, stream, cb) {
    let av = ['-c', configCfg].concat(args);
    if (isScality) {
        av = av.concat(isScality);
    }
    process.stdout.write(`${program} ${av}\n`);
    const allData = [];
    const allErrData = [];
    const child = proc.spawn(program, av);
    child.stdout.on('data', data => {
        allData.push(data.toString());
        process.stdout.write(data.toString());
    });
    child.stderr.on('data', data => {
        allErrData.push(data.toString());
        process.stdout.write(data.toString());
    });
    child.on('close', () => {
        if (stream === 'stderr') {
            const foundIt = allErrData.join('').split('\n')
                .filter(item => item.indexOf(lineFinder) > -1)
                .some(item => item.indexOf(testString) > -1);
            return cb(foundIt);
        }
        const foundIt = allData.join('').split('\n')
            .filter(item => item.indexOf(lineFinder) > -1)
            .some(item => item.indexOf(testString) > -1);
        return cb(foundIt);
    });
}

function findEndString(data, start) {
    const delimiter = data[start];
    const end = data.length;
    for (let i = start + 1; i < end; ++i) {
        if (data[i] === delimiter) {
            return (i);
        } else if (data[i] === '\\') {
            ++i;
        }
    }
    return (-1);
}

function findEndJson(data, start) {
    let count = 0;
    const end = data.length;
    for (let i = start; i < end; ++i) {
        if (data[i] === '{') {
            ++count;
        } else if (data[i] === '}') {
            --count;
        } else if (data[i] === '"' || data[i] === "'") {
            i = findEndString(data, i);
        }
        if (count === 0) {
            return (i);
        }
    }
    return (-1);
}

function readJsonFromChild(child, lineFinder, cb) {
    const allData = [];
    child.stderr.on('data', data => {
        allData.push(data.toString());
        process.stdout.write(data.toString());
    });
    child.on('close', () => {
        const data = allData.join('');
        const findLine = data.indexOf(lineFinder);
        const findBrace = data.indexOf('{', findLine);
        const findEnd = findEndJson(data, findBrace);
        const endJson = data.substring(findBrace, findEnd + 1)
            .replace(/"/g, '\\"').replace(/'/g, '"');
        return cb(JSON.parse(endJson));
    });
}

// Pull line of interest from stderr (to get debug output)
function provideLineOfInterest(args, lineFinder, cb) {
    const argsWithCfg = ['-c', configCfg].concat(args);
    const av = isScality ? argsWithCfg.concat(isScality) : argsWithCfg;
    process.stdout.write(`${program} ${av}\n`);
    const child = proc.spawn(program, av);
    readJsonFromChild(child, lineFinder, cb);
}

function retrieveInfo() {
    const configFile = isScality ? `${__dirname}/${configCfg}` : configCfg;
    const data = fs.readFileSync(configFile, 'utf8').split('\n');
    const res = {
        accessKey: null,
        secretKey: null,
        host: null,
        port: 0,
    };
    data.forEach(item => {
        const keyValue = item.split('=');
        if (keyValue.length === 2) {
            const key = keyValue[0].trim();
            const value = keyValue[1].trim();
            if (key === 'access_key') {
                res.accessKey = value;
            } else if (key === 'secret_key') {
                res.secretKey = value;
            } else if (key === 'host_base') {
                const hostInfo = value.split(':');
                if (hostInfo.length > 1) {
                    res.host = hostInfo[0];
                    res.port = parseInt(hostInfo[1], 10);
                } else {
                    res.host = hostInfo[0];
                    res.port = 80;
                }
            }
        }
    });
    return res;
}

function createEncryptedBucket(name, cb) {
    const res = retrieveInfo();
    const prog = `${__dirname}/../../../bin/create_encrypted_bucket.js`;
    let args = [
        prog,
        '-a', res.accessKey,
        '-k', res.secretKey,
        '-b', name,
        '-h', res.host,
        '-p', res.port,
        '-v',
    ];
    if (conf.https) {
        args = args.concat('-s');
    }
    const body = [];
    const child = proc.spawn(args[0], args)
    .on('exit', () => {
        const hasSucceed = body.join('').split('\n').find(item => {
            const json = safeJSONParse(item);
            const test = !(json instanceof Error) && json.name === 'S3' &&
                json.statusCode === 200;
            if (test) {
                return true;
            }
            return false;
        });
        if (!hasSucceed) {
            process.stderr.write(`${body.join('')}\n`);
            return cb(new Error('Cannot create encrypted bucket'));
        }
        return cb();
    })
    .on('error', cb);
    child.stdout.on('data', chunk => body.push(chunk.toString()));
}

describe('s3cmd putBucket', () => {
    test('should put a valid bucket', done => {
        exec(['mb', `s3://${bucket}`], done);
    });

    // scality-us-west-1 is NOT using legacyAWSBehvior
    // in test location config and in end to end so this test should
    // pass by returning error. If legacyAWSBehvior, request
    // would return a 200
    test('put the same bucket, should fail', done => {
        exec([
            'mb', `s3://${bucket}`,
            '--bucket-location=scality-us-west-1',
        ], done, 13);
    });

    test('put an invalid bucket, should fail', done => {
        exec(['mb', `s3://${invalidName}`], done, 11);
    });

    test('should put a valid bucket with region', done => {
        exec(['mb', 's3://regioned', '--region=us-east-1'], done);
    });

    test('should delete bucket put with region', done => {
        exec(['rb', 's3://regioned', '--region=us-east-1'], done);
    });

    if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
        test('creates a valid bucket with server side encryption', done => {
            this.timeout(5000);
            exec(['rb', `s3://${bucket}`], err => {
                if (err) {
                    return done(err);
                }
                return createEncryptedBucket(bucket, done);
            });
        });
    }
});

describe('s3cmd put and get bucket ACLs', () => {
    this.timeout(60000);
    // Note that s3cmd first gets the current ACL and then
    // sets the new one so by running setacl you are running a
    // get and a put
    test('should set a canned ACL', done => {
        exec(['setacl', `s3://${bucket}`, '--acl-public'], done);
    });

    test('should get canned ACL that was set', done => {
        checkRawOutput(['info', `s3://${bucket}`], 'ACL', '*anon*: READ',
        'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });

    test('should set a specific ACL', done => {
        exec([
            'setacl', `s3://${bucket}`,
            `--acl-grant=write:${emailAccount}`,
        ], done);
    });

    test('should get specific ACL that was set', done => {
        checkRawOutput(['info', `s3://${bucket}`], 'ACL',
        `${lowerCaseEmail}: WRITE`, 'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });
});

describe('s3cmd getBucket', () => {
    test('should list existing bucket', done => {
        exec(['ls', `s3://${bucket}`], done);
    });

    test('list non existing bucket, should fail', done => {
        exec(['ls', `s3://${nonexist}`], done, 12);
    });
});

describe('s3cmd getService', () => {
    test("should get a list of a user's buckets", done => {
        checkRawOutput(['ls'], 's3://', `${bucket}`, 'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });

    test("should have response headers matching AWS's response headers", done => {
        provideLineOfInterest(['ls', '--debug'], 'DEBUG: Response: {',
        parsedObject => {
            expect(parsedObject.headers['x-amz-id-2']).toBeTruthy();
            expect(parsedObject.headers['transfer-encoding']).toBeTruthy();
            expect(parsedObject.headers['x-amz-request-id']).toBeTruthy();
            const gmtDate = new Date(parsedObject.headers.date)
                .toUTCString();
            expect(parsedObject.headers.date).toBe(gmtDate);
            expect(parsedObject
                .headers['content-type']).toBe('application/xml');
            expect(parsedObject
                .headers['set-cookie']).toBe(undefined);
            done();
        });
    });
});

describe('s3cmd putObject', () => {
    this.timeout(15000);
    beforeAll(done => {
        createFile(upload, 1048576, done);
    });

    test('should put file in existing bucket', done => {
        exec(['put', upload, `s3://${bucket}`], done);
    });

    test('should put file with the same name in existing bucket', done => {
        exec(['put', upload, `s3://${bucket}`], done);
    });

    test('put file in non existing bucket, should fail', done => {
        exec(['put', upload, `s3://${nonexist}`], done, 12);
    });
});

describe('s3cmd getObject', () => {
    this.timeout(0);
    afterAll(done => {
        deleteFile(download, done);
    });

    test('should get existing file in existing bucket', done => {
        exec(['get', `s3://${bucket}/${upload}`, download], done);
    });

    test('downloaded file should equal uploaded file', done => {
        diff(upload, download, done);
    });

    test('get non existing file in existing bucket, should fail', done => {
        exec(['get', `s3://${bucket}/${nonexist}`, 'fail'], done, 12);
    });

    test('get file in non existing bucket, should fail', done => {
        exec(['get', `s3://${nonexist}/${nonexist}`, 'fail2'], done, 12);
    });
});

describe('s3cmd copyObject without MPU to same bucket', () => {
    this.timeout(40000);

    afterAll(done => {
        deleteFile(downloadCopy, done);
    });

    test('should copy an object to the same bucket', done => {
        exec([
            'cp', `s3://${bucket}/${upload}`,
            `s3://${bucket}/${upload}copy`,
        ], done);
    });

    test('should get an object that was copied', done => {
        exec(['get', `s3://${bucket}/${upload}copy`, downloadCopy], done);
    });

    test('downloaded copy file should equal original uploaded file', done => {
        diff(upload, downloadCopy, done);
    });

    test('should delete copy of object', done => {
        exec(['rm', `s3://${bucket}/${upload}copy`], done);
    });
});

describe('s3cmd copyObject without MPU to different bucket ' +
    '(always unencrypted)',
    () => {
        const copyBucket = 'receiverbucket';
        this.timeout(40000);

        beforeAll(done => {
            exec(['mb', `s3://${copyBucket}`], done);
        });

        afterAll('delete downloaded file and receiver bucket' +
            'copied', done => {
            deleteFile(downloadCopy, () => {
                exec(['rb', `s3://${copyBucket}`], done);
            });
        });

        test('should copy an object to the new bucket', done => {
            exec([
                'cp', `s3://${bucket}/${upload}`,
                `s3://${copyBucket}/${upload}`,
            ], done);
        });

        test('should get an object that was copied', done => {
            exec(['get', `s3://${copyBucket}/${upload}`, downloadCopy], done);
        });

        test('downloaded copy file should equal original uploaded file', done => {
            diff(upload, downloadCopy, done);
        });

        test('should delete copy of object', done => {
            exec(['rm', `s3://${copyBucket}/${upload}`], done);
        });
    });

describe('s3cmd put and get object ACLs', () => {
    this.timeout(60000);
    // Note that s3cmd first gets the current ACL and then
    // sets the new one so by running setacl you are running a
    // get and a put
    test('should set a canned ACL', done => {
        exec(['setacl', `s3://${bucket}/${upload}`, '--acl-public'], done);
    });

    test('should get canned ACL that was set', done => {
        checkRawOutput(['info', `s3://${bucket}/${upload}`], 'ACL',
        '*anon*: READ', 'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });

    test('should set a specific ACL', done => {
        exec(['setacl', `s3://${bucket}/${upload}`,
            `--acl-grant=read:${emailAccount}`], done);
    });

    test('should get specific ACL that was set', done => {
        checkRawOutput(['info', `s3://${bucket}/${upload}`], 'ACL',
        `${lowerCaseEmail}: READ`, 'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });

    test('should return error if set acl for ' +
        'nonexistent object', done => {
        exec(['setacl', `s3://${bucket}/${nonexist}`,
            '--acl-public'], done, 12);
    });
});

describe('s3cmd delObject', () => {
    test('should delete existing object', done => {
        exec(['rm', `s3://${bucket}/${upload}`], done);
    });

    test('delete an already deleted object, should return a 204', done => {
        provideLineOfInterest(['rm', `s3://${bucket}/${upload}`, '--debug'],
        'DEBUG: Response: {', parsedObject => {
            expect(parsedObject.status).toBe(204);
            done();
        });
    });

    test('delete non-existing object, should return a 204', done => {
        provideLineOfInterest(['rm', `s3://${bucket}/${nonexist}`, '--debug'],
        'DEBUG: Response: {', parsedObject => {
            expect(parsedObject.status).toBe(204);
            done();
        });
    });

    test('try to get the deleted object, should fail', done => {
        exec(['get', `s3://${bucket}/${upload}`, download], done, 12);
    });
});

describe('connector edge cases', () => {
    this.timeout(0);
    beforeAll(done => {
        createEmptyFile(emptyUpload, done);
    });
    afterAll(done => {
        deleteFile(upload, () => {
            deleteFile(download, () => {
                deleteFile(emptyUpload, () => {
                    deleteFile(emptyDownload, done);
                });
            });
        });
    });

    test('should put previous file in existing bucket', done => {
        exec(['put', upload, `s3://${bucket}`], done);
    });

    test('should get existing file in existing bucket', done => {
        exec(['get', `s3://${bucket}/${upload}`, download], done);
    });

    test('should put a 0 Bytes file', done => {
        exec(['put', emptyUpload, `s3://${bucket}`], done);
    });

    test('should get a 0 Bytes file', done => {
        exec(['get', `s3://${bucket}/${emptyUpload}`, emptyDownload], done);
    });

    test('should delete a 0 Bytes file', done => {
        exec(['del', `s3://${bucket}/${emptyUpload}`], done);
    });
});

describe('s3cmd multipart upload', () => {
    this.timeout(0);
    beforeAll(done => {
        this.timeout(60000);
        createFile(MPUpload, 62914560, done);
    });

    afterAll(done => {
        deleteFile(MPUpload, () => {
            deleteFile(MPDownload, () => {
                deleteFile(MPDownloadCopy, done);
            });
        });
    });

    test('should put an object via a multipart upload', done => {
        exec(['put', MPUpload, `s3://${bucket}`], done);
    });

    test('should list multipart uploads', done => {
        exec(['multipart', `s3://${bucket}`], done);
    });

    test('should get an object that was put via multipart upload', done => {
        exec(['get', `s3://${bucket}/${MPUpload}`, MPDownload], done);
    });

    test('downloaded file should equal uploaded file', done => {
        diff(MPUpload, MPDownload, done);
    });

    test('should copy an object that was put via multipart upload', done => {
        exec([
            'cp', `s3://${bucket}/${MPUpload}`,
            `s3://${bucket}/${MPUpload}copy`,
        ], done);
    });

    test('should get an object that was copied', done => {
        exec(['get', `s3://${bucket}/${MPUpload}copy`, MPDownloadCopy], done);
    });

    test('downloaded copy file should equal original uploaded file', done => {
        diff(MPUpload, MPDownloadCopy, done);
    });

    test('should delete multipart uploaded object', done => {
        exec(['rm', `s3://${bucket}/${MPUpload}`], done);
    });

    test('should delete copy of multipart uploaded object', done => {
        exec(['rm', `s3://${bucket}/${MPUpload}copy`], done);
    });

    test('should not be able to get deleted object', done => {
        exec(['get', `s3://${bucket}/${MPUpload}`, download], done, 12);
    });
});

MPUploadSplitter.forEach(file => {
    describe('s3cmd multipart upload with splitter in name', () => {
        this.timeout(0);
        beforeAll(done => {
            this.timeout(60000);
            createFile(file, 16777216, done);
        });

        afterAll(done => {
            deleteFile(file, () => {
                deleteFile(MPDownload, done);
            });
        });

        test('should put an object via a multipart upload', done => {
            exec(['put', file, `s3://${bucket}`], done);
        });

        test('should list multipart uploads', done => {
            exec(['multipart', `s3://${bucket}`], done);
        });

        test('should get an object that was put via multipart upload', done => {
            exec(['get', `s3://${bucket}/${file}`, MPDownload], done);
        });

        test('downloaded file should equal uploaded file', done => {
            diff(file, MPDownload, done);
        });

        test('should delete multipart uploaded object', done => {
            exec(['rm', `s3://${bucket}/${file}`], done);
        });

        test('should not be able to get deleted object', done => {
            exec(['get', `s3://${bucket}/${file}`, download], done, 12);
        });
    });
});


describe('s3cmd put, get and delete object with spaces ' +
    'in object key names', () => {
    this.timeout(0);
    const keyWithSpacesAndPluses = 'key with spaces and + pluses +';
    beforeAll(done => {
        createFile(upload, 1000, done);
    });
    afterAll(done => {
        deleteFile(upload, () => {
            deleteFile(download, done);
        });
    });

    const bucket = 'freshbucket';

    test('should put a valid bucket', done => {
        exec(['mb', `s3://${bucket}`], done);
    });

    test('should put file with spaces in key in existing bucket', done => {
        exec(['put', upload, `s3://${bucket}/${keyWithSpacesAndPluses}`], done);
    });

    test('should get file with spaces', done => {
        exec(['get', `s3://${bucket}/${keyWithSpacesAndPluses}`, download],
             done);
    });

    test('should list bucket showing file with spaces', done => {
        checkRawOutput(['ls', `s3://${bucket}`], `s3://${bucket}`,
        keyWithSpacesAndPluses, 'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });

    test('downloaded file should equal uploaded file', done => {
        diff(upload, download, done);
    });

    test('should delete file with spaces', done => {
        exec(['del', `s3://${bucket}/${keyWithSpacesAndPluses}`], done);
    });

    test('should delete empty bucket', done => {
        exec(['rb', `s3://${bucket}`], done);
    });
});

describe('s3cmd info', () => {
    const bucket = 's3cmdinfobucket';

    beforeEach(done => {
        exec(['mb', `s3://${bucket}`], done);
    });

    afterEach(done => {
        exec(['rb', `s3://${bucket}`], done);
    });

    // test that POLICY and CORS are returned as 'none'
    test('should find that policy has a value of none', done => {
        checkRawOutput(['info', `s3://${bucket}`], 'policy', 'none',
        'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });

    test('should find that cors has a value of none', done => {
        checkRawOutput(['info', `s3://${bucket}`], 'cors', 'none',
        'stdout', foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });

    describe('after putting cors configuration', () => {
        const corsConfig = '<?xml version="1.0" encoding="UTF-8" ' +
        'standalone="yes"?><CORSConfiguration><CORSRule>' +
        '<AllowedMethod>PUT</AllowedMethod>' +
        '<AllowedOrigin>http://www.allowedorigin.com</AllowedOrigin>' +
        '</CORSRule></CORSConfiguration>';
        const filename = 'corss3cmdfile';

        beforeEach(done => {
            fs.writeFile(filename, corsConfig, () => {
                exec(['setcors', filename, `s3://${bucket}`], done);
            });
        });

        afterEach(done => {
            deleteFile(filename, done);
        });

        test('should find that cors has a value', done => {
            checkRawOutput(['info', `s3://${bucket}`], 'cors', corsConfig,
            'stdout', foundIt => {
                expect(foundIt).toBeTruthy();
                done();
            });
        });
    });
});

describe('s3cmd delBucket', () => {
    test('delete non-empty bucket, should fail', done => {
        exec(['rb', `s3://${bucket}`], done, 13);
    });

    test('should delete remaining object', done => {
        exec(['rm', `s3://${bucket}/${upload}`], done);
    });

    test('should delete empty bucket', done => {
        exec(['rb', `s3://${bucket}`], done);
    });

    test('try to get the deleted bucket, should fail', done => {
        exec(['ls', `s3://${bucket}`], done, 12);
    });
});

describe('s3cmd recursive delete with objects put by MPU', () => {
    const upload16MB = 'test16MB';
    beforeAll(function setup(done) {
        this.timeout(120000);
        exec(['mb', `s3://${bucket}`], () => {
            createFile(upload16MB, 16777216, () => {
                async.timesLimit(50, 1, (n, next) => {
                    exec([
                        'put', upload16MB, `s3://${bucket}/key${n}`,
                        '--multipart-chunk-size-mb=5',
                    ], next);
                }, done);
            });
        });
    });

    test('should delete all the objects and the bucket', done => {
        exec(['rb', '-r', `s3://${bucket}`, '--debug'], done);
    });

    afterAll(done => {
        deleteFile(upload16MB, done);
    });
});

describeSkipIfE2E('If no location is sent with the request', () => {
    beforeEach(done => {
        exec(['mb', `s3://${bucket}`], done);
    });
    afterEach(done => {
        exec(['rb', `s3://${bucket}`], done);
    });
    // WARNING: change "us-east-1" to another locationConstraint depending
    // on the restEndpoints (./config.json)
    test('endpoint should be used to determine the locationConstraint', done => {
        checkRawOutput(['info', `s3://${bucket}`], 'Location', 'us-east-1',
        'stdout',
        foundIt => {
            expect(foundIt).toBeTruthy();
            done();
        });
    });
});
