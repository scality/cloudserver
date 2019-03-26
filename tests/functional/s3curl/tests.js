'use strict'; // eslint-disable-line strict

const assert = require('assert');
const proc = require('child_process');
const process = require('process');
const parseString = require('xml2js').parseString;

const conf = require('../../../lib/Config').config;

const transport = conf.https ? 'https' : 'http';
let sslArguments = ['-s'];
if (conf.https && conf.https.ca) {
    sslArguments = ['-s', '--cacert', conf.httpsPath.ca];
}
const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const program = `${__dirname}/s3curl.pl`;
const upload = 'test1MB';
const aclUpload = 'test500KB';
const download = 'tmpfile';
const bucket = 's3universe';
const aclBucket = 'acluniverse';
const nonexist = 'nonexist';
const prefix = 'topLevel';
const delimiter = '/';
let ownerCanonicalId = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d52'
    + '18e7cd47ef2be';
const endpoint = `${transport}://${ipAddress}:8000`;

// Let's precompute a few paths
const bucketPath = `${endpoint}/${bucket}`;
const basePath = `${prefix}${delimiter}`;
const prefixedPath = `${bucketPath}/${basePath}`;

/*
 * XXX TODO FIXME TODO XXX
 * The following codeblock aims at improving flexibility of this tests by
 * overriding some specific test values from the environment. This is partly
 * aimed at re-using this test suite in a different context (end-to-end
 * testing rather than functional testing)
 * XXX TODO FIXME TODO XXX
 */
if (process.env.S3_TESTVAL_OWNERCANONICALID) {
    ownerCanonicalId = process.env.S3_TESTVAL_OWNERCANONICALID;
}

function diff(putFile, receivedFile, done) {
    process.stdout.write(`diff ${putFile} ${receivedFile}\n`);
    proc.spawn('diff', [putFile, receivedFile]).on('exit', code => {
        expect(code).toBe(0);
        done();
    });
}


function createFile(name, bytes, callback) {
    process.stdout.write(`dd if=/dev/urandom of=${name} bs=${bytes} count=1\n`);
    let ret = proc.spawnSync('dd', ['if=/dev/urandom', `of=${name}`,
        `bs=${bytes}`, 'count=1'], { stdio: 'inherit' });
    expect(ret.status).toBe(0);
    process.stdout.write(`chmod ugoa+rw ${name}\n`);
    ret = proc.spawnSync('chmod', ['ugo+rw', name], { stdio: 'inherit' });
    expect(ret.status).toBe(0);
    callback();
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    proc.spawnSync('rm', [file]);
    callback();
}

// Test whether the proper xml error response is received
function assertError(data, expectedOutput, done) {
    parseString(data, (err, result) => {
        expect(result.Error.Code[0]).toBe(expectedOutput);
        done();
    });
}

// Get stdout and stderr stringified
function provideRawOutput(args, cb) {
    const av = args.concat(sslArguments);
    process.stdout.write(`${program} ${av}\n`);
    const child = proc.spawn(program, av);
    const procData = {
        stdout: '',
        stderr: '',
    };
    child.stdout.on('data', data => {
        procData.stdout += data.toString();
    });
    child.stderr.on('data', data => {
        procData.stderr += data.toString();
    });
    child.on('error', cb);
    child.on('close', code => {
        process.stdout.write(`s3curl return code : ${code}\n`);
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
            } else {
                process.stdout.write(`${lines.join('\n')}\n`);
                return cb(new Error("Can't find line in http response code"));
            }
        } else {
            process.stdout.write(`stdout: ${procData.stdout}`);
            return cb(new Error('Cannot have stderr'));
        }
        return cb(httpCode, procData);
    });
}

/**
 * @brief This function creates a bunch of objects, using a file as a basis for
 * the data to PUT
 *
 * @param {String} filepath The path of the file to use as source data for the
 *                          creation of all objects requested to this function
 * @param {String[]} objectPaths  The paths of the objects to create, in order.
 * @param {Callback} cb The callback to call once all objects have been created.
 *
 * @return {undefined}
 */
function putObjects(filepath, objectPaths, cb) {
    provideRawOutput(
        [`--put=${filepath}`, '--', objectPaths[0], '-v'],
        httpCode => {
            expect(httpCode).toBe('200 OK');
            if (objectPaths.length > 1) {
                return putObjects(filepath, objectPaths.slice(1), cb);
            }
            return cb();
        });
}

/**
 * @brief This function deletes a list of objects and buckets.
 * Items can be either objects or buckets, since s3curl manages them all
 * the same through only a --delete option.
 *
 * @param {String[]} items The paths of buckets and/or objects to delete, in
 *                         order.
 * @param {Callback} cb The callback to call once all items have been deleted.
 *
 * @return {undefined}
 */
function deleteRemoteItems(items, cb) {
    provideRawOutput(
        ['--delete', '--', items[0], '-v'],
        httpCode => {
            expect(httpCode).toBe('204 NO CONTENT');
            if (items.length > 1) {
                return deleteRemoteItems(items.slice(1), cb);
            }
            return cb();
        });
}

describe('s3curl put delete buckets', () => {
    describe('s3curl put buckets', () => {
        afterAll(done => {
            deleteRemoteItems([bucketPath], done);
        });

        test('should put a valid bucket', done => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode).toBe('200 OK');
                    done();
                });
        });

        test('should return 409 error in new regions and 200 in us-east-1 ' +
            '(legacyAWSBehvior) when try to put a bucket with a name ' +
            'already being used', done => {
            provideRawOutput(['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode === '200 OK'
                    || httpCode === '409 CONFLICT').toBeTruthy();
                    done();
                });
        });

        test('should not be able to put a bucket with invalid xml' +
            ' in the post body', done => {
            provideRawOutput([
                '--createBucket',
                '--',
                '--data',
                'malformedxml',
                bucketPath,
                '-v',
            ], (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'MalformedXML',
                    done);
            });
        });

        test('should not be able to put a bucket with xml that does' +
            ' not conform to s3 docs for locationConstraint', done => {
            provideRawOutput([
                '--createBucket',
                '--',
                '--data',
                '<Hello>a</Hello>',
                bucketPath,
                '-v',
            ], (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'MalformedXML',
                    done);
            });
        });

        test('should not be able to put a bucket with an invalid name', done => {
            provideRawOutput(
                ['--createBucket', '--', `${endpoint}/2`, '-v'],
                (httpCode, rawOutput) => {
                    expect(httpCode).toBe('400 BAD REQUEST');
                    assertError(rawOutput.stdout, 'InvalidBucketName', done);
                });
        });
    });

    describe('s3curl delete bucket', () => {
        beforeAll(done => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode).toBe('200 OK');
                    done();
                });
        });

        afterAll(done => {
            deleteRemoteItems([bucketPath], done);
        });

        test('should be able to delete a bucket', done => {
            deleteRemoteItems([bucketPath], done);
        });

        test('should not be able to get a bucket that was deleted', done => {
            provideRawOutput(
                ['--', bucketPath, '-v'],
                (httpCode, rawOutput) => {
                    expect(httpCode).toBe('404 NOT FOUND');
                    assertError(rawOutput.stdout, 'NoSuchBucket', done);
                });
        });

        test('should be able to create a bucket with a name' +
            'of a bucket that has previously been deleted', done => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode).toBe('200 OK');
                    done();
                });
        });
    });
});

describe('s3curl put and get bucket ACLs', () => {
    afterAll(done => {
        deleteRemoteItems([
            `${endpoint}/${aclBucket}`,
            `${endpoint}/${aclBucket}2`,
        ], done);
    });

    test('should be able to create a bucket with a canned ACL', done => {
        provideRawOutput([
            '--createBucket',
            '--',
            '-H',
            'x-amz-acl:public-read',
            `${endpoint}/${aclBucket}`,
            '-v',
        ], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test('should be able to get a canned ACL', done => {
        provideRawOutput(
            ['--', `${endpoint}/${aclBucket}?acl`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, xml) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    expect(xml.AccessControlPolicy
                        .Owner[0].ID[0]).toBe(ownerCanonicalId);
                    expect(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Grantee[0].ID[0]).toBe(ownerCanonicalId);
                    expect(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Permission[0]).toBe('FULL_CONTROL');
                    expect(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[1]
                        .Grantee[0].URI[0]).toBe('http://acs.amazonaws.com/groups/global/AllUsers');
                    expect(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[1]
                        .Permission[0]).toBe('READ');
                    done();
                });
            });
    });

    test('should be able to create a bucket with a specific ACL', done => {
        provideRawOutput([
            '--createBucket',
            '--',
            '-H',
            'x-amz-grant-read:uri=' +
                'http://acs.amazonaws.com/groups/global/AllUsers',
            `${endpoint}/${aclBucket}2`,
            '-v',
        ], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test('should be able to get a specifically set ACL', done => {
        provideRawOutput(
            ['--', `${endpoint}/${aclBucket}2?acl`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, xml) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    expect(xml.AccessControlPolicy
                        .Owner[0].ID[0]).toBe(ownerCanonicalId);
                    expect(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Grantee[0].URI[0]).toBe('http://acs.amazonaws.com/groups/global/AllUsers');
                    expect(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Permission[0]).toBe('READ');
                    done();
                });
            });
    });
});

describe('s3curl getService', () => {
    beforeAll(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                provideRawOutput(
                    ['--createBucket', '--', `${endpoint}/${aclBucket}`, '-v'],
                    httpCode => {
                        expect(httpCode).toBe('200 OK');
                        done();
                    });
            });
    });

    afterAll(done => {
        deleteRemoteItems([
            bucketPath,
            `${endpoint}/${aclBucket}`,
        ], done);
    });

    test('should get a list of all buckets created by user account', done => {
        provideRawOutput(
            ['--', `${endpoint}`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, xml) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    const bucketNames = xml.ListAllMyBucketsResult
                                           .Buckets[0].Bucket
                                           .map(item => item.Name[0]);
                    const whereIsMyBucket = bucketNames.indexOf(bucket);
                    expect(whereIsMyBucket > -1).toBeTruthy();
                    const whereIsMyAclBucket = bucketNames.indexOf(aclBucket);
                    expect(whereIsMyAclBucket > -1).toBeTruthy();
                    done();
                });
            });
    });
});

describe('s3curl putObject', () => {
    beforeAll(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                createFile(upload, 1048576, done);
            });
    });

    afterAll(done => {
        deleteRemoteItems([
            `${prefixedPath}${upload}1`,
            `${prefixedPath}${upload}2`,
            `${prefixedPath}${upload}3`,
            bucketPath,
        ], done);
    });

    // curl behavior is not consistent across the environments
    // skipping the test for now
    test.skip('should not be able to put an object if request does not have ' +
        'content-length header', done => {
        provideRawOutput([
            '--debug',
            `--put=${upload}`,
            '--',
            '-H',
            'content-length:',
            `${prefixedPath}${upload}1`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('411 LENGTH REQUIRED');
            assertError(rawOutput.stdout, 'MissingContentLength', done);
        });
    });

    test('should not be able to put an object if content-md5 header is ' +
    'invalid', done => {
        provideRawOutput(['--debug', `--put=${upload}`,
            '--contentMd5', 'toto', '--',
            `${endpoint}/${bucket}/` +
            `${prefix}${delimiter}${upload}1`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'InvalidDigest', done);
            });
    });

    // skip until we figure out how to parse the response in the CI
    test.skip('should not be able to put an object if content-md5 header is ' +
    'mismatched MD5', done => {
        provideRawOutput(['--debug', `--put=${upload}`,
            '--contentMd5', 'rL0Y20zC+Fzt72VPzMSk2A==', '--',
            `${endpoint}/${bucket}/` +
            `${prefix}${delimiter}${upload}1`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'BadDigest', done);
            });
    });

    test(
        'should not be able to put an object in a bucket with an invalid name',
        done => {
            provideRawOutput([
                '--debug',
                `--put=${upload}`,
                '--',
                `${endpoint}/2/${basePath}${upload}1`,
                '-v',
            ], (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'InvalidBucketName', done);
            });
        }
    );

    test(
        'should not be able to put an object in a bucket that does not exist',
        done => {
            provideRawOutput([
                '--debug',
                `--put=${upload}`,
                '--',
                `${endpoint}/${nonexist}/${basePath}${upload}1`,
                '-v',
            ], (httpCode, rawOutput) => {
                expect(httpCode).toBe('404 NOT FOUND');
                assertError(rawOutput.stdout, 'NoSuchBucket', done);
            });
        }
    );

    test('should put first object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput([
            '--debug',
            `--put=${upload}`,
            '--',
            `${prefixedPath}${upload}1`,
            '-v',
        ], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test('should put second object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput(
            [`--put=${upload}`, '--', `${prefixedPath}${upload}2`, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                done();
            });
    });

    test('should put third object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput([
            `--put=${upload}`,
            '--',
            `${prefixedPath}${upload}3`,
            '-v',
        ], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });
});

describe('s3curl getBucket', () => {
    const objects = [
        `${prefixedPath}${upload}1`,
        `${prefixedPath}${upload}2`,
        `${prefixedPath}${upload}3`,
    ];

    beforeAll(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                createFile(upload, 1048576, () => {
                    putObjects(upload, objects, done);
                });
            });
    });

    afterAll(done => {
        const toRemove = objects.concat([bucketPath]);
        deleteRemoteItems(toRemove, done);
    });

    test('should list all objects if no prefix or delimiter specified', done => {
        provideRawOutput(
            ['--', bucketPath, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    expect(result.ListBucketResult
                        .Contents[0].Key[0]).toBe(`${basePath}${upload}1`);
                    expect(result.ListBucketResult
                        .Contents[1].Key[0]).toBe(`${basePath}${upload}2`);
                    expect(result.ListBucketResult
                        .Contents[2].Key[0]).toBe(`${basePath}${upload}3`);
                    done();
                });
            });
    });

    test('should list a common prefix if a common prefix and delimiter are ' +
    'specified', done => {
        provideRawOutput([
            '--',
            `${bucketPath}?delimiter=${delimiter}&prefix=${prefix}`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                expect(result.ListBucketResult
                    .CommonPrefixes[0].Prefix[0]).toBe(basePath);
                done();
            });
        });
    });

    test('should not list a common prefix if no delimiter is specified', done => {
        provideRawOutput(
            ['--', `${bucketPath}?&prefix=${prefix}`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    const keys = Object.keys(result.ListBucketResult);
                    const location = keys.indexOf('CommonPrefixes');
                    expect(location).toBe(-1);
                    expect(result.ListBucketResult
                        .Contents[0].Key[0]).toBe(`${basePath}${upload}1`);
                    done();
                });
            });
    });

    test('should provide a next marker if maxs keys exceeded ' +
        'and delimiter specified', done => {
        provideRawOutput(
            ['--', `${bucketPath}?delimiter=x&max-keys=2`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    expect(result.ListBucketResult
                        .NextMarker[0]).toBe(`${basePath}${upload}2`);
                    expect(result.ListBucketResult
                        .IsTruncated[0]).toBe('true');
                    done();
                });
            });
    });

    test('should return InvalidArgument error with negative max-keys', done => {
        provideRawOutput(
            ['--', `${bucketPath}?&max-keys=-2`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'InvalidArgument', done);
            });
    });

    test('should return InvalidArgument error with invalid max-keys', done => {
        provideRawOutput(
            ['--', `${bucketPath}?max-keys='slash'`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'InvalidArgument', done);
            });
    });

    test('should return an EncodingType XML tag with the value "url"', done => {
        provideRawOutput(
            ['--', bucketPath, '-G', '-d', 'encoding-type=url', '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    expect(result.ListBucketResult
                        .EncodingType[0]).toBe('url');
                    done();
                });
            });
    });

    test('should return an InvalidArgument error when given an invalid ' +
        'encoding type', done => {
        provideRawOutput(
            ['--', bucketPath, '-G', '-d', 'encoding-type=invalidURI', '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    expect(result.Error.Code[0]).toBe('InvalidArgument');
                    expect(result.Error.Message[0]).toBe('Invalid Encoding Method specified in Request');
                    done();
                });
            });
    });
});

describe('s3curl head bucket', () => {
    beforeAll(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                done();
            });
    });

    afterAll(done => {
        deleteRemoteItems([bucketPath], done);
    });

    test('should return a 404 response if bucket does not exist', done => {
        provideRawOutput(
            ['--head', '--', `${endpoint}/${nonexist}`, '-v'],
            httpCode => {
                expect(httpCode).toBe('404 NOT FOUND');
                done();
            });
    });

    test('should return a 200 response if bucket exists' +
        ' and user is authorized', done => {
        provideRawOutput(
            ['--head', '--', bucketPath, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                done();
            });
    });
});

describe('s3curl getObject', () => {
    beforeAll(done => {
        createFile(upload, 1048576, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode).toBe('200 OK');
                    done();
                });
        });
    });

    afterAll(done => {
        const objects = [
            `${bucketPath}/getter`,
            bucketPath,
        ];
        deleteRemoteItems(objects, () => {
            deleteFile(upload, () => deleteFile(download, done));
        });
    });

    test('should put object with metadata', done => {
        provideRawOutput([
            `--put=${upload}`,
            '--',
            '-H',
            'x-amz-meta-mine:BestestObjectEver',
            `${bucketPath}/getter`,
            '-v',
        ], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test('should get an existing file in an existing bucket', done => {
        provideRawOutput(
            ['--', '-o', download, `${bucketPath}/getter`, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                done();
            });
    });

    test('downloaded file should equal uploaded file', done => {
        diff(upload, download, done);
    });
});

describe('s3curl head object', () => {
    beforeAll(done => {
        createFile(upload, 1048576, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode).toBe('200 OK');
                    provideRawOutput([
                        `--put=${upload}`,
                        '--',
                        '-H',
                        'x-amz-meta-mine:BestestObjectEver',
                        `${bucketPath}/getter`,
                        '-v',
                    ], httpCode => {
                        expect(httpCode).toBe('200 OK');
                        done();
                    });
                });
        });
    });

    afterAll(done => {
        deleteRemoteItems([
            `${bucketPath}/getter`,
            bucketPath,
        ], done);
    });

    test("should get object's metadata", done => {
        provideRawOutput(
            ['--head', '--', `${bucketPath}/getter`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                const lines = rawOutput.stdout.split('\n');
                const userMetadata = 'x-amz-meta-mine: BestestObjectEver\r';
                expect(lines.indexOf(userMetadata) > -1).toBeTruthy();
                expect(rawOutput.stdout.indexOf('ETag') > -1).toBeTruthy();
                done();
            });
    });
});

describe('s3curl object ACLs', () => {
    beforeAll(done => {
        createFile(aclUpload, 512000, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode).toBe('200 OK');
                    done();
                });
        });
    });

    afterAll(done => {
        deleteRemoteItems([
            `${bucketPath}/${aclUpload}withcannedacl`,
            `${bucketPath}/${aclUpload}withspecificacl`,
            bucketPath,
        ], () => deleteFile(aclUpload, done));
    });

    test('should put an object with a canned ACL', done => {
        provideRawOutput([
            `--put=${aclUpload}`,
            '--',
            '-H',
            'x-amz-acl:public-read',
            `${bucketPath}/${aclUpload}withcannedacl`,
            '-v',
        ], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test("should get an object's canned ACL", done => {
        provideRawOutput([
            '--',
            `${bucketPath}/${aclUpload}withcannedacl?acl`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                expect(result.AccessControlPolicy
                    .Owner[0].ID[0]).toBe(ownerCanonicalId);
                expect(result.AccessControlPolicy
                    .AccessControlList[0].Grant[0]
                    .Grantee[0].ID[0]).toBe(ownerCanonicalId);
                expect(result.AccessControlPolicy
                    .AccessControlList[0].Grant[0]
                    .Permission[0]).toBe('FULL_CONTROL');
                expect(result.AccessControlPolicy
                    .AccessControlList[0].Grant[1]
                    .Grantee[0].URI[0]).toBe('http://acs.amazonaws.com/groups/global/AllUsers');
                expect(result.AccessControlPolicy
                    .AccessControlList[0].Grant[1]
                    .Permission[0]).toBe('READ');
                done();
            });
        });
    });

    test('should put an object with a specific ACL', done => {
        provideRawOutput([
            `--put=${aclUpload}`,
            '--',
            '-H',
            'x-amz-grant-read:uri=' +
                'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
            `${bucketPath}/${aclUpload}withspecificacl`,
            '-v',
        ], httpCode => {
            expect(httpCode).toBe('200 OK');
            done();
        });
    });

    test("should get an object's specific ACL", done => {
        provideRawOutput([
            '--',
            `${bucketPath}/${aclUpload}withspecificacl?acl`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                expect(result.AccessControlPolicy
                    .Owner[0].ID[0]).toBe(ownerCanonicalId);
                expect(result.AccessControlPolicy
                    .AccessControlList[0].Grant[0]
                    .Grantee[0].URI[0]).toBe('http://acs.amazonaws.com/groups/global/' +
                'AuthenticatedUsers');
                expect(result.AccessControlPolicy
                    .AccessControlList[0].Grant[0]
                    .Permission[0]).toBe('READ');
                done();
            });
        });
    });

    test('should return a NoSuchKey error if try to get an object' +
        'ACL for an object that does not exist', done => {
        provideRawOutput(
            ['--', `${bucketPath}/keydoesnotexist?acl`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('404 NOT FOUND');
                assertError(rawOutput.stdout, 'NoSuchKey', done);
            });
    });
});

describe('s3curl multipart upload', () => {
    const key = 'multipart';
    const upload = 'smallUpload';
    let uploadId = null;

    beforeAll(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                expect(httpCode).toBe('200 OK');
                // initiate mpu
                provideRawOutput([
                    '--',
                    '-X',
                    'POST',
                    `${bucketPath}/${key}?uploads`,
                    '-v',
                ], (httpCode, rawOutput) => {
                    parseString(rawOutput.stdout, (err, result) => {
                        if (err) {
                            assert.ifError(err);
                        }
                        uploadId =
                        result.InitiateMultipartUploadResult.UploadId[0];
                        // create file to copy
                        createFile(upload, 100, () => {
                            // put file to copy
                            putObjects(upload, [`${bucketPath}/copyme`], done);
                        });
                    });
                });
            });
    });

    afterAll(done => {
        deleteRemoteItems([
            `${bucketPath}/copyme`,
            `${bucketPath}/${key}?uploadId=${uploadId}`,
            bucketPath,
        ], () => deleteFile(upload, done));
    });

    test('should return error for list parts call if no key sent', done => {
        provideRawOutput([
            '--',
            `${bucketPath}?uploadId=${uploadId}`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('400 BAD REQUEST');
            assertError(rawOutput.stdout, 'InvalidRequest', done);
        });
    });

    test('should return error for put part call if no key sent', done => {
        provideRawOutput([
            '--',
            '-X', 'PUT',
            `${bucketPath}?partNumber=1&uploadId=${uploadId}`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('400 BAD REQUEST');
            assertError(rawOutput.stdout, 'InvalidRequest', done);
        });
    });

    test('should return error for complete mpu call if no key sent', done => {
        provideRawOutput([
            '--',
            '-X', 'POST',
            `${bucketPath}?uploadId=${uploadId}`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('400 BAD REQUEST');
            assertError(rawOutput.stdout, 'InvalidRequest', done);
        });
    });

    test('should return error for abort mpu call if no key sent', done => {
        provideRawOutput([
            '--',
            '-X', 'DELETE',
            `${bucketPath}?uploadId=${uploadId}`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('400 BAD REQUEST');
            assertError(rawOutput.stdout, 'InvalidRequest', done);
        });
    });

    test('should list parts of multipart upload with no parts', done => {
        provideRawOutput([
            '--',
            `${bucketPath}/${key}?uploadId=${uploadId}`,
            '-v',
        ], (httpCode, rawOutput) => {
            expect(httpCode).toBe('200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                expect(result.ListPartsResult.UploadId[0]).toBe(uploadId);
                expect(result.ListPartsResult.Bucket[0]).toBe(bucket);
                expect(result.ListPartsResult.Key[0]).toBe(key);
                expect(result.ListPartsResult.Part).toBe(undefined);
                done();
            });
        });
    });

    test('should copy a part and return lastModified as ISO', done => {
        provideRawOutput(
            ['--', `${bucketPath}/${key}?uploadId=${uploadId}&partNumber=1`,
                '-X', 'PUT', '-H',
                `x-amz-copy-source:${bucket}/copyme`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    const lastModified = result.CopyPartResult
                        .LastModified[0];
                    const isoDateString = new Date(lastModified).toISOString();
                    expect(lastModified).toBe(isoDateString);
                    done();
                });
            });
    });
});

describe('s3curl copy object', () => {
    beforeAll(done => {
        createFile(upload, 1048576, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    expect(httpCode).toBe('200 OK');
                    putObjects(upload, [`${bucketPath}/copyme`], done);
                });
        });
    });

    afterAll(done => {
        deleteRemoteItems([
            `${bucketPath}/copyme`,
            `${bucketPath}/iamacopy`,
            bucketPath,
        ], () => deleteFile(upload, done));
    });

    test('should copy an object and return lastModified as ISO', done => {
        provideRawOutput(
            ['--', `${bucketPath}/iamacopy`, '-X', 'PUT', '-H',
                `x-amz-copy-source:${bucket}/copyme`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    const lastModified = result.CopyObjectResult
                        .LastModified[0];
                    const isoDateString = new Date(lastModified).toISOString();
                    expect(lastModified).toBe(isoDateString);
                    done();
                });
            });
    });
});

describe('s3curl multi-object delete', () => {
    test('should return an error if md5 is wrong', done => {
        provideRawOutput(['--post', 'multiDelete.xml', '--contentMd5',
            'p5/WA/oEr30qrEEl21PAqw==', '--',
            `${endpoint}/${bucket}/?delete`, '-v'],
            (httpCode, rawOutput) => {
                expect(httpCode).toBe('400 BAD REQUEST');
                assertError(rawOutput.stdout, 'BadDigest',
                    done);
            });
    });
});
