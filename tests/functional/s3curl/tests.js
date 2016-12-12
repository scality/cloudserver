'use strict'; // eslint-disable-line strict

const assert = require('assert');
const proc = require('child_process');
const process = require('process');
const parseString = require('xml2js').parseString;

require('babel-core/register');
const conf = require('../../../lib/Config').default;

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
        assert.strictEqual(code, 0);
        done();
    });
}


function createFile(name, bytes, callback) {
    process.stdout.write(`dd if=/dev/urandom of=${name} bs=${bytes} count=1\n`);
    proc.spawn('dd', ['if=/dev/urandom', `of=${name}`,
        `bs=${bytes}`, 'count=1'], { stdio: 'inherit' }).on('exit', code => {
            assert.strictEqual(code, 0);
            process.stdout.write(`chmod ugoa+rw ${name}\n`);
            proc.spawn('chmod', ['ugo+rw', name], { stdio: 'inherit' })
                .on('exit', code => {
                    assert.strictEqual(code, 0);
                    callback();
                });
        });
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    proc.spawn('rm', [file]).on('exit', () => {
        callback();
    });
}

// Test whether the proper xml error response is received
function assertError(data, expectedOutput, done) {
    parseString(data, (err, result) => {
        assert.strictEqual(result.Error.Code[0], expectedOutput);
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
            assert.strictEqual(httpCode, '200 OK');
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
            assert.strictEqual(httpCode, '204 NO CONTENT');
            if (items.length > 1) {
                return deleteRemoteItems(items.slice(1), cb);
            }
            return cb();
        });
}

describe('s3curl put delete buckets', () => {
    describe('s3curl put buckets', () => {
        after(done => {
            deleteRemoteItems([bucketPath], done);
        });

        it('should put a valid bucket', done => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    done();
                });
        });

        it('should not be able to put a bucket with a name ' +
            'already being used', done => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                (httpCode, rawOutput) => {
                    assert.strictEqual(httpCode, '409 CONFLICT');
                    assertError(rawOutput.stdout, 'BucketAlreadyOwnedByYou',
                        done);
                });
        });

        it('should not be able to put a bucket with invalid xml' +
            ' in the post body', done => {
            provideRawOutput([
                '--createBucket',
                '--',
                '--data',
                'malformedxml',
                bucketPath,
                '-v',
            ], (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '400 BAD REQUEST');
                assertError(rawOutput.stdout, 'MalformedXML',
                    done);
            });
        });

        it('should not be able to put a bucket with xml that does' +
            ' not conform to s3 docs for locationConstraint', done => {
            provideRawOutput([
                '--createBucket',
                '--',
                '--data',
                '<Hello>a</Hello>',
                bucketPath,
                '-v',
            ], (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '400 BAD REQUEST');
                assertError(rawOutput.stdout, 'MalformedXML',
                    done);
            });
        });

        it('should not be able to put a bucket with an invalid name', done => {
            provideRawOutput(
                ['--createBucket', '--', `${endpoint}/2`, '-v'],
                (httpCode, rawOutput) => {
                    assert.strictEqual(httpCode, '400 BAD REQUEST');
                    assertError(rawOutput.stdout, 'InvalidBucketName', done);
                });
        });
    });

    describe('s3curl delete bucket', () => {
        before(done => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    done();
                });
        });

        after(done => {
            deleteRemoteItems([bucketPath], done);
        });

        it('should be able to delete a bucket', done => {
            deleteRemoteItems([bucketPath], done);
        });

        it('should not be able to get a bucket that was deleted', done => {
            provideRawOutput(
                ['--', bucketPath, '-v'],
                (httpCode, rawOutput) => {
                    assert.strictEqual(httpCode, '404 NOT FOUND');
                    assertError(rawOutput.stdout, 'NoSuchBucket', done);
                });
        });

        it('should be able to create a bucket with a name' +
            'of a bucket that has previously been deleted', done => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    done();
                });
        });
    });
});

describe('s3curl put and get bucket ACLs', () => {
    after(done => {
        deleteRemoteItems([
            `${endpoint}/${aclBucket}`,
            `${endpoint}/${aclBucket}2`,
        ], done);
    });

    it('should be able to create a bucket with a canned ACL', done => {
        provideRawOutput([
            '--createBucket',
            '--',
            '-H',
            'x-amz-acl:public-read',
            `${endpoint}/${aclBucket}`,
            '-v',
        ], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should be able to get a canned ACL', done => {
        provideRawOutput(
            ['--', `${endpoint}/${aclBucket}?acl`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, xml) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    assert.strictEqual(xml.AccessControlPolicy
                        .Owner[0].ID[0], ownerCanonicalId);
                    assert.strictEqual(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Grantee[0].ID[0], ownerCanonicalId);
                    assert.strictEqual(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Permission[0], 'FULL_CONTROL');
                    assert.strictEqual(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[1]
                        .Grantee[0].URI[0],
                        'http://acs.amazonaws.com/groups/global/AllUsers');
                    assert.strictEqual(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[1]
                        .Permission[0], 'READ');
                    done();
                });
            });
    });

    it('should be able to create a bucket with a specific ACL', done => {
        provideRawOutput([
            '--createBucket',
            '--',
            '-H',
            'x-amz-grant-read:uri=' +
                'http://acs.amazonaws.com/groups/global/AllUsers',
            `${endpoint}/${aclBucket}2`,
            '-v',
        ], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should be able to get a specifically set ACL', done => {
        provideRawOutput(
            ['--', `${endpoint}/${aclBucket}2?acl`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, xml) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    assert.strictEqual(xml.AccessControlPolicy
                        .Owner[0].ID[0], ownerCanonicalId);
                    assert.strictEqual(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Grantee[0].URI[0],
                        'http://acs.amazonaws.com/groups/global/AllUsers');
                    assert.strictEqual(xml.AccessControlPolicy
                        .AccessControlList[0].Grant[0]
                        .Permission[0], 'READ');
                    done();
                });
            });
    });
});

describe('s3curl getService', () => {
    before(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                provideRawOutput(
                    ['--createBucket', '--', `${endpoint}/${aclBucket}`, '-v'],
                    httpCode => {
                        assert.strictEqual(httpCode, '200 OK');
                        done();
                    });
            });
    });

    after(done => {
        deleteRemoteItems([
            bucketPath,
            `${endpoint}/${aclBucket}`,
        ], done);
    });

    it('should get a list of all buckets created by user account', done => {
        provideRawOutput(
            ['--', `${endpoint}`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, xml) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    const bucketNames = xml.ListAllMyBucketsResult
                                           .Buckets[0].Bucket
                                           .map(item => item.Name[0]);
                    const whereIsMyBucket = bucketNames.indexOf(bucket);
                    assert(whereIsMyBucket > -1);
                    const whereIsMyAclBucket = bucketNames.indexOf(aclBucket);
                    assert(whereIsMyAclBucket > -1);
                    done();
                });
            });
    });
});

describe('s3curl putObject', () => {
    before(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                createFile(upload, 1048576, done);
            });
    });

    after(done => {
        deleteRemoteItems([
            `${prefixedPath}${upload}1`,
            `${prefixedPath}${upload}2`,
            `${prefixedPath}${upload}3`,
            bucketPath,
        ], done);
    });

    // curl behavior is not consistent across the environments
    // skipping the test for now
    it.skip('should not be able to put an object if request does not have ' +
        'content-length header',
        done => {
            provideRawOutput([
                '--debug',
                `--put=${upload}`,
                '--',
                '-H',
                'content-length:',
                `${prefixedPath}${upload}1`,
                '-v',
            ], (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '411 LENGTH REQUIRED');
                assertError(rawOutput.stdout, 'MissingContentLength', done);
            });
        });

    const itSkipIfNotChill = process.env.CHILL || process.env.IP ?
        it : it.skip;

    itSkipIfNotChill('should be able to put a non rfc ' +
        'header (token contains slash)',
        done => {
            provideRawOutput([
                '--debug',
                `--put=${upload}`,
                '--',
                '-H',
                'x-amz-meta-custom/header: foo',
                `${prefixedPath}${upload}1`,
                '-v',
            ], httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                done();
            });
        });

    itSkipIfNotChill('should be able to get a non rfc header ' +
        '(token contains slash)',
        done => {
            provideRawOutput([
                '--debug',
                '--head',
                '--',
                `${prefixedPath}${upload}1`,
                '-v',
            ], (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                assert(rawOutput.stdout
                    .indexOf('x-amz-meta-custom/header: foo') > -1);
                done();
            });
        });

    it('should not be able to put an object in a bucket with an invalid name',
        done => {
            provideRawOutput([
                '--debug',
                `--put=${upload}`,
                '--',
                `${endpoint}/2/${basePath}${upload}1`,
                '-v',
            ], (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '400 BAD REQUEST');
                assertError(rawOutput.stdout, 'InvalidBucketName', done);
            });
        });

    it('should not be able to put an object in a bucket that does not exist',
        done => {
            provideRawOutput([
                '--debug',
                `--put=${upload}`,
                '--',
                `${endpoint}/${nonexist}/${basePath}${upload}1`,
                '-v',
            ], (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '404 NOT FOUND');
                assertError(rawOutput.stdout, 'NoSuchBucket', done);
            });
        });

    it('should put first object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput([
            '--debug',
            `--put=${upload}`,
            '--',
            `${prefixedPath}${upload}1`,
            '-v',
        ], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should put second object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput(
            [`--put=${upload}`, '--', `${prefixedPath}${upload}2`, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                done();
            });
    });

    it('should put third object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput([
            `--put=${upload}`,
            '--',
            `${prefixedPath}${upload}3`,
            '-v',
        ], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
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

    before(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                createFile(upload, 1048576, () => {
                    putObjects(upload, objects, done);
                });
            });
    });

    after(done => {
        const toRemove = objects.concat([bucketPath]);
        deleteRemoteItems(toRemove, done);
    });

    it('should list all objects if no prefix or delimiter specified', done => {
        provideRawOutput(
            ['--', bucketPath, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    assert.strictEqual(result.ListBucketResult
                        .Contents[0].Key[0], `${basePath}${upload}1`);
                    assert.strictEqual(result.ListBucketResult
                        .Contents[1].Key[0], `${basePath}${upload}2`);
                    assert.strictEqual(result.ListBucketResult
                        .Contents[2].Key[0], `${basePath}${upload}3`);
                    done();
                });
            });
    });

    it('should list a common prefix if a common prefix and delimiter are ' +
    'specified', done => {
        provideRawOutput([
            '--',
            `${bucketPath}?delimiter=${delimiter}&prefix=${prefix}`,
            '-v',
        ], (httpCode, rawOutput) => {
            assert.strictEqual(httpCode, '200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                assert.strictEqual(result.ListBucketResult
                    .CommonPrefixes[0].Prefix[0], basePath);
                done();
            });
        });
    });

    it('should not list a common prefix if no delimiter is specified', done => {
        provideRawOutput(
            ['--', `${bucketPath}?&prefix=${prefix}`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    const keys = Object.keys(result.ListBucketResult);
                    const location = keys.indexOf('CommonPrefixes');
                    assert.strictEqual(location, -1);
                    assert.strictEqual(result.ListBucketResult
                        .Contents[0].Key[0], `${basePath}${upload}1`);
                    done();
                });
            });
    });

    it('should provide a next marker if maxs keys exceeded ' +
        'and delimiter specified', done => {
        provideRawOutput(
            ['--', `${bucketPath}?delimiter=x&max-keys=2`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    assert.strictEqual(result.ListBucketResult
                        .NextMarker[0], `${basePath}${upload}2`);
                    assert.strictEqual(result.ListBucketResult
                        .IsTruncated[0], 'true');
                    done();
                });
            });
    });

    it('should return an EncodingType XML tag with the value "url"', done => {
        provideRawOutput(
            ['--', bucketPath, '-G', '-d', 'encoding-type=url', '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    assert.strictEqual(result.ListBucketResult
                        .EncodingType[0], 'url');
                    done();
                });
            });
    });

    it('should return an InvalidArgument error when given an invalid ' +
        'encoding type', done => {
        provideRawOutput(
            ['--', bucketPath, '-G', '-d', 'encoding-type=invalidURI', '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '400 BAD REQUEST');
                parseString(rawOutput.stdout, (err, result) => {
                    if (err) {
                        assert.ifError(err);
                    }
                    assert.strictEqual(result.Error.Code[0], 'InvalidArgument');
                    assert.strictEqual(result.Error.Message[0],
                        'Invalid Encoding Method specified in Request');
                    done();
                });
            });
    });
});

describe('s3curl head bucket', () => {
    before(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                done();
            });
    });

    after(done => {
        deleteRemoteItems([bucketPath], done);
    });

    it('should return a 404 response if bucket does not exist', done => {
        provideRawOutput(
            ['--head', '--', `${endpoint}/${nonexist}`, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '404 NOT FOUND');
                done();
            });
    });

    it('should return a 200 response if bucket exists' +
        ' and user is authorized', done => {
        provideRawOutput(
            ['--head', '--', bucketPath, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                done();
            });
    });
});

describe('s3curl getObject', () => {
    before(done => {
        createFile(upload, 1048576, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    done();
                });
        });
    });

    after('delete created file and downloaded file', done => {
        const objects = [
            `${bucketPath}/getter`,
            bucketPath,
        ];
        deleteRemoteItems(objects, () => {
            deleteFile(upload, () => deleteFile(download, done));
        });
    });

    it('should put object with metadata', done => {
        provideRawOutput([
            `--put=${upload}`,
            '--',
            '-H',
            'x-amz-meta-mine:BestestObjectEver',
            `${bucketPath}/getter`,
            '-v',
        ], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it('should get an existing file in an existing bucket', done => {
        provideRawOutput(
            ['--', '-o', download, `${bucketPath}/getter`, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
                done();
            });
    });

    it('downloaded file should equal uploaded file', done => {
        diff(upload, download, done);
    });
});

describe('s3curl head object', () => {
    before(done => {
        createFile(upload, 1048576, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    provideRawOutput([
                        `--put=${upload}`,
                        '--',
                        '-H',
                        'x-amz-meta-mine:BestestObjectEver',
                        `${bucketPath}/getter`,
                        '-v',
                    ], httpCode => {
                        assert.strictEqual(httpCode, '200 OK');
                        done();
                    });
                });
        });
    });

    after(done => {
        deleteRemoteItems([
            `${bucketPath}/getter`,
            bucketPath,
        ], done);
    });

    it("should get object's metadata", done => {
        provideRawOutput(
            ['--head', '--', `${bucketPath}/getter`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                const lines = rawOutput.stdout.split('\n');
                const userMetadata = 'x-amz-meta-mine: BestestObjectEver\r';
                assert(lines.indexOf(userMetadata) > -1);
                assert(rawOutput.stdout.indexOf('ETag') > -1);
                done();
            });
    });
});

describe('s3curl object ACLs', () => {
    before(done => {
        createFile(aclUpload, 512000, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    done();
                });
        });
    });

    after(done => {
        deleteRemoteItems([
            `${bucketPath}/${aclUpload}withcannedacl`,
            `${bucketPath}/${aclUpload}withspecificacl`,
            bucketPath,
        ], () => deleteFile(aclUpload, done));
    });

    it('should put an object with a canned ACL', done => {
        provideRawOutput([
            `--put=${aclUpload}`,
            '--',
            '-H',
            'x-amz-acl:public-read',
            `${bucketPath}/${aclUpload}withcannedacl`,
            '-v',
        ], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it("should get an object's canned ACL", done => {
        provideRawOutput([
            '--',
            `${bucketPath}/${aclUpload}withcannedacl?acl`,
            '-v',
        ], (httpCode, rawOutput) => {
            assert.strictEqual(httpCode, '200 OK');
            parseString(rawOutput.stdout, (err, result) => {
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
    });

    it('should put an object with a specific ACL', done => {
        provideRawOutput([
            `--put=${aclUpload}`,
            '--',
            '-H',
            'x-amz-grant-read:uri=' +
                'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
            `${bucketPath}/${aclUpload}withspecificacl`,
            '-v',
        ], httpCode => {
            assert.strictEqual(httpCode, '200 OK');
            done();
        });
    });

    it("should get an object's specific ACL", done => {
        provideRawOutput([
            '--',
            `${bucketPath}/${aclUpload}withspecificacl?acl`,
            '-v',
        ], (httpCode, rawOutput) => {
            assert.strictEqual(httpCode, '200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                assert.strictEqual(result.AccessControlPolicy
                    .Owner[0].ID[0], ownerCanonicalId);
                assert.strictEqual(result.AccessControlPolicy
                    .AccessControlList[0].Grant[0]
                    .Grantee[0].URI[0],
                    'http://acs.amazonaws.com/groups/global/' +
                    'AuthenticatedUsers');
                assert.strictEqual(result.AccessControlPolicy
                    .AccessControlList[0].Grant[0]
                    .Permission[0], 'READ');
                done();
            });
        });
    });

    it('should return a NoSuchKey error if try to get an object' +
        'ACL for an object that does not exist', done => {
        provideRawOutput(
            ['--', `${bucketPath}/keydoesnotexist?acl`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '404 NOT FOUND');
                assertError(rawOutput.stdout, 'NoSuchKey', done);
            });
    });
});

describe('s3curl multipart upload', () => {
    const key = 'multipart';
    const upload = 'smallUpload';
    let uploadId = null;

    before(done => {
        provideRawOutput(
            ['--createBucket', '--', bucketPath, '-v'],
            httpCode => {
                assert.strictEqual(httpCode, '200 OK');
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

    after(done => {
        deleteRemoteItems([
            `${bucketPath}/copyme`,
            `${bucketPath}/${key}?uploadId=${uploadId}`,
            bucketPath,
        ], () => deleteFile(upload, done));
    });

    it('should list parts of multipart upload with no parts', done => {
        provideRawOutput([
            '--',
            `${bucketPath}/${key}?uploadId=${uploadId}`,
            '-v',
        ], (httpCode, rawOutput) => {
            assert.strictEqual(httpCode, '200 OK');
            parseString(rawOutput.stdout, (err, result) => {
                assert.strictEqual(result.ListPartsResult.UploadId[0],
                                   uploadId);
                assert.strictEqual(result.ListPartsResult.Bucket[0],
                                   bucket);
                assert.strictEqual(result.ListPartsResult.Key[0], key);
                assert.strictEqual(result.ListPartsResult.Part,
                                   undefined);
                done();
            });
        });
    });

    it('should copy a part and return lastModified as ISO', done => {
        provideRawOutput(
            ['--', `${bucketPath}/${key}?uploadId=${uploadId}&partNumber=1`,
            '-X', 'PUT', '-H',
            `x-amz-copy-source:${bucket}/copyme`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    const lastModified = result.CopyPartResult
                        .LastModified[0];
                    const isoDateString = new Date(lastModified).toISOString();
                    assert.strictEqual(lastModified, isoDateString);
                    done();
                });
            });
    });
});

describe('s3curl copy object', () => {
    before(done => {
        createFile(upload, 1048576, () => {
            provideRawOutput(
                ['--createBucket', '--', bucketPath, '-v'],
                httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    putObjects(upload, [`${bucketPath}/copyme`], done);
                });
        });
    });

    after(done => {
        deleteRemoteItems([
            `${bucketPath}/copyme`,
            `${bucketPath}/iamacopy`,
            bucketPath,
        ], () => deleteFile(upload, done));
    });

    it('should copy an object and return lastModified as ISO', done => {
        provideRawOutput(
            ['--', `${bucketPath}/iamacopy`, '-X', 'PUT', '-H',
            `x-amz-copy-source:${bucket}/copyme`, '-v'],
            (httpCode, rawOutput) => {
                assert.strictEqual(httpCode, '200 OK');
                parseString(rawOutput.stdout, (err, result) => {
                    const lastModified = result.CopyObjectResult
                        .LastModified[0];
                    const isoDateString = new Date(lastModified).toISOString();
                    assert.strictEqual(lastModified, isoDateString);
                    done();
                });
            });
    });
});
