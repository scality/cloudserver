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
function provideRawOutput(args, httpCode, cb) {
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
    child.on('error', err => cb(err));
    child.on('close', code => {
        process.stdout.write(`s3curl return code : ${code}\n`);
        let httpCode;
        if (procData.stderr !== '') {
            console.log('procData.stderr', procData.stderr);
            if (procData.stderr.indexOf(`HTTP/1.1 ${httpCode}`)) {
                return cb(null, procData);
            }
            return cb(new Error("Can't find http response code"));
        }
        process.stdout.write(`stdout: ${procData.stdout}`);
        return cb(new Error('Cannot have stderr'));
    });
}

describe('s3curl put and delete buckets', () => {
    it('should put a valid bucket', done => {
        provideRawOutput(['--createBucket', '--',
            `${endpoint}/${bucket}`, '-v'], '200 OK', done);
    });

    it('should not be able to put a bucket with a name ' +
        'already being used', done => {
        provideRawOutput(['--createBucket', '--',
            `${endpoint}/${bucket}`, '-v'], '409 CONFLICT',
            (err, rawOutput) => {
                if (err) {
                    return done(err);
                }
                return assertError(rawOutput.stdout, 'BucketAlreadyOwnedByYou',
                 done);
            });
    });

    it('should not be able to put a bucket with invalid xml' +
        ' in the post body', done => {
        provideRawOutput(['--createBucket', '--',
            '--data', 'malformedxml', `${endpoint}/${bucket}`, '-v'],
            '400 BAD REQUEST', (err, rawOutput) => {
                if (err) {
                    return done(err);
                }
                return assertError(rawOutput.stdout, 'MalformedXML',
                 done);
            });
    });
    it('should not be able to put a bucket with xml that does' +
        ' not conform to s3 docs for locationConstraint', done => {
        provideRawOutput(['--createBucket', '--',
            '--data', '<Hello>a</Hello>', `${endpoint}/${bucket}`, '-v'],
            '400 BAD REQUEST', (err, rawOutput) => {
                if (err) {
                    return done(err);
                }
                return assertError(rawOutput.stdout, 'MalformedXML',
                 done);
            });
    });

    it('should not be able to put a bucket with an invalid name', done => {
        provideRawOutput(['--createBucket', '--',
            `${endpoint}/2`, '-v'], '400 BAD REQUEST', done);
    });

    it('should be able to delete a bucket', done => {
        provideRawOutput(['--delete', '--',
            `${endpoint}/${bucket}`, '-v'], '204 NO CONTENT', done);
    });

    it('should not be able to get a bucket that was deleted', done => {
        provideRawOutput(
            ['--', `${endpoint}/${bucket}`, '-v'], '404 NOT FOUND',
            (err, rawOutput) => {
                if (err) {
                    return done(err);
                }
                return assertError(rawOutput.stdout, 'NoSuchBucket',
                 done);
            });
    });

    it('should be able to create a bucket with a name' +
        'of a bucket that has previously been deleted', done => {
        provideRawOutput(['--createBucket', '--',
            `${endpoint}/${bucket}`, '-v'], '200 OK', done);
    });
});

describe('s3curl put and get bucket ACLs', () => {
    it('should be able to create a bucket with a canned ACL', done => {
        provideRawOutput(['--createBucket', '--', '-H',
        'x-amz-acl:public-read',
        `${endpoint}/${aclBucket}`, '-v'], '200 OK', done);
    });

    it('should be able to get a canned ACL', done => {
        provideRawOutput(['--',
        `${endpoint}/${aclBucket}?acl`, '-v'], '200 OK',
        (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, xml) => {
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
        provideRawOutput(['--createBucket', '--', '-H',
        'x-amz-grant-read:uri=http://acs.amazonaws.com/groups/global/AllUsers',
        `${endpoint}/${aclBucket}2`, '-v'], '200 OK', done);
    });

    it('should be able to get a specifically set ACL', done => {
        provideRawOutput(['--',
        `${endpoint}/${aclBucket}2?acl`, '-v'], '200 OK',
        (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, xml) => {
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
    it('should get a list of all buckets created by user account', done => {
        provideRawOutput(['--', `${endpoint}`, '-v'], '200 OK',
        (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, xml) => {
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
    before('create file to put', done => {
        createFile(upload, 1048576, done);
    });

    // curl behavior is not consistent across the environments
    // skipping the test for now
    it.skip('should not be able to put an object if request does not have ' +
        'content-length header',
        done => {
            provideRawOutput(['--debug', `--put=${upload}`, '--',
                '-H', 'content-length:',
                `${endpoint}/${bucket}/` +
                `${prefix}${delimiter}${upload}1`, '-v'], '411 LENGTH REQUIRED',
                (err, rawOutput) => {
                    if (err) {
                        return done(err);
                    }
                    return assertError(rawOutput.stdout, 'MissingContentLength',
                    done);
                });
        });

    it('should not be able to put an object if content-md5 header is ' +
    'invalid',
        done => {
            provideRawOutput(['--debug', `--put=${upload}`,
                '--contentMd5', 'toto', '--',
                `${endpoint}/${bucket}/` +
                `${prefix}${delimiter}${upload}1`, '-v'], '400 BAD REQUEST',
                (err, rawOutput) => {
                    if (err) {
                        return done(err);
                    }
                    return assertError(rawOutput.stdout, 'InvalidDigest', done);
                });
        });

    it('should not be able to put an object if content-md5 header is ' +
    'mismatched MD5',
        done => {
            provideRawOutput(['--debug', `--put=${upload}`,
                '--contentMd5', 'rL0Y20zC+Fzt72VPzMSk2A==', '--',
                `${endpoint}/${bucket}/` +
                `${prefix}${delimiter}${upload}1`, '-v'], '400 BAD REQUEST',
                (err, rawOutput) => {
                    if (err) {
                        return done(err);
                    }
                    return assertError(rawOutput.stdout, 'BadDigest', done);
                });
        });

    it('should not be able to put an object in a bucket with an invalid name',
        done => {
            provideRawOutput(['--debug', `--put=${upload}`, '--',
                `${endpoint}/2/` +
                `${prefix}${delimiter}${upload}1`, '-v'], '400 BAD REQUEST',
                (err, rawOutput) => {
                    if (err) {
                        return done(err);
                    }
                    return assertError(rawOutput.stdout, 'InvalidBucketName',
                    done);
                });
        });

    it('should not be able to put an object in a bucket that does not exist',
        done => {
            provideRawOutput(['--debug', `--put=${upload}`, '--',
                `${endpoint}/${nonexist}/` +
                `${prefix}${delimiter}${upload}1`, '-v'], '404 NOT FOUND',
                (err, rawOutput) => {
                    if (err) {
                        return done(err);
                    }
                    return assertError(rawOutput.stdout, 'NoSuchBucket', done);
                });
        });

    it('should put first object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput(['--debug', `--put=${upload}`, '--',
            `${endpoint}/${bucket}/` +
            `${prefix}${delimiter}${upload}1`, '-v'], '200 OK', done);
    });

    it('should put second object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput([`--put=${upload}`, '--',
            `${endpoint}/${bucket}/` +
            `${prefix}${delimiter}${upload}2`, '-v'], '200 OK', done);
    });

    it('should put third object in existing bucket with prefix ' +
    'and delimiter', done => {
        provideRawOutput([`--put=${upload}`, '--',
            `${endpoint}/${bucket}/` +
            `${prefix}${delimiter}${upload}3`, '-v'], '200 OK', done);
    });
});

describe('s3curl getBucket', () => {
    it('should list all objects if no prefix or delimiter specified', done => {
        provideRawOutput(['--',
        `${endpoint}/${bucket}`, '-v'], '200 OK',
        (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, result) => {
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
    });

    it('should list a common prefix if a common prefix and delimiter are ' +
    'specified', done => {
        provideRawOutput(['--',
        `${endpoint}/${bucket}?delimiter=${delimiter}` +
        `&prefix=${prefix}`, '-v'], '200 OK', (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                assert.strictEqual(result.ListBucketResult
                    .CommonPrefixes[0].Prefix[0], 'topLevel/');
                done();
            });
        });
    });

    it('should not list a common prefix if no delimiter is specified', done => {
        provideRawOutput(['--',
        `${endpoint}/${bucket}?` +
        `&prefix=${prefix}`, '-v'], '200 OK', (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, result) => {
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
    });

    it('should provide a next marker if maxs keys exceeded ' +
        'and delimiter specified', done => {
        provideRawOutput(['--',
        `${endpoint}/${bucket}?` +
        'delimiter=x&max-keys=2', '-v'], '200 OK', (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                assert.strictEqual(result.ListBucketResult
                    .NextMarker[0], 'topLevel/test1MB2');
                assert.strictEqual(result.ListBucketResult
                    .IsTruncated[0], 'true');
                done();
            });
        });
    });
});

describe('s3curl head bucket', () => {
    it('should return a 404 response if bucket does not exist', done => {
        provideRawOutput(['--head', '--',
        `${endpoint}/${nonexist}`, '-v'], '404 NOT FOUND', done);
    });

    it('should return a 200 response if bucket exists' +
        ' and user is authorized', done => {
        provideRawOutput(['--head', '--',
        `${endpoint}/${bucket}`, '-v'], '200 OK', done);
    });
});

describe('s3curl getObject', () => {
    after('delete created file and downloaded file', done => {
        deleteFile(upload, () => {
            deleteFile(download, done);
        });
    });

    it('should put object with metadata', done => {
        provideRawOutput([`--put=${upload}`, '--',
            '-H', 'x-amz-meta-mine:BestestObjectEver',
            `${endpoint}/${bucket}/getter`, '-v'], '200 OK', done);
    });

    it('should get an existing file in an existing bucket', done => {
        provideRawOutput(['--', '-o', download,
            `${endpoint}/${bucket}/getter`, '-v'], '200 OK', done);
    });

    it('downloaded file should equal uploaded file', done => {
        diff(upload, download, done);
    });
});

describe('s3curl head object', () => {
    it("should get object's metadata", done => {
        provideRawOutput(['--head', '--',
            `${endpoint}/${bucket}/getter`, '-v'], '200 OK',
            (err, rawOutput) => {
                if (err) {
                    return done(err);
                }
                const lines = rawOutput.stdout.split('\n');
                const userMetadata = 'x-amz-meta-mine: BestestObjectEver\r';
                assert(lines.indexOf(userMetadata) > -1);
                assert(rawOutput.stdout.indexOf('ETag') > -1);
                return done();
            });
    });
});

describe('s3curl object ACLs', () => {
    before('create file to put', done => {
        createFile(aclUpload, 512000, done);
    });
    after('delete created file', done => {
        deleteFile(aclUpload, done);
    });

    it('should put an object with a canned ACL', done => {
        provideRawOutput([`--put=${aclUpload}`, '--',
            '-H', 'x-amz-acl:public-read',
            `${endpoint}/${bucket}/` +
            `${aclUpload}withcannedacl`, '-v'], '200 OK', done);
    });

    it("should get an object's canned ACL", done => {
        provideRawOutput(['--',
        `${endpoint}/${bucket}/` +
        `${aclUpload}withcannedacl?acl`, '-v'], '200 OK', (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, result) => {
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
        provideRawOutput([`--put=${aclUpload}`, '--',
            '-H', 'x-amz-grant-read:uri=' +
            'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
            `${endpoint}/${bucket}/` +
            `${aclUpload}withspecificacl`, '-v'], '200 OK', done);
    });

    it("should get an object's specific ACL", done => {
        provideRawOutput(['--',
        `${endpoint}/${bucket}/` +
        `${aclUpload}withspecificacl?acl`, '-v'], '200 OK',
        (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, result) => {
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
        provideRawOutput(['--',
        `${endpoint}/${bucket}/` +
        'keydoesnotexist?acl', '-v'], '404 NOT FOUND',
        (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return assertError(rawOutput.stdout, 'NoSuchKey', done);
        });
    });
});

describe('s3curl multipart upload', () => {
    it('should list parts of multipart upload with no parts', done => {
        const key = 'multipart';
        provideRawOutput([
            '--',
            '-X',
            'POST',
            `${endpoint}/${bucket}/${key}?uploads`,
            '-v',
        ], '200 OK', (err, rawOutput) => {
            if (err) {
                return done(err);
            }
            return parseString(rawOutput.stdout, (err, result) => {
                if (err) {
                    assert.ifError(err);
                }
                const uploadId =
                    result.InitiateMultipartUploadResult.UploadId[0];
                provideRawOutput([
                    '--',
                    `${endpoint}/${bucket}/${key}?` +
                    `uploadId=${uploadId}`,
                    '-v',
                ], '200 OK', (err, rawOutput) => {
                    if (err) {
                        return done(err);
                    }
                    return parseString(rawOutput.stdout, (err, result) => {
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
        });
    });
});
