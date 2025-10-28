const assert = require('assert');
const async = require('async');
const {
    ListObjectsCommand,
    ListObjectVersionsCommand,
    DeleteObjectsCommand,
} = require('@aws-sdk/client-s3');

function _deleteVersionList(s3Client, versionList, bucket, callback) {
    if (versionList === undefined || versionList.length === 0) {
        return callback();
    }
    const params = {
        Bucket: bucket,
        Delete: {
            Objects: versionList.map(version => ({
                Key: version.Key,
                VersionId: version.VersionId,
            })),
        },
    };
    return s3Client.send(new DeleteObjectsCommand(params))
        .then(() => callback())
        .catch(callback);
}

const testUtils = {};

testUtils.runIfMongo = process.env.S3METADATA === 'mongodb' ?
    describe : describe.skip;

testUtils.runAndCheckSearch = (s3Client, bucketName, encodedSearch, listVersions,
    testResult, done) => {
    const makeRequest = async () => {
        try {
            const input = { 
                Bucket: bucketName,
            };
            
            let command;
            if (listVersions) {
                command = new ListObjectVersionsCommand(input);
            } else {
                command = new ListObjectsCommand(input);
            }
            
            // Add middleware to inject the search query parameter
            // SDK v3 automatically encodes query parameters, so we decode first to avoid double-encoding
            command.middlewareStack.add(
                next => async args => {
                    if (!args.request.query) {
                        // eslint-disable-next-line no-param-reassign
                        args.request.query = {};
                    }
                    // Decode the already-encoded search string since SDK v3 will encode it again
                    // eslint-disable-next-line no-param-reassign
                    args.request.query.search = decodeURIComponent(encodedSearch);
                    if (listVersions) {
                        // eslint-disable-next-line no-param-reassign
                        args.request.query.versions = '';
                    }
                    
                    return next(args);
                },
                {
                    step: 'build',
                    name: 'addSearchQuery',
                }
            );
            
            const res = await s3Client.send(command);
            
            if (listVersions) {
                if (testResult) {
                    assert.notStrictEqual(res.Versions[0].VersionId, undefined);
                    if (Array.isArray(testResult)) {
                        assert.strictEqual(res.Versions.length, testResult.length);
                        async.forEachOf(testResult, (expected, i, next) => {
                            assert.strictEqual(res.Versions[i].Key, expected);
                            next();
                        }, done);
                        return;
                    } else {
                        assert(res.Versions[0], 'should be Contents listed');
                        assert.strictEqual(res.Versions[0].Key, testResult);
                        assert.strictEqual(res.Versions.length, 1);
                    }
                } else {
                    assert.strictEqual(res.Versions.length, 0);
                }
            } else {
                if (testResult && typeof testResult === 'object' && testResult.code) {
                    // This was expected to be an error, but we got success
                    done(new Error('Expected error but got success'));
                }
                if (testResult) {
                    assert(res.Contents[0], 'should be Contents listed');
                    assert.strictEqual(res.Contents[0].Key, testResult);
                    assert.strictEqual(res.Contents.length, 1);
                } else {
                    assert.strictEqual(res.Contents?.length, undefined);
                }
            }
            done();
        } catch (err) {
            if (testResult && typeof testResult === 'object' && testResult.code) {
                assert.strictEqual(err.name, testResult.code);
                assert.strictEqual(err.message, testResult.message);
                done();
            }
            done(err);
        }
    };
    makeRequest();
};

testUtils.removeAllVersions = (s3Client, bucket, callback) => {
    async.waterfall([
        cb => s3Client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
            .then(data => cb(null, data))
            .catch(cb),
        (data, cb) => _deleteVersionList(s3Client, data.DeleteMarkers, bucket,
            err => cb(err, data)),
        (data, cb) => _deleteVersionList(s3Client, data.Versions, bucket,
            err => cb(err, data)),
        (data, cb) => {
            if (data.IsTruncated) {
                const params = {
                    Bucket: bucket,
                    KeyMarker: data.NextKeyMarker,
                    VersionIdMarker: data.NextVersionIdMarker,
                };
                return testUtils.removeAllVersions(s3Client, params, cb);
            }
            return cb();
        },
    ], callback);
};

module.exports = testUtils;
