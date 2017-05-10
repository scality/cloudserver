const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    versioningSuspended,
    versioningEnabled,
    removeAllVersions,
} = require('../../lib/utility/versioning-util.js');

const bucket = `versioning-bucket-${Date.now()}`;
const key = 'anObject';
// formats differ for AWS and S3, use respective sample ids to obtain
// correct error response in tests
const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

function _assertNoError(err, desc) {
    assert.strictEqual(err, null, `Unexpected err ${desc || ''}: ${err}`);
}

describe('delete marker creation in bucket with null version', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const nullVersionBody = 'nullversionbody';

        beforeEach(done => {
            s3.createBucket({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                } // put null object
                return s3.putObject({
                    Bucket: bucket,
                    Key: key,
                    Body: nullVersionBody,
                }, done);
            });
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, err => {
                    assert.strictEqual(err, null,
                        `Error deleting bucket: ${err}`);
                    return done();
                });
            });
        });

        it('should keep the null version if versioning enabled', done => {
            async.waterfall([
                callback => s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }, err => callback(err)),
                callback =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        assert.strictEqual(data.Versions.length, 1);
                        assert.strictEqual(data.Versions[0].VersionId,
                            'null');
                        return callback();
                    }),
                callback => s3.deleteObject({ Bucket: bucket, Key: key },
                    (err, data) => {
                        _assertNoError(err, 'creating delete marker');
                        assert.strictEqual(data.DeleteMarker, 'true');
                        assert(data.VersionId);
                        return callback(null, data.VersionId);
                    }),
                (deleteMarkerVerId, callback) =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        assert.strictEqual(data.Versions.length, 1);
                        assert.strictEqual(data.Versions[0].VersionId,
                            'null');
                        assert.strictEqual(data.DeleteMarkers[0].VersionId,
                            deleteMarkerVerId);
                        return callback();
                    }),
            ], done);
        });

        it('delete marker overwrites null version if versioning suspended',
        done => {
            async.waterfall([
                callback => s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }, err => callback(err)),
                callback =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        assert.strictEqual(data.Versions.length, 1);
                        assert.strictEqual(data.Versions[0].VersionId,
                            'null');
                        return callback();
                    }),
                callback => s3.deleteObject({ Bucket: bucket, Key: key },
                    (err, data) => {
                        _assertNoError(err, 'creating delete marker');
                        assert.strictEqual(data.DeleteMarker, 'true');
                        assert.strictEqual(data.VersionId, 'null');
                        return callback(null, data.VersionId);
                    }),
                (deleteMarkerVerId, callback) =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        assert.strictEqual(data.Versions.length, 0);
                        assert.strictEqual(data.DeleteMarkers[0].VersionId,
                            deleteMarkerVerId);
                        return callback();
                    }),
            ], done);
        });
    });
});

describe('aws-node-sdk test delete object', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let versionIds = undefined;

        // setup test
        before(done => {
            versionIds = [];
            s3.createBucket({ Bucket: bucket }, done);
        });

        // delete bucket after testing
        after(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err.code === 'NoSuchBucket') {
                    return done();
                } else if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, err => {
                    assert.strictEqual(err, null,
                        `Error deleting bucket: ${err}`);
                    return done();
                });
            });
        });

        it('delete non existent object should not create a delete marker',
        done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: `${key}000`,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.DeleteMarker, undefined);
                assert.strictEqual(res.VersionId, undefined);
                return done();
            });
        });

        it('creating non-versioned object', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.equal(res.VersionId, undefined);
                return done();
            });
        });

        it('delete in non-versioned bucket should not create delete marker',
        done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.equal(res.VersionId, undefined);
                return s3.deleteObject({
                    Bucket: bucket,
                    Key: `${key}2`,
                }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.DeleteMarker, undefined);
                    assert.strictEqual(res.VersionId, undefined);
                    return done();
                });
            });
        });

        it('enable versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        it('should not send back error for non-existing key (specific version)',
            done => {
                s3.deleteObject({
                    Bucket: bucket,
                    Key: `${key}3`,
                    VersionId: 'null',
                }, err => {
                    if (err) {
                        return done(err);
                    }
                    return done();
                });
            });

        it('delete non existent object should create a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: `${key}2`,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.DeleteMarker, 'true');
                assert.notEqual(res.VersionId, undefined);
                return s3.deleteObject({
                    Bucket: bucket,
                    Key: `${key}2`,
                }, (err, res2) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res2.DeleteMarker, 'true');
                    assert.notEqual(res2.VersionId, res.VersionId);
                    return s3.deleteObject({
                        Bucket: bucket,
                        Key: `${key}2`,
                        VersionId: res.VersionId,
                    }, err => {
                        if (err) {
                            return done(err);
                        }
                        return s3.deleteObject({
                            Bucket: bucket,
                            Key: `${key}2`,
                            VersionId: res2.VersionId,
                        }, err => done(err));
                    });
                });
            });
        });

        it('delete non existent version should not create delete marker',
        done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: nonExistingId,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.VersionId, nonExistingId);
                return s3.listObjectVersions({ Bucket: bucket }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.DeleteMarkers.length, 0);
                    return done();
                });
            });
        });

        it('put a version to the object', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: 'test',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                versionIds.push('null');
                versionIds.push(res.VersionId);
                assert.notEqual(res.VersionId, undefined);
                return done();
            });
        });

        it('should create a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.DeleteMarker, 'true');
                assert.strictEqual(
                    versionIds.find(item => item === res.VersionId),
                    undefined);
                versionIds.push(res.VersionId);
                return done();
            });
        });

        it('should return 404 with a delete marker', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
            }, function test(err) {
                if (!err) {
                    return done(new Error('should return 404'));
                }
                const headers = this.httpResponse.headers;
                assert.strictEqual(headers['x-amz-delete-marker'], 'true');
                return done();
            });
        });

        it('should delete the null version', done => {
            const version = versionIds.shift();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.VersionId, version);
                assert.equal(res.DeleteMarker, undefined);
                return done();
            });
        });

        it('should delete the versionned object', done => {
            const version = versionIds.shift();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.VersionId, version);
                assert.equal(res.DeleteMarker, undefined);
                return done();
            });
        });

        it('should delete the delete-marker version', done => {
            const version = versionIds.shift();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.VersionId, version);
                assert.equal(res.DeleteMarker, 'true');
                return done();
            });
        });

        it('put a new version', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: 'test',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                versionIds.push(res.VersionId);
                assert.notEqual(res.VersionId, undefined);
                return done();
            });
        });

        it('get the null version', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }, err => {
                if (!err || err.code !== 'NoSuchVersion') {
                    return done(err || 'should send back an error');
                }
                return done();
            });
        });

        it('suspending versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        it('delete non existent object should create a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: `${key}2`,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.DeleteMarker, 'true');
                assert.notEqual(res.VersionId, undefined);
                return s3.deleteObject({
                    Bucket: bucket,
                    Key: `${key}2`,
                }, (err, res2) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res2.DeleteMarker, 'true');
                    assert.strictEqual(res2.VersionId, res.VersionId);
                    return s3.deleteObject({
                        Bucket: bucket,
                        Key: `${key}2`,
                        VersionId: res.VersionId,
                    }, err => done(err));
                });
            });
        });

        it('should put a new delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.DeleteMarker, 'true');
                assert.strictEqual(res.VersionId, 'null');
                return done();
            });
        });

        it('enabling versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        it('should get the null version', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }, function test(err) {
                const headers = this.httpResponse.headers;
                assert.strictEqual(headers['x-amz-delete-marker'], 'true');
                assert.strictEqual(headers['x-amz-version-id'], 'null');
                if (err && err.code !== 'MethodNotAllowed') {
                    return done(err);
                } else if (err) {
                    return done();
                }
                return done('should return an error');
            });
        });

        it('put a new version to store the null version', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: 'test',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                versionIds.push(res.VersionId);
                return done();
            });
        });

        it('suspending versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        it('put null version', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: 'test-null-version',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.VersionId, undefined);
                return done();
            });
        });

        it('enabling versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        it('should get the null version', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.Body.toString(), 'test-null-version');
                return done();
            });
        });

        it('should add a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.DeleteMarker, 'true');
                versionIds.push(res.VersionId);
                return done();
            });
        });

        it('should get the null version', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.Body.toString(), 'test-null-version');
                return done();
            });
        });

        it('should add a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.DeleteMarker, 'true');
                assert.strictEqual(
                    versionIds.find(item => item === res.VersionId),
                    undefined);
                versionIds.push(res.VersionId);
                return done();
            });
        });

        it('should set the null version as master', done => {
            let version = versionIds.pop();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.VersionId, version);
                assert.strictEqual(res.DeleteMarker, 'true');
                version = versionIds.pop();
                return s3.deleteObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: version,
                }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.VersionId, version);
                    assert.strictEqual(res.DeleteMarker, 'true');
                    return s3.getObject({
                        Bucket: bucket,
                        Key: key,
                    }, (err, res) => {
                        if (err) {
                            return done(err);
                        }
                        assert.strictEqual(res.Body.toString(),
                            'test-null-version');
                        return done();
                    });
                });
            });
        });

        it('should delete null version', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res.VersionId, 'null');
                return s3.getObject({
                    Bucket: bucket,
                    Key: key,
                }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.VersionId,
                        versionIds[versionIds.length - 1]);
                    return done();
                });
            });
        });

        it('should be able to delete the bucket', done => {
            async.eachSeries(versionIds, (id, next) => {
                s3.deleteObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: id,
                }, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    assert.strictEqual(res.VersionId, id);
                    return next();
                });
            }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, err => done(err));
            });
        });
    });
});
