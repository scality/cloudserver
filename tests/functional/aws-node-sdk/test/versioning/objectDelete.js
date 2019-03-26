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
    expect(err).toBe(null);
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
                    expect(err).toBe(null);
                    return done();
                });
            });
        });

        test('should keep the null version if versioning enabled', done => {
            async.waterfall([
                callback => s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }, err => callback(err)),
                callback =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        expect(data.Versions.length).toBe(1);
                        expect(data.Versions[0].VersionId).toBe('null');
                        return callback();
                    }),
                callback => s3.deleteObject({ Bucket: bucket, Key: key },
                    (err, data) => {
                        _assertNoError(err, 'creating delete marker');
                        expect(data.DeleteMarker).toBe('true');
                        expect(data.VersionId).toBeTruthy();
                        return callback(null, data.VersionId);
                    }),
                (deleteMarkerVerId, callback) =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        expect(data.Versions.length).toBe(1);
                        expect(data.Versions[0].VersionId).toBe('null');
                        expect(data.DeleteMarkers[0].VersionId).toBe(deleteMarkerVerId);
                        return callback();
                    }),
            ], done);
        });

        test('delete marker overwrites null version if versioning suspended', done => {
            async.waterfall([
                callback => s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }, err => callback(err)),
                callback =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        expect(data.Versions.length).toBe(1);
                        expect(data.Versions[0].VersionId).toBe('null');
                        return callback();
                    }),
                callback => s3.deleteObject({ Bucket: bucket, Key: key },
                    (err, data) => {
                        _assertNoError(err, 'creating delete marker');
                        expect(data.DeleteMarker).toBe('true');
                        expect(data.VersionId).toBe('null');
                        return callback(null, data.VersionId);
                    }),
                (deleteMarkerVerId, callback) =>
                    s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
                        _assertNoError(err, 'listing object versions');
                        expect(data.Versions.length).toBe(0);
                        expect(data.DeleteMarkers[0].VersionId).toBe(deleteMarkerVerId);
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
        let versionIds;

        // setup test
        beforeAll(done => {
            versionIds = [];
            s3.createBucket({ Bucket: bucket }, done);
        });

        // delete bucket after testing
        afterAll(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err.code === 'NoSuchBucket') {
                    return done();
                } else if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, err => {
                    expect(err).toBe(null);
                    return done();
                });
            });
        });

        test('delete non existent object should not create a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: `${key}000`,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.DeleteMarker).toBe(undefined);
                expect(res.VersionId).toBe(undefined);
                return done();
            });
        });

        test('creating non-versioned object', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toEqual(undefined);
                return done();
            });
        });

        test(
            'delete in non-versioned bucket should not create delete marker',
            done => {
                s3.putObject({
                    Bucket: bucket,
                    Key: key,
                }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res.VersionId).toEqual(undefined);
                    return s3.deleteObject({
                        Bucket: bucket,
                        Key: `${key}2`,
                    }, (err, res) => {
                        if (err) {
                            return done(err);
                        }
                        expect(res.DeleteMarker).toBe(undefined);
                        expect(res.VersionId).toBe(undefined);
                        return done();
                    });
                });
            }
        );

        test('enable versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        test(
            'should not send back error for non-existing key (specific version)',
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
            }
        );

        test('delete non existent object should create a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: `${key}2`,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.DeleteMarker).toBe('true');
                expect(res.VersionId).not.toEqual(undefined);
                return s3.deleteObject({
                    Bucket: bucket,
                    Key: `${key}2`,
                }, (err, res2) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res2.DeleteMarker).toBe('true');
                    expect(res2.VersionId).not.toEqual(res.VersionId);
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

        test('delete non existent version should not create delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: nonExistingId,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toBe(nonExistingId);
                return s3.listObjectVersions({ Bucket: bucket }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res.DeleteMarkers.length).toBe(0);
                    return done();
                });
            });
        });

        test('put a version to the object', done => {
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
                expect(res.VersionId).not.toEqual(undefined);
                return done();
            });
        });

        test('should create a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.DeleteMarker).toBe('true');
                expect(versionIds.find(item => item === res.VersionId)).toBe(undefined);
                versionIds.push(res.VersionId);
                return done();
            });
        });

        test('should return 404 with a delete marker', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
            }, function test(err) {
                if (!err) {
                    return done(new Error('should return 404'));
                }
                const headers = this.httpResponse.headers;
                expect(headers['x-amz-delete-marker']).toBe('true');
                return done();
            });
        });

        test('should delete the null version', done => {
            const version = versionIds.shift();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toBe(version);
                expect(res.DeleteMarker).toEqual(undefined);
                return done();
            });
        });

        test('should delete the versionned object', done => {
            const version = versionIds.shift();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toBe(version);
                expect(res.DeleteMarker).toEqual(undefined);
                return done();
            });
        });

        test('should delete the delete-marker version', done => {
            const version = versionIds.shift();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toBe(version);
                expect(res.DeleteMarker).toEqual('true');
                return done();
            });
        });

        test('put a new version', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: 'test',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                versionIds.push(res.VersionId);
                expect(res.VersionId).not.toEqual(undefined);
                return done();
            });
        });

        test('get the null version', done => {
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

        test('suspending versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        test('delete non existent object should create a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: `${key}2`,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.DeleteMarker).toBe('true');
                expect(res.VersionId).not.toEqual(undefined);
                return s3.deleteObject({
                    Bucket: bucket,
                    Key: `${key}2`,
                }, (err, res2) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res2.DeleteMarker).toBe('true');
                    expect(res2.VersionId).toBe(res.VersionId);
                    return s3.deleteObject({
                        Bucket: bucket,
                        Key: `${key}2`,
                        VersionId: res.VersionId,
                    }, err => done(err));
                });
            });
        });

        test('should put a new delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.DeleteMarker).toBe('true');
                expect(res.VersionId).toBe('null');
                return done();
            });
        });

        test('enabling versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        test('should get the null version', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }, function test(err) {
                const headers = this.httpResponse.headers;
                expect(headers['x-amz-delete-marker']).toBe('true');
                expect(headers['x-amz-version-id']).toBe('null');
                if (err && err.code !== 'MethodNotAllowed') {
                    return done(err);
                } else if (err) {
                    return done();
                }
                return done('should return an error');
            });
        });

        test('put a new version to store the null version', done => {
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

        test('suspending versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        test('put null version', done => {
            s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: 'test-null-version',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toBe(undefined);
                return done();
            });
        });

        test('enabling versioning', done => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            s3.putBucketVersioning(params, done);
        });

        test('should get the null version', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.Body.toString()).toBe('test-null-version');
                return done();
            });
        });

        test('should add a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.DeleteMarker).toBe('true');
                versionIds.push(res.VersionId);
                return done();
            });
        });

        test('should get the null version', done => {
            s3.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.Body.toString()).toBe('test-null-version');
                return done();
            });
        });

        test('should add a delete marker', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.DeleteMarker).toBe('true');
                expect(versionIds.find(item => item === res.VersionId)).toBe(undefined);
                versionIds.push(res.VersionId);
                return done();
            });
        });

        test('should set the null version as master', done => {
            let version = versionIds.pop();
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toBe(version);
                expect(res.DeleteMarker).toBe('true');
                version = versionIds.pop();
                return s3.deleteObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: version,
                }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res.VersionId).toBe(version);
                    expect(res.DeleteMarker).toBe('true');
                    return s3.getObject({
                        Bucket: bucket,
                        Key: key,
                    }, (err, res) => {
                        if (err) {
                            return done(err);
                        }
                        expect(res.Body.toString()).toBe('test-null-version');
                        return done();
                    });
                });
            });
        });

        test('should delete null version', done => {
            s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res.VersionId).toBe('null');
                return s3.getObject({
                    Bucket: bucket,
                    Key: key,
                }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res.VersionId).toBe(versionIds[versionIds.length - 1]);
                    return done();
                });
            });
        });

        test('should be able to delete the bucket', done => {
            async.eachSeries(versionIds, (id, next) => {
                s3.deleteObject({
                    Bucket: bucket,
                    Key: key,
                    VersionId: id,
                }, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    expect(res.VersionId).toBe(id);
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
