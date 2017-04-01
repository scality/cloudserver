import assert from 'assert';
import { S3 } from 'aws-sdk';
import async from 'async';

import getConfig from '../support/config';

const bucket = `versioning-bucket-${Date.now()}`;
const key = 'anObject';

const testing = process.env.VERSIONING === 'no' ? describe.skip : describe;

testing('aws-node-sdk test delete object', function testSuite() {
    this.timeout(600000);
    let s3 = undefined;
    let versionIds = undefined;

    function _deleteVersionList(versionList, bucket, callback) {
        async.each(versionList, (versionInfo, cb) => {
            const versionId = versionInfo.VersionId;
            const params = { Bucket: bucket, Key: versionInfo.Key,
            VersionId: versionId };
            s3.deleteObject(params, cb);
        }, callback);
    }
    function _removeAllVersions(bucket, callback) {
        return s3.listObjectVersions({ Bucket: bucket }, (err, data) => {
            process.stdout.write(
                'list object versions before deletion' +
                `${JSON.stringify(data, undefined, '\t')}`);
            if (err && err.NoSuchBucket) {
                return callback();
            } else if (err) {
                return callback(err);
            }
            return _deleteVersionList(data.DeleteMarkers, bucket, err => {
                if (err) {
                    return callback(err);
                }
                return _deleteVersionList(data.Versions, bucket, callback);
            });
        });
    }

    // setup test
    before(done => {
        versionIds = [];
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        s3.createBucket({ Bucket: bucket }, done);
    });

    // delete bucket after testing
    after(done => {
        // TODO: remove conditional after listing is implemented
        if (process.env.AWS_ON_AIR === 'true') {
            return _removeAllVersions(bucket, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, err => {
                    assert.strictEqual(err, null,
                        `Error deleting bucket: ${err}`);
                    return done();
                });
            });
        }
        return done();
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

    it('creating non-versionned object', done => {
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

    it('delete object in non-versioned bucket should not create delete marker',
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
            assert.strictEqual(versionIds.find(item => item === res.VersionId),
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
            assert.strictEqual(versionIds.find(item => item === res.VersionId),
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
