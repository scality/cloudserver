const assert = require('assert');
const async = require('async');

const MongoClient = require('mongodb').MongoClient;
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;
const { errors, versioning } = require('arsenal');

const genVID = versioning.VersionID.generateVersionId;
const { VersionId, DbPrefixes } = versioning.VersioningConstants;
const VID_SEP = VersionId.Separator;

const log = require('../utils/fakeLogger');

const replicaSetHosts = 'localhost:27017,localhost:27018,localhost:27019';
const writeConcern = 'majority';
const replicaSet = 'rs0';
const readPreference = 'primary';
const mongoUrl = `mongodb://${replicaSetHosts}/?w=${writeConcern}&` +
    `replicaSet=${replicaSet}&readPreference=${readPreference}`;
const replicationGroupId = 'RG001';
const TEST_DB = 'test';
const BUCKET_NAME = 'test-bucket';

const mongoClientInterface = new MongoClientInterface({
    replicaSetHosts,
    writeConcern,
    replicaSet,
    readPreference,
    replicationGroupId,
    database: TEST_DB,
    logger: log,
});

const runIfMongo =
    process.env.S3METADATA === 'mongodb' ? describe : describe.skip;

function unescape(obj) {
    return JSON.parse(JSON.stringify(obj).
                      replace(/\uFF04/g, '$').
                      replace(/\uFF0E/g, '.'));
}

let uidCounter = 0;
function generateVersionId() {
    return genVID(`${process.pid}.${uidCounter++}`,
                    replicationGroupId);
}

function formatMasterKeyV0(key) {
    return `${key}`;
}

function formatMasterKeyV1(key) {
    return `${DbPrefixes.Master}${key}`;
}

function formatVersionKeyV0(key, versionId) {
    return `${key}${VID_SEP}${versionId}`;
}

function formatVersionKeyV1(key, versionId) {
    return `${DbPrefixes.Version}${formatVersionKeyV0(key, versionId)}`;
}

function formatMasterKey(key, vFormat) {
    if (vFormat === 'v0') {
        return formatMasterKeyV0(key);
    } else {
        return formatMasterKeyV1(key);
    }
}

function formatVersionKey(key, versionId, vFormat) {
    if (vFormat === 'v0') {
        return formatVersionKeyV0(key, versionId);
    } else {
        return formatVersionKeyV1(key, versionId);
    }
}

runIfMongo('MongoClientInterface::deleteObject', () => {
    let mongoClient;
    let collection;

    function getObject(objName, versionId, vFormat, cb) {
        let key = null;
        if (versionId) {
            key = formatVersionKey(objName, versionId, vFormat);
        } else {
            key = formatMasterKey(objName, vFormat);
        }
        collection.findOne({
            _id: key,
        }, {}, (err, doc) => {
            if (err) {
                return cb(err);
            }
            if (!doc) {
                return cb(errors.NoSuchKey);
            }
            if (doc.value.tags) {
                // eslint-disable-next-line
                doc.value.tags = unescape(doc.value.tags);
            }
            return cb(null, doc.value);
        });
    }

    function putNonVersionedObject(objName, objVal, vFormat, cb) {
        const key = formatMasterKey(objName, vFormat);
        collection.update({
            _id: key,
        }, {
            _id: objName,
            value: objVal,
        }, {
            upsert: true,
        }, err => {
            if (err) {
                return cb(err);
            }
            return cb(null);
        });
    }

    function putVersionedObject(objName, versionId, objVal, vFormat, cb) {
        const mKey = formatMasterKey(objName, vFormat);
        const vKey = formatVersionKey(objName, versionId, vFormat);
        collection.bulkWrite([{
            updateOne: {
                filter: {
                    _id: vKey,
                },
                update: {
                    $set: { _id: vKey, value: objVal },
                },
                upsert: true,
            },
        }, {
            updateOne: {
                filter: {
                    _id: mKey,
                    $or: [{
                        'value.versionId': {
                            $exists: false,
                        },
                    },
                    {
                        'value.versionId': {
                            $gt: versionId,
                        },
                    },
                         ],
                },
                update: {
                    $set: { _id: objName, value: objVal },
                },
                upsert: true,
            },
        }], {
            ordered: true,
        }, err => {
            if (err) {
                /*
                * Related to https://jira.mongodb.org/browse/SERVER-14322
                * It happens when we are pushing two versions "at the same time"
                * and the master one does not exist. In MongoDB, two threads are
                * trying to create the same key, the master version, and one of
                * them, the one with the highest versionID (less recent one),
                * fails.
                * We check here than than the MongoDB error is related to the
                * second operation, the master version update and than the error
                * code is the one related to mentionned issue.
                */
                if (err.code === 11000) {
                    return cb(null);
                } else {
                    return cb(err);
                }
            }
            return cb(null);
        });
    }

    function deleteObject(bucketName, objName, versionId, cb) {
        return mongoClientInterface.deleteObject(
            bucketName, objName, { versionId }, log, err => {
                if (err) {
                    return cb(err);
                }
                return cb(null);
            });
    }

    function getObjectCount(cb) {
        collection.count((err, count) => {
            if (err) {
                cb(err);
            }
            cb(null, count);
        });
    }

    before(done => {
        async.waterfall([
            next => mongoClientInterface.setup(next),
            next => MongoClient.connect(mongoUrl, {}, (err, client) => {
                    if (err) {
                        return next(err);
                    }
                    mongoClient = client;
                    return next();
                }),
        ], done);
    });

    beforeEach(done => {
        const db = mongoClient.db(TEST_DB);
        return db.createCollection(BUCKET_NAME, (err, result) => {
            if (err) {
                return done(err);
            }
            collection = result;
            return done();
        });
    });

    after(done => {
        const db = mongoClient.db(TEST_DB);
        async.waterfall([
            next => mongoClientInterface.close(err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            next => db.dropDatabase(err => {
                if (err) {
                    return next(err);
                }
                return next(null);
            }),
            next => mongoClient.close(true, next),
        ], done);
    });

    afterEach(done => collection.drop(err => {
            if (err) {
                return done(err);
            }
            return done();
        })
    );

    it('Should delete non versioned object', done => {
        const bucketName = BUCKET_NAME;
        const params = {
            objName: 'non-deleted-object',
            objVal: {
                key: 'non-deleted-object',
                versionId: 'null',
            },
            vFormat: 'v0',
        };

        return async.waterfall([
            next => {
            // we put the master version of object
            putNonVersionedObject(params.objName, params.objVal, params.vFormat, err => {
                assert.deepStrictEqual(err, null);
                return next();
            });
            },
            next => {
            // we put the master version of a second object
            params.objName = 'object-to-deleted';
            params.objVal.key = 'object-to-deleted';
            putNonVersionedObject(params.objName, params.objVal, params.vFormat, err => {
                assert.deepStrictEqual(err, null);
                return next();
            });
            },
            next => {
            // We delete the first object
            deleteObject(bucketName, params.objName, null, err => {
                assert.deepStrictEqual(err, null);
                return next();
            });
            },
            next => {
            // Object must be removed
            getObject(params.objName, null, params.vFormat, err => {
                assert.deepStrictEqual(err, errors.NoSuchKey);
                return next();
            });
            },
            next => {
            // only 1 object remaining in db
            getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 1);
                return next();
            });
            },
        ], done);
    });

    it('Should not throw error when object non existent', done => {
        const objName = 'non-existent-object';
        const bucketName = BUCKET_NAME;
        return deleteObject(bucketName, objName, null, err => {
            assert.deepStrictEqual(err, null);
            return done();
        });
    });

    it('Should not throw error when bucket non existent', done => {
        const objName = 'non-existent-object';
        const bucketName = BUCKET_NAME;
        return deleteObject(bucketName, objName, null, err => {
            assert.deepStrictEqual(err, null);
            return done();
        });
    });

    it('Master should not be updated when non lastest version is deleted', done => {
        const bucketName = BUCKET_NAME;
        let versionId1 = null;
        let versionId2 = null;

        const params = {
            objName: 'test-object',
            objVal: {
                key: 'test-object',
                versionId: 'null',
            },
            vFormat: 'v0',
        };

        return async.waterfall([
            next => {
            // we start by creating a new version and master
            versionId1 = generateVersionId(this.replicationGroupId);
            params.versionId = versionId1;
            params.objVal.versionId = versionId1;
            putVersionedObject(params.objName, params.versionId, params.objVal,
                params.vFormat, err => {
                assert.deepStrictEqual(err, null);
                return next();
            });
            },
            next => {
            // we create a second version of the same object (master is updated)
            versionId2 = generateVersionId(this.replicationGroupId);
            params.versionId = versionId2;
            params.objVal.versionId = versionId2;
            putVersionedObject(params.objName, params.versionId, params.objVal,
                params.vFormat, err => {
                assert.deepStrictEqual(err, null);
                return next();
            });
            },
            next => {
            // we delete the first version
            params.versionId = versionId1;
            deleteObject(bucketName, params.objName, params.versionId, err => {
                assert.deepStrictEqual(err, null);
                return next();
            });
            },
            next => {
            // the first version should no longer be available
            params.versionId = versionId1;
            getObject(params.objName, params.versionId, params.vFormat, err => {
                assert.deepStrictEqual(err, errors.NoSuchKey);
                return next();
            });
            },
            next => {
            // master must be containing second version metadata
            getObject(params.objName, null, params.vFormat, (err, data) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(data.versionId, versionId2);
                return next();
            });
            },
            next => {
            // master and one version remaining in db
            getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 2);
                return next();
            });
            },
        ], done);
    });

    it('Master should be updated when last version is deleted', done => {
        const bucketName = BUCKET_NAME;
        let versionId1 = null;
        let versionId2 = null;

        const params = {
            objName: 'test-object',
            objVal: {
                key: 'test-object',
                versionId: 'null',
            },
            vFormat: 'v0',
        };

        return async.waterfall([
            next => {
                // creating a new version and master
                versionId1 = generateVersionId(this.replicationGroupId);
                params.versionId = versionId1;
                params.objVal.versionId = versionId1;
                putVersionedObject(params.objName, params.versionId, params.objVal,
                    params.vFormat, err => {
                    assert.deepStrictEqual(err, null);
                    return next();
                });
            },
            next => {
                // Adding a second version and updating master with it's data
                versionId2 = generateVersionId(this.replicationGroupId);
                params.versionId = versionId2;
                params.objVal.versionId = versionId2;
                putVersionedObject(params.objName, params.versionId, params.objVal,
                    params.vFormat, err => {
                    assert.deepStrictEqual(err, null);
                    return next();
                });
            },
            next => {
                // deleting latest version
                params.versionId = versionId2;
                deleteObject(bucketName, params.objName, params.versionId, err => {
                    assert.deepStrictEqual(err, null);
                    return next();
                });
            },
            next => {
                // latest version must be removed
                params.versionId = versionId2;
                getObject(params.objName, params.versionId, params.vFormat, err => {
                    assert.deepStrictEqual(err, errors.NoSuchKey);
                    return next();
                });
            },
            next => {
                // master must be updated to contain first version data
                getObject(params.objName, null, params.vFormat, (err, data) => {
                    assert.deepStrictEqual(err, null);
                    assert(data.isPHD);
                    return next();
                });
            },
            next => {
                // one master and version in the db
                getObjectCount((err, count) => {
                    assert.deepStrictEqual(err, null);
                    assert.strictEqual(count, 2);
                    return next();
                });
            },
        ], done);
    });

    it('Should fail when version id non existent', done => {
        const versionId = generateVersionId(this.replicationGroupId);
        const objName = 'test-object';
        const bucketName = BUCKET_NAME;

        return deleteObject(bucketName, objName, versionId, err => {
            assert.deepStrictEqual(err, errors.NoSuchKey);
            return done();
        });
    });
});

