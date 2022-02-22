const assert = require('assert');
const async = require('async');

const MongoClient = require('mongodb').MongoClient;
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;
const { versioning } = require('arsenal');

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

runIfMongo('MongoClientInterface::listObjects', () => {
    let mongoClient;
    let collection;

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

    function putBulkObjectVersions(objName, objVal, vFormat, versionNb, cb) {
        let count = 0;
        async.whilst(
            () => count < versionNb,
            cbIterator => {
                count++;
                const versionId = generateVersionId();
                // eslint-disable-next-line
                objVal.versionId = versionId;
                return putVersionedObject(objName, versionId, objVal, vFormat,
                                        cbIterator);
            },
            err => {
                if (err) {
                    return cb(err);
                }
                return cb(null);
            },
        );
    }

    function listObjects(bucketName, params, cb) {
        return mongoClientInterface.listObject(
            bucketName, params, log, (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, data);
            });
    }

    before(done => {
        async.waterfall([
            next => {
                MongoClient.connect(mongoUrl, {}, (err, client) => {
                    if (err) {
                        return next(err);
                    }
                    mongoClient = client;
                    return next(null);
                });
            },
            next => {
                const db = mongoClient.db(TEST_DB);
                db.createCollection(BUCKET_NAME, (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    collection = result;
                    return next(null);
                });
            },
            next => mongoClientInterface.setup(next),
            next => {
                const params = {
                    objName: 'pfx1-test-object',
                    objVal: {
                        key: 'pfx1-test-object',
                        versionId: 'null',
                    },
                    vFormat: 'v0',
                    nbVersions: 5,
                };
                putBulkObjectVersions(params.objName, params.objVal, params.vFormat,
                    params.nbVersions, err => {
                        if (err) {
                            return next(err);
                        }
                        return next(null);
                });
            },
            next => {
                const params = {
                    objName: 'pfx2-test-object',
                    objVal: {
                        key: 'pfx2-test-object',
                        versionId: 'null',
                    },
                    vFormat: 'v0',
                    nbVersions: 5,
                };
                putBulkObjectVersions(params.objName, params.objVal, params.vFormat,
                    params.nbVersions, err => {
                        if (err) {
                            return next(err);
                        }
                        return next(null);
                });
            },
        ], done);
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

    it('Should list master versions of objects', done => {
        const bucketName = BUCKET_NAME;
        const params = {
            listingType: 'DelimiterMaster',
            maxKeys: 100,
        };

        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(data.Contents.length, 2);
            return done();
        });
    });

    it('Should truncate list of master versions of objects', done => {
        const bucketName = BUCKET_NAME;
        const params = {
            listingType: 'DelimiterMaster',
            maxKeys: 1,
        };

        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(data.Contents.length, 1);
            return done();
        });
    });

    it('Should list master versions of objects that start with prefix', done => {
        const bucketName = BUCKET_NAME;
        const params = {
            listingType: 'DelimiterMaster',
            maxKeys: 100,
            prefix: 'pfx2',
        };

        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(data.Contents.length, 1);
            return done();
        });
    });

    it('Should return empty results when bucket not existing (master)', done => {
        const bucketName = 'non-existent-bucket';
        const params = {
            listingType: 'DelimiterMaster',
            maxKeys: 100,
        };

        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert(data);
            assert.strictEqual(data.Contents.length, 0);
            return done();
        });
    });

    it('Should list all versions of objects', done => {
        const bucketName = BUCKET_NAME;
        const params = {
            listingType: 'DelimiterVersions',
            maxKeys: 1000,
        };
        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(data.Versions.length, 10);
            return done();
        });
    });

    it('Should truncate list of master versions of objects', done => {
        const bucketName = BUCKET_NAME;
        const params = {
            listingType: 'DelimiterVersions',
            maxKeys: 5,
        };

        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(data.Versions.length, 5);
            return done();
        });
    });

    it('Should list master versions of objects that start with prefix', done => {
        const bucketName = BUCKET_NAME;
        const params = {
            listingType: 'DelimiterVersions',
            maxKeys: 100,
            prefix: 'pfx2',
        };

        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(data.Versions.length, 5);
            return done();
        });
    });

    it('Should return empty results when bucket not existing (version)', done => {
        const bucketName = 'non-existent-bucket';
        const params = {
            listingType: 'DelimiterVersions',
            maxKeys: 100,
        };

        return listObjects(bucketName, params, (err, data) => {
            assert.deepStrictEqual(err, null);
            assert(data);
            assert.strictEqual(data.Versions.length, 0);
            return done();
        });
    });

    it('should check entire list with pagination', () => {
        const versionsPerKey = {};
        const bucketName = BUCKET_NAME;
        const get = (maxKeys, keyMarker, cb) => listObjects(bucketName, {
                listingType: 'DelimiterVersions',
                maxKeys,
                keyMarker,
            }, (err, res) => {
                if (err) {
                    return cb(err);
                }
                res.Versions.forEach(version => {
                    if (versionsPerKey[version.key]) {
                        versionsPerKey[version.key] += 1;
                    } else {
                        versionsPerKey[version.key] = 0;
                    }
                });
                if (res.IsTruncated) {
                    return get(maxKeys, res.NextKeyMarker, cb);
                }
                return cb(null);
        });

        return get(3, null, err => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(Object.keys(versionsPerKey).length, 2);
            assert.strictEqual(versionsPerKey['pfx1-test-object'], 5);
            assert.strictEqual(versionsPerKey['pfx2-test-object'], 5);
        });
    });
});
