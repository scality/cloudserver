const assert = require('assert');
const async = require('async');

const MongoClient = require('mongodb').MongoClient;
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;
const { errors, versioning } = require('arsenal');
const { DbPrefixes } = versioning.VersioningConstants;

const log = require('../utils/fakeLogger');

const replicaSetHosts = 'localhost:27017,localhost:27018,localhost:27019';
const writeConcern = 'majority';
const replicaSet = 'rs0';
const readPreference = 'primary';
const mongoUrl = `mongodb://${replicaSetHosts}/?w=${writeConcern}&` +
    `replicaSet=${replicaSet}&readPreference=${readPreference}`;

const VID_SEP = '\0';
const TEST_DB = 'test';
const BUCKET_NAME = 'test-bucket';
const OBJECT_NAME = 'test-object';
const VERSION_ID = '98451712418844999999RG001  22019.0';
const BUCKET_VFORMAT_V0 = 'v0';

const mongoClientInterface = new MongoClientInterface({
    replicaSetHosts,
    writeConcern,
    replicaSet,
    readPreference,
    replicationGroupId: 'RG001',
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

runIfMongo('MongoClientInterface::putObject', () => {
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

    function putObject(bucketName, objName, objVal, params, cb) {
        return mongoClientInterface.putObject(
            bucketName, objName, objVal, params, log, (err, res) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, res);
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

    it('Should put a new non versionned object', done => {
        const bucketName = BUCKET_NAME;
        const objName = OBJECT_NAME;
        const objVal = {
            key: OBJECT_NAME,
            versionId: 'null',
            updated: false,
        };
        const params = {
            versioning: null,
            versionId: null,
            repairMaster: null,
        };
        async.waterfall([
            next => putObject(bucketName, objName, objVal, params, err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                return next();
            }),
            // When versionning not active only one document is created (master)
            next => getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 1);
                return next();
            }),
        ], done);
    });

    it('Should update the metadata', done => {
        const bucketName = BUCKET_NAME;
        const objName = OBJECT_NAME;
        const objVal = {
            key: OBJECT_NAME,
            versionId: 'null',
            updated: false,
        };
        const params = {
            versioning: null,
            versionId: null,
            repairMaster: null,
        };
        async.waterfall([
            next => putObject(bucketName, objName, objVal, params, err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            next => {
                objVal.updated = true;
                putObject(bucketName, objName, objVal, params, err => {
                    if (err) {
                        return next(err);
                    }
                    return next();
                });
            },
            // object metadata must be updated
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                assert.strictEqual(object.updated, true);
                return next();
            }),
            // Only a master version should be created
            next => getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 1);
                return next();
            }),
        ], done);
    });

    it('Should put versionned object with the specific versionId', done => {
        const bucketName = BUCKET_NAME;
        const objName = OBJECT_NAME;
        const objVal = {
            key: OBJECT_NAME,
            versionId: VERSION_ID,
            updated: false,
        };
        const params = {
            versioning: true,
            versionId: VERSION_ID,
            repairMaster: null,
        };
        async.waterfall([
            next => putObject(bucketName, objName, objVal, params, err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            // checking if metadata corresponds to what was given to the function
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                assert.strictEqual(object.versionId, VERSION_ID);
                return next();
            }),
            // We'll have one master and one version
            next => getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 2);
                return next();
            }),
        ], done);
    });

    it('Should put new version and update master', done => {
        const bucketName = BUCKET_NAME;
        const objName = OBJECT_NAME;
        const objVal = {
            key: OBJECT_NAME,
            versionId: VERSION_ID,
            updated: false,
        };
        const params = {
            versioning: true,
            versionId: null,
            repairMaster: null,
        };
        let versionId = null;

        async.waterfall([
            // We first create a master and a version
            next => putObject(bucketName, objName, objVal, params, err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                versionId = object.versionId;
                return next();
            }),
            // We put another version of the object
            next => putObject(bucketName, objName, objVal, params, err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            // Master must be updated
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                assert.notStrictEqual(object.versionId, versionId);
                return next();
            }),
            // we'll have two versions and one master
            next => getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 3);
                return next();
            }),
        ], done);
    });

    it('Should update master when versionning is disabled', done => {
        const bucketName = BUCKET_NAME;
        const objName = OBJECT_NAME;
        const objVal = {
            key: OBJECT_NAME,
            versionId: VERSION_ID,
            updated: false,
        };
        const params = {
            versioning: true,
            versionId: null,
            repairMaster: null,
        };
        let versionId = null;
        async.waterfall([
            // We first create a new version and master
            next => putObject(bucketName, objName, objVal, params, err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                versionId = object.versionId;
                return next();
            }),
            next => {
                // Disabling versionning and putting new version
                params.versioning = false;
                params.versionId = '';
                return putObject(bucketName, objName, objVal, params, err => {
                    if (err) {
                        return next(err);
                    }
                    return next();
                });
            },
            // Master must be updated
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                assert.notStrictEqual(object.versionId, versionId);
                return next();
            }),
            // The second put shouldn't create a new version
            next => getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 2);
                return next();
            }),
        ], done);
    });

    it('Should update latest version and repair master', done => {
        const bucketName = BUCKET_NAME;
        const objName = OBJECT_NAME;
        const objVal = {
            key: OBJECT_NAME,
            versionId: VERSION_ID,
            updated: false,
        };
        const params = {
            versioning: true,
            versionId: VERSION_ID,
            repairMaster: null,
        };
        async.waterfall([
            // We first create a new version and master
            next => putObject(bucketName, objName, objVal, params, err => {
                if (err) {
                    return next(err);
                }
                return next();
            }),
            next => {
                // Updating the version and repairing master
                params.repairMaster = true;
                objVal.updated = true;
                return putObject(bucketName, objName, objVal, params, err => {
                    if (err) {
                        return next(err);
                    }
                    return next();
                });
            },
            // Master must be updated
            next => getObject(objName, null, BUCKET_VFORMAT_V0, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, OBJECT_NAME);
                assert.strictEqual(object.versionId, VERSION_ID);
                assert.strictEqual(object.updated, true);
                return next();
            }),
            // The second put shouldn't create a new version
            next => getObjectCount((err, count) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(count, 2);
                return next();
            }),
        ], done);
    });
});
