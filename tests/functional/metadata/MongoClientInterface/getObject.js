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

runIfMongo('MongoClientInterface::getObject', () => {
    let mongoClient;
    let collection;
    let versionId1;
    let versionId2;

    const params = {
        objName: 'pfx1-test-object',
        objVal: {
            key: 'pfx1-test-object',
            versionId: 'null',
        },
        vFormat: 'v0',
    };

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

    function updateMasterObject(objName, versionId, objVal, vFormat, cb) {
        const mKey = formatMasterKey(objName, vFormat);
        collection.updateOne(
            {
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
            {
                $set: { _id: objName, value: objVal },
            },
            { upsert: true },
            err => {
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

    function getObject(bucketName, objName, versionId, cb) {
        return mongoClientInterface.getObject(
            bucketName, objName, { versionId }, log, (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, data);
            });
    }

    before(done => {
        async.waterfall([
            next => mongoClientInterface.setup(next),
            next => {
                MongoClient.connect(mongoUrl, {}, (err, client) => {
                    if (err) {
                        return next(err);
                    }
                    mongoClient = client;
                    return next(null);
                });
            },
        ], done);
    });

    beforeEach(done => {
        async.waterfall([
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
            next => {
                versionId1 = generateVersionId();
                params.objVal.versionId = versionId1;
                putVersionedObject(params.objName, versionId1, params.objVal,
                    params.vFormat, next);
            },
            next => {
                versionId2 = generateVersionId();
                params.objVal.versionId = versionId2;
                putVersionedObject(params.objName, versionId2, params.objVal,
                    params.vFormat, next);
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

    afterEach(done => collection.drop(err => {
            if (err) {
                return done(err);
            }
            return done();
        })
    );

    it('Should return latest version of object', done => {
        const bucketName = BUCKET_NAME;
        return getObject(bucketName, params.objName, null, (err, object) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(object.key, params.objName);
            assert.strictEqual(object.versionId, versionId2);
            return done();
        });
    });

    it('Should return the specified version of object', done => {
        const bucketName = BUCKET_NAME;
        return getObject(bucketName, params.objName, versionId1, (err, object) => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(object.key, params.objName);
            assert.strictEqual(object.versionId, versionId1);
            return done();
        });
    });

    it('Should throw error when version non existent', done => {
        const bucketName = BUCKET_NAME;
        const versionId = '12345';
        return getObject(bucketName, params.objName, versionId, err => {
            assert.deepStrictEqual(err, errors.NoSuchKey);
            return done();
        });
    });

    it('Should throw error when object non existent', done => {
        const bucketName = BUCKET_NAME;
        const objName = 'non-existent-object';
        return getObject(bucketName, objName, null, err => {
            assert.deepStrictEqual(err, errors.NoSuchKey);
            return done();
        });
    });

    it('Should throw error when object non existent', done => {
        const bucketName = 'non-existent-bucket';
        return getObject(bucketName, params.objName, null, err => {
            assert.deepStrictEqual(err, errors.NoSuchKey);
            return done();
        });
    });

    it('Should return latest version when master is PHD', done => {
        const bucketName = BUCKET_NAME;
        async.waterfall([
            next => {
                // adding isPHD flag to master
                const phdVersionId = generateVersionId();
                params.objVal.versionId = phdVersionId;
                params.objVal.isPHD = true;
                updateMasterObject(params.objName, phdVersionId, params.objVal,
                    params.vFormat, next);
            },
            // Should return latest object version
            next => getObject(bucketName, params.objName, null, (err, object) => {
                assert.deepStrictEqual(err, null);
                assert.strictEqual(object.key, params.objName);
                assert.strictEqual(object.versionId, versionId2);
                delete params.objVal.isPHD;
                return next();
            })
        ], done);
    });
});
