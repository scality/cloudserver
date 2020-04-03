const assert = require('assert');
const async = require('async');

const MongoClient = require('mongodb').MongoClient;
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;
const { errors } = require('arsenal');

const log = require('./utils/fakeLogger');

const replicaSetHosts = 'localhost:27017,localhost:27018,localhost:27019';
const writeConcern = 'majority';
const replicaSet = 'rs0';
const readPreference = 'primary';
const mongoUrl = `mongodb://${replicaSetHosts}/?w=${writeConcern}&` +
    `replicaSet=${replicaSet}&readPreference=${readPreference}`;

const VID_SEP = '\0';
const TEST_DB = 'test';
const TEST_COLLECTION = 'test-collection';
const BUCKET_NAME = 'test-bucket';
const OBJECT_NAME = 'test-object';
const VERSION_ID = '98451712418844999999RG001  22019.0';

const mongoClientInterface = new MongoClientInterface({
    replicaSetHosts,
    writeConcern,
    replicaSet,
    readPreference,
    replicationGroupId: 'RG001',
    database: TEST_DB,
    logger: log,
});

const objVal = {
    key: OBJECT_NAME,
    versionId: VERSION_ID,
    updated: false,
};

const updatedObjVal = { updated: true };

const runIfMongo =
    process.env.S3METADATA === 'mongodb' ? describe : describe.skip;

function unescape(obj) {
    return JSON.parse(JSON.stringify(obj).
                      replace(/\uFF04/g, '$').
                      replace(/\uFF0E/g, '.'));
}

function getCaseMethod(number) {
    assert(number >= 0 && number <= 4);

    const methodNames = {
        0: 'putObjectNoVer',
        1: 'putObjectVerCase1',
        2: 'putObjectVerCase2',
        3: 'putObjectVerCase3',
        4: 'putObjectVerCase4',
    };
    return methodNames[number];
}

runIfMongo('MongoClientInterface', () => {
    let mongoClient;
    let collection;

    function getObject(params, cb) {
        let objName = OBJECT_NAME;
        if (params && params.versionId) {
            // eslint-disable-next-line
            objName = `${objName}${VID_SEP}${params.versionId}`;
        }
        collection.findOne({
            _id: objName,
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

    function checkVersionAndMasterMatch(versionId, cb) {
        async.parallel([
            next => getObject({}, next),
            next => getObject({ versionId }, next),
        ], (err, res) => {
            if (err) {
                return cb(err);
            }
            assert.strictEqual(res.length, 2);
            assert.deepStrictEqual(res[0], res[1]);
            return cb();
        });
    }

    // objectValue is an optional argument
    function putObject(caseNum, params, objectValue, cb) {
        const method = getCaseMethod(caseNum);
        const dupeObjVal = Object.assign({}, objectValue || objVal);
        mongoClientInterface[method](collection, BUCKET_NAME, OBJECT_NAME,
            dupeObjVal, params, log, (err, res) => {
                if (err) {
                    return cb(err);
                }
                let parsedRes;
                if (res) {
                    try {
                        parsedRes = JSON.parse(res);
                    } catch (error) {
                        return cb(error);
                    }
                }
                return cb(null, parsedRes);
            });
    }

    function checkNewPutObject(caseNum, params, cb) {
        const method = getCaseMethod(caseNum);
        const bucket = 'a';
        const key = 'b';
        async.series([
            next => mongoClientInterface[method](
                collection, bucket, key, updatedObjVal, params, log, next),
            next => {
                collection.findOne({ _id: key }, (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    assert.strictEqual(result._id, key);
                    assert(result.value.updated);
                    return next();
                });
            },
        ], cb);
    }

    before(done => {
        MongoClient.connect(mongoUrl, {}, (err, client) => {
            if (err) {
                return done(err);
            }
            mongoClient = client;
            return done();
        });
    });

    beforeEach(done => {
        const db = mongoClient.db(TEST_DB);
        return db.createCollection(TEST_COLLECTION, (err, result) => {
            if (err) {
                return done(err);
            }
            collection = result;
            return done();
        });
    });

    after(done => mongoClient.close(true, done));

    afterEach(done => {
        const db = mongoClient.db(TEST_DB);
        return db.dropDatabase(err => {
            if (err) {
                return done(err);
            }
            return done();
        });
    });

    describe('::putObjectNoVer', () => {
        it('should put new metadata', done => checkNewPutObject(0, {}, done));
    });

    describe('::putObjectVerCase1', () => {
        it('should put new metadata and update master', done => {
            async.waterfall([
                next => putObject(1, {}, null,
                    (err, res) => next(err, res.versionId)),
                (id, next) => checkVersionAndMasterMatch(id, next),
            ], done);
        });
    });

    describe('::putObjectVerCase2', () => {
        it('should put new metadata', done => checkNewPutObject(2, {}, done));

        it('should set new version id for master', done => {
            async.waterfall([
                // first create new ver and master
                next => putObject(1, {}, null, next),
                // check master and version were created and match
                (res, next) => checkVersionAndMasterMatch(res.versionId,
                    err => next(err, res.versionId)),
                // call ver case 2
                (id, next) => putObject(2, {}, null, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    assert(id !== res.versionId);
                    return next(null, res.versionId);
                }),
                // assert master updated with new version id
                (newId, next) => getObject({}, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    assert.strictEqual(res.versionId, newId);
                    return next(null, newId);
                }),
                // new version entry should not have been created
                (id, next) => getObject({ versionId: id }, err => {
                    assert(err);
                    assert(err.NoSuchKey);
                    return next();
                }),
            ], done);
        });
    });

    describe('::putObjectVerCase3', () => {
        it('should put new metadata', done =>
            checkNewPutObject(3, { versionId: VERSION_ID }, done));

        it('should put new metadata and not update master', done => {
            async.waterfall([
                // first create new ver and master
                next => putObject(1, {}, null, next),
                // check master and version were created and match
                (res, next) => checkVersionAndMasterMatch(res.versionId,
                    err => next(err, res.versionId)),
                // call ver case 3
                (id, next) => putObject(3, { versionId: VERSION_ID }, null,
                    (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        // assert new version id created
                        assert(id !== res.versionId);
                        assert.strictEqual(res.versionId, VERSION_ID);
                        return next(null, id);
                    }),
                // assert master did not update and matches old initial version
                (oldId, next) => getObject({}, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    assert.strictEqual(oldId, res.versionId);
                    return next();
                }),
                // assert new version was created
                next => getObject({ versionId: VERSION_ID }, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    assert(res);
                    assert.strictEqual(res.versionId, VERSION_ID);
                    return next();
                }),
            ], done);
        });

        it('should put new metadata and update master if version id matches',
        done => {
            async.waterfall([
                // first create new ver and master
                next => putObject(1, {}, null, next),
                // check master and version were created and match
                (res, next) => checkVersionAndMasterMatch(res.versionId,
                    err => next(err, res.versionId)),
                // call ver case 3 w/ same version id and update
                (id, next) => mongoClientInterface.putObjectVerCase3(collection,
                    BUCKET_NAME, OBJECT_NAME, updatedObjVal,
                    { versionId: id }, log, err => next(err, id)),
                (oldId, next) => getObject({}, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    // assert updated
                    assert(res);
                    assert(res.updated);
                    // assert same version id
                    assert.strictEqual(oldId, res.versionId);
                    return next();
                }),
            ], done);
        });
    });

    describe('::putObjectVerCase4', () => {
        function putAndCheckCase4(versionId, cb) {
            const objectValue = Object.assign({}, objVal, { versionId });
            async.waterfall([
                // put object
                next => putObject(4, { versionId }, objectValue, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, res.versionId);
                }),
                (id, next) => getObject({}, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    // assert PHD was placed on master
                    assert(res);
                    assert.strictEqual(res.isPHD, true);
                    // assert same version id as master
                    assert.strictEqual(id, res.versionId);
                    return next();
                }),
            ], cb);
        }

        it('should put new metadata and update master', done => {
            putAndCheckCase4(VERSION_ID, done);
        });

        it('should always update master', done => {
            let count = 0;
            function getNewVersion() {
                const prefix = `984517124${count}8844999999`;
                const repID = 'RG001  ';
                const suffix = `22019.${count++}`;
                return `${prefix}${repID}${suffix}`;
            }
            async.series([
                next => putAndCheckCase4(getNewVersion(), next),
                next => putAndCheckCase4(getNewVersion(), next),
                next => putAndCheckCase4(getNewVersion(), next),
            ], done);
        });
    });
});
