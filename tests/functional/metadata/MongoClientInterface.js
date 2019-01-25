const assert = require('assert');
const async = require('async');

const MongoClient = require('mongodb').MongoClient;
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;

const log = require('./utils/fakeLogger');

const replicaSetHosts = 'localhost:27018,localhost:27019,localhost:27020';
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
const TAG_1 = '557b9096-f3d9-4a70-bbb9-72edc757287f';
const TAG_2 = '3d7383a8-3d43-4370-b276-66f14352140e';

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

const updatedObjVal = {
    updated: true,
};

const runIfMongo =
    process.env.S3METADATA === 'mongodb' ? describe : describe.skip;

runIfMongo('MongoClientInterface', () => {
    let mongoClient;
    let collection;

    function checkTag({ shouldHaveUpdated }, cb) {
        return collection.findOne({ _id: OBJECT_NAME }, (err, result) => {
            if (err) {
                return cb(err);
            }
            if (shouldHaveUpdated) {
                assert(result.tag !== TAG_1);
                assert(result.value.updated);
            } else {
                assert(result.tag === TAG_1);
                assert.deepStrictEqual(result.value, objVal);
            }
            return cb();
        });
    }

    beforeEach(done =>
        MongoClient.connect(mongoUrl, {}, (err, client) => {
            if (err) {
                return done(err);
            }
            mongoClient = client;
            const db = mongoClient.db(TEST_DB);
            return db.createCollection(TEST_COLLECTION, (err, result) => {
                if (err) {
                    return done(err);
                }
                collection = result;
                return done();
            });
        }));

    afterEach(done => {
        const db = mongoClient.db(TEST_DB);
        return db.dropDatabase(err => {
            if (err) {
                return done(err);
            }
            return mongoClient.close(true, done);
        });
    });

    describe('::putObjectNoVer', () => {
        beforeEach(done =>
            collection.insertOne({
                _id: OBJECT_NAME,
                tag: TAG_1,
                value: objVal,
            }, done));

        function testPutMetadata({ params, shouldHaveUpdated }, cb) {
            async.series([
                next => mongoClientInterface.putObjectNoVer(collection,
                    BUCKET_NAME, OBJECT_NAME, updatedObjVal, params, log, next),
                next => checkTag({ shouldHaveUpdated }, next),
            ], cb);
        }

        it('should update metadata when no tag is provided', done =>
            testPutMetadata({
                params: {},
                shouldHaveUpdated: true,
            }, done));

        it('should update metadata when matching tag is provided', done =>
            testPutMetadata({
                params: {
                    condPut: {
                        tag: TAG_1,
                    },
                },
                shouldHaveUpdated: true,
            }, done));

        it('should not update metadata when non-matching tag is provided',
            done =>
                testPutMetadata({
                    params: {
                        condPut: {
                            tag: 'non-matching-tag',
                        },
                    },
                    shouldHaveUpdated: false,
                }, done));
    });

    describe('::putObjectVerCase2', () => {
        beforeEach(done => {
            collection.insertOne({
                _id: OBJECT_NAME,
                tag: TAG_1,
                value: objVal,
            }, done);
        });

        function testPutMetadata({ params, shouldHaveUpdated }, cb) {
            async.series([
                next => mongoClientInterface.putObjectVerCase2(collection,
                    BUCKET_NAME, OBJECT_NAME, updatedObjVal, params, log, next),
                next => checkTag({ shouldHaveUpdated }, next),
            ], cb);
        }

        it('should update metadata when no tag is provided', done =>
            testPutMetadata({
                params: {},
                shouldHaveUpdated: true,
            }, done));

        it('should update metadata when matching tag is provided', done =>
            testPutMetadata({
                params: {
                    condPut: {
                        tag: TAG_1,
                    },
                },
                shouldHaveUpdated: true,
            }, done));

        it('should not update metadata when non-matching tag is provided',
            done =>
                testPutMetadata({
                    params: {
                        condPut: {
                            tag: 'non-matching-tag',
                        },
                    },
                    shouldHaveUpdated: false,
                }, done));
    });

    describe('::putObjectVerCase3', () => {
        const vObjName = `${OBJECT_NAME}${VID_SEP}${VERSION_ID}`;

        beforeEach(done => {
            async.series([
                next => collection.insertOne({
                    _id: vObjName,
                    tag: TAG_1,
                    value: objVal,
                }, next),
                next => collection.insertOne({
                    _id: OBJECT_NAME,
                    tag: TAG_2,
                    value: objVal,
                }, next),
            ], done);
        });

        function testPutMetadata({ params, shouldHaveUpdated }, cb) {
            async.series([
                next => mongoClientInterface.putObjectVerCase3(collection,
                    BUCKET_NAME, OBJECT_NAME, updatedObjVal, params, log, next),
                next => async.series([
                    done =>
                        collection.findOne({
                            _id: vObjName,
                        }, (err, result) => {
                            if (err) {
                                return cb(err);
                            }
                            if (shouldHaveUpdated) {
                                assert(result.tag !== TAG_1);
                                assert(result.value.updated);
                            } else {
                                assert(result.tag === TAG_1);
                                assert.deepStrictEqual(result.value, objVal);
                            }
                            return done();
                        }),
                    done =>
                        collection.findOne({
                            _id: OBJECT_NAME,
                        }, (err, result) => {
                            if (err) {
                                return cb(err);
                            }
                            if (shouldHaveUpdated) {
                                assert(result.tag !== TAG_2);
                                assert(result.value.updated);
                            } else {
                                assert(result.tag === TAG_2);
                                assert.deepStrictEqual(result.value, objVal);
                            }
                            return done();
                        }),
                ], next),
            ], cb);
        }

        it('should update metadata when no tag is provided', done => {
            testPutMetadata({
                params: {
                    versionId: VERSION_ID,
                },
                shouldHaveUpdated: true,
            }, done);
        });

        it('should update metadata when matching tag is provided', done =>
            testPutMetadata({
                params: {
                    condPut: {
                        tag: TAG_2,
                    },
                    versionId: VERSION_ID,
                },
                shouldHaveUpdated: true,
            }, done));

        it('should not update metadata when non-matching tag is provided',
            done =>
                testPutMetadata({
                    params: {
                        condPut: {
                            tag: 'non-matching-tag',
                        },
                        versionId: VERSION_ID,
                    },
                    shouldHaveUpdated: false,
                }, done));
    });
});
