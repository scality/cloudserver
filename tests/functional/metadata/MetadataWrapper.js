const assert = require('assert');
const async = require('async');
const uuid = require('uuid/v4');

const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const BucketInfo = require('arsenal').models.BucketInfo;
const MongoClient = require('mongodb').MongoClient;
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;

const log = require('./utils/fakeLogger');

const replicaSetHosts = 'localhost:27018,localhost:27019,localhost:27020';
const writeConcern = 'majority';
const replicaSet = 'rs0';
const readPreference = 'primary';
// const mongoUrl = `mongodb://${replicaSetHosts}/?w=${writeConcern}&` +
//     `replicaSet=${replicaSet}&readPreference=${readPreference}`;
const mongoUrl = 'mongodb://127.0.0.1:27017';

const VID_SEP = '\0';
const TEST_DB = 'test';
// const TEST_COLLECTION = 'test-collection';
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

const metadata = new MetadataWrapper('mongodb', {
    mongodb: {
        replicaSetHosts,
        writeConcern,
        replicaSet,
        readPreference,
        replicationGroupId: 'RG001',
        database: TEST_DB,    
    },
}, undefined, log);

const tag = uuid();
const objMD = { updated: false };
const bucketInfo = new BucketInfo(BUCKET_NAME, 'foo', 'bar', `${new Date()}`);

const runIfMongo =
    process.env.S3METADATA === 'mongodb' ? describe : describe.skip;
    
function jobFunc(bucket, key, tag, objVal) {
    // TODO: Update the tag from the next read.
    
    // Check if the update is relevant. If it's not, return null. For example,
    // if you find yourself updating an overwritten object, there is no need to
    // continue.
    // if (tag !== TAG_1) {
    //     return { retry: false };
    // }
    // Perform the operation. For example, updating the storage class.
    const valueUpdate = { updated: true };
    return { objVal: Object.assign({}, objVal, valueUpdate) };
}

runIfMongo('MongoClientInterface', () => {
    let mongoClient;
    let collection;
    let db;
    
    before(done => metadata.setup(done));

    beforeEach(done => {
        async.series([
            next => MongoClient.connect(mongoUrl, {}, (err, client) => {
                if (err) {
                    return next(err);
                }
                mongoClient = client;
                return next();
            }),
            next => {
                db = mongoClient.db(TEST_DB);
                db.createCollection(TEST_COLLECTION, next);
            },
            next => metadata.createBucket(BUCKET_NAME, bucketInfo, log, next),
        ], done);
    });
    
    afterEach(done => {
        const db = mongoClient.db(TEST_DB);
        return db.dropDatabase(err => {
            if (err) {
                return done(err);
            }
            return mongoClient.close(true, done);
        });
    });

    it('should put object metadata when jobFunc returns', done => 
        async.series([
            next => {
                const collection = db.collection(BUCKET_NAME);
                collection.insertOne({
                    _id: OBJECT_NAME,
                    tag: TAG_1,
                    value: objMD,
                }, next);
            },
            next => metadata.safePutObjectMD(BUCKET_NAME, OBJECT_NAME, 
                { jobFunc, tag }, log, next),
            next => metadata.getObjectMD(BUCKET_NAME, OBJECT_NAME, {}, log, 
                (err, objMD) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(objMD.updated, true);
                    done();
                }),
        ], done));

    it('should not put object metadata', done => 
        async.series([
            next => {
                const collection = db.collection(BUCKET_NAME);
                collection.insertOne({
                    _id: OBJECT_NAME,
                    tag: TAG_2,
                    value: objMD,
                }, next);
            },
            next => metadata.safePutObjectMD(BUCKET_NAME, OBJECT_NAME, 
                { jobFunc, tag: uuid() }, log, next),
            next => metadata.getObjectMD(BUCKET_NAME, OBJECT_NAME, {}, log, 
                (err, objMD) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(objMD.updated, false);
                    done();
                }),
        ], done));
        
    it.only('should put object metadata when conflicting writes', function t(done) {
        this.timeout(30000)
        const collection = db.collection(BUCKET_NAME);
        function iteratee(n, callback) {
            async.parallel([
                next => {
                    collection.update(
                        { 
                            _id: OBJECT_NAME,
                        },
                        {
                            _id: OBJECT_NAME,
                            tag: uuid(),
                            value: objMD,
                        }, err => {
                            console.log('updated...');
                            next(err);
                        });
                },
                next => {
                    metadata.safePutObjectMD(BUCKET_NAME, OBJECT_NAME, { 
                        jobFunc, 
                        condPut: {
                            tag
                        },
                    }, log, next);
                },
            ], callback);
        }
        collection.insertOne({
            _id: OBJECT_NAME,
            tag: TAG_1,
            value: objMD,
        }, err => {
            if (err) {
                return done(err);
            }
            async.times(10, iteratee, done);
        });
        
        // async.series([
        //     next => {
        // 
        //     },
        // 
        //     next => metadata.getObjectMD(BUCKET_NAME, OBJECT_NAME, {}, log, 
        //         (err, objMD) => {
        //             if (err) {
        //                 return done(err);
        //             }
        //             assert.strictEqual(objMD.updated, true);
        //             done();
        //         }),
        // ], done)    
    });
});
