const async = require('async');
const { models, errors } = require('arsenal');
const BucketInfo = models.BucketInfo;
const { MongoClient } = require('mongodb');

const replicaSetHosts = 'localhost:27018,localhost:27019,localhost:27020';
const writeConcern = 'majority';
const replicaSet = 'rs0';
const readPreference = 'primary';
const database = 'metadata';

const METASTORE = '__metastore';

const mongoUrl = `mongodb://${replicaSetHosts}/?w=${writeConcern}&` +
    `replicaSet=${replicaSet}&readPreference=${readPreference}`;

const ownerCanonicalId =
    '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';

class MongoTestClient {
    constructor(config) {
        this.mongoUrl = config.mongoUrl;
        this.options = config.options;
        this.database = config.database;
        this.client = null;
        this.db = null;
    }

    isConnected() {
        return !!this.client;
    }

    disconnectClient(cb) {
        return this.client.close(true, cb);
    }

    connectClient(cb) {
        return MongoClient.connect(this.mongoUrl, this.options,
        (err, client) => {
            if (err) {
                return cb(err);
            }
            this.client = client;
            this.db = client.db(this.database, {
                ignoreUndefined: true,
            });
            return cb();
        });
    }

    createBucket(bucketName, location, cb) {
        const creationDate = new Date().toJSON();
        const bucketMD = new BucketInfo(bucketName,
            ownerCanonicalId, ownerCanonicalId, creationDate,
            BucketInfo.currentModelVersion());
        bucketMD.setLocationConstraint(location);
        const bucketInfo = BucketInfo.fromObj(bucketMD);
        const bucketMDStr = bucketInfo.serialize();
        const mdValue = JSON.parse(bucketMDStr);
        const m = this.db.collection(METASTORE);
        m.update({
            _id: bucketName,
        }, {
            _id: bucketName,
            value: mdValue,
        }, {
            upsert: true,
        }, err => {
            if (err) {
                return cb(errors.InternalError);
            }
            return this.db.createCollection(bucketName, {}, cb);
        });
    }

    deleteBucket(bucketName, cb) {
        return async.series({
            deleteCollection: next => {
                const c = this.db.collection(bucketName);
                c.drop({}, err => {
                    if (err && err.codeName !== 'NameSpaceNodeFound') {
                        return next(err);
                    }
                    return next();
                });
            },
            deleteMetastoreEntry: next => {
                const m = this.db.collection(METASTORE);
                m.findOneAndDelete({
                    _id: bucketName,
                }, {}, (err, res) => {
                    if (err || res.ok !== 1) {
                        return next(errors.InternalError);
                    }
                    return next();
                });
            },
        }, cb);
    }
}

const mongoClient = new MongoTestClient({
    mongoUrl,
    options: {},
    database,
});

module.exports = {
    MongoTestClient,
    mongoClient,
};
