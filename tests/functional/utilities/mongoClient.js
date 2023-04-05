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
const PENSIEVE = 'PENSIEVE';

const mongoUrl = `mongodb://${replicaSetHosts}/?w=${writeConcern}&` +
    `replicaSet=${replicaSet}&readPreference=${readPreference}`;

const ownerCanonicalId =
    '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
const overlayVersionId = 'configuration/overlay-version';
const getOverlayConfigId = vId => `configuration/overlay/${vId}`;

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
        return this.client.close(true).then(() => cb()).catch(() => cb());
    }

    connectClient(cb) {
        return MongoClient.connect(this.mongoUrl, this.options)
            .then(client => {
                this.client = client;
                this.db = client.db(this.database, {
                    ignoreUndefined: true,
                });
                return cb();
            })
            .catch(err => cb(err));
    }

    /**
     * @param {Object} overlay - overlay object
     * @param {Number} id - version id
     * @param {Function} cb - callback(err)
     * @return {undefined}
     */
    setupMockOverlayConfig(overlay, id, cb) {
        const c = this.db.collection(PENSIEVE);
        c.bulkWrite(
            [{
                updateOne: {
                    filter: {
                        _id: overlayVersionId,
                    },
                    update: {
                        $set: {
                            _id: overlayVersionId, value: id,
                        }
                    },
                    upsert: true,
                },
            }, {
                updateOne: {
                    filter: {
                        _id: getOverlayConfigId(id),
                    },
                    update: {
                        $set: {
                            _id: getOverlayConfigId(id), value: overlay,
                        }
                    },
                    upsert: true,
                }
            }],
            { ordered: 1 }).then(() => cb()).catch(err => cb(err));
    }

    deleteMockOverlayConfig(id, cb) {
        const c = this.db.collection(PENSIEVE);
        c.bulkWrite(
            [{
                deleteOne: {
                    filter: {
                        _id: overlayVersionId,
                    },
                },
            }, {
                deleteOne: {
                    filter: {
                        _id: getOverlayConfigId(id),
                    }
                }
            }], {}).then(() => cb()).catch(err => cb(err));
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
        m.updateOne({
            _id: bucketName,
        }, {
            $set: {
                _id: bucketName,
                value: mdValue,
            },
        }, {
            upsert: true,
        }).then(() => this.db.createCollection(bucketName, {})
            .then(() => cb())
            .catch(err => cb(err)))
            .catch(() => cb(errors.InternalError));
    }

    deleteBucket(bucketName, cb) {
        return async.series({
            deleteCollection: next => {
                const c = this.db.collection(bucketName);
                c.drop({}).then(() => next()).catch(err => {
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
                }, {}).then(() => next()).catch(err => {
                    if (err && err.codeName !== 'NameSpaceNodeFound') {
                        return next(err);
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
