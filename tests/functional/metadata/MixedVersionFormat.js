const assert = require('assert');
const async = require('async');
const withV4 = require('../aws-node-sdk/test/support/withV4');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const MongoClient = require('mongodb').MongoClient;
const errors = require('arsenal');

const replicaSetHosts = 'localhost:27017,localhost:27018,localhost:27019';
const writeConcern = 'majority';
const replicaSet = 'rs0';
const readPreference = 'primary';
const mongoUrl = `mongodb://${replicaSetHosts}/?w=${writeConcern}&` +
    `replicaSet=${replicaSet}&readPreference=${readPreference}`;

/**
 * These tests are intended to see if the vFormat of buckets is respected
 * when performing operations on the buckets. We perform operations on
 * buckets with mixed vFormats that were created at the same time
 *
 * The mongo metadata uses a cache to store the vFormat of buckets,
 * that value is supposed to be immutable, hence the value in cache never
 * gets updated. Also the value of the default vFormat to use can not be
 * modified while cloudserver is running.
 *
 * To overcome these issues, we set the METADATA_MAX_CACHED_BUCKETS
 * environement variable to 1 which means that only one bucket vFormat is cached
 * at once, and set the DEFAULT_BUCKET_KEY_FORMAT variable to v1.
 *
 * Now that we can only store one bucket vFormat in cache, buckets v0 and v1 are
 * created in order, which leads to the first bucket's (v0) vFormat being removed
 * from the cache. The vFormat of the v0 bucket is then updated manually.
 * Next time the v0 bucket vFormat gets requested it will return the updated version
 * i.e v0
 */
describe('Mongo backend mixed bucket format versions', () => {
    withV4(sigCfg => {
        let mongoClient;
        let bucketUtil;
        let s3;

        function updateBucketVFormat(bucketName, vFormat) {
            const db = mongoClient.db('metadata');
            return db.collection('__metastore')
                .updateOne({
                    _id: bucketName,
                }, {
                    $set: { vFormat },
                }, {});
        }

        function getObject(bucketName, key, cb) {
            const db = mongoClient.db('metadata');
            return db.collection(bucketName)
            .findOne({
                _id: key,
            }, {}, (err, doc) => {
                if (err) {
                    return cb(err);
                }
                if (!doc) {
                    return cb(errors.NoSuchKey);
                }
                return cb(null, doc.value);
            });
        }

        before(done => {
            MongoClient.connect(mongoUrl, {}, (err, client) => {
                if (err) {
                    return done(err);
                }
                mongoClient = client;
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
                return done();
            });
        });

        beforeEach(() => {
            process.stdout.write('Creating buckets');
            return bucketUtil.createMany(['v0-bucket', 'v1-bucket'])
            .then(async () => {
                process.stdout.write('Updating bucket vFormat');
                await updateBucketVFormat('v0-bucket', 'v0');
                await updateBucketVFormat('v1-bucket', 'v1');
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying buckets');
            return bucketUtil.emptyMany(['v0-bucket', 'v1-bucket'])
            .then(() => {
                process.stdout.write('Deleting buckets');
                return bucketUtil.deleteMany(['v0-bucket', 'v1-bucket']);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        after(done => mongoClient.close(true, done));

        ['v0', 'v1'].forEach(vFormat => {
            it(`Should perform operations on non versioned bucket in ${vFormat} format`, done => {
                const paramsObj1 = {
                    Bucket: `${vFormat}-bucket`,
                    Key: `${vFormat}-object-1`
                };
                const paramsObj2 = {
                    Bucket: `${vFormat}-bucket`,
                    Key: `${vFormat}-object-2`
                };
                const masterKey = vFormat === 'v0' ? `${vFormat}-object-1` : `\x7fM${vFormat}-object-1`;
                async.series([
                    next => s3.putObject(paramsObj1, next),
                    next => s3.putObject(paramsObj2, next),
                    // check if data stored in the correct format
                    next => getObject(`${vFormat}-bucket`, masterKey, (err, doc) => {
                        assert.ifError(err);
                        assert.strictEqual(doc.key, `${vFormat}-object-1`);
                        return next();
                    }),
                    // test if we can get object
                    next => s3.getObject(paramsObj1, next),
                    // test if we can list objects
                    next => s3.listObjects({ Bucket: `${vFormat}-bucket` }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.Contents.length, 2);
                        const keys = data.Contents.map(obj => obj.Key);
                        assert(keys.includes(`${vFormat}-object-1`));
                        assert(keys.includes(`${vFormat}-object-2`));
                        return next();
                    })
                ], done);
            });

            it(`Should perform operations on versioned bucket in ${vFormat} format`, done => {
                const paramsObj1 = {
                    Bucket: `${vFormat}-bucket`,
                    Key: `${vFormat}-object-1`
                };
                const paramsObj2 = {
                    Bucket: `${vFormat}-bucket`,
                    Key: `${vFormat}-object-2`
                };
                const versioningParams = {
                    Bucket: `${vFormat}-bucket`,
                    VersioningConfiguration: {
                     Status: 'Enabled',
                    }
                };
                const masterKey = vFormat === 'v0' ? `${vFormat}-object-1` : `\x7fM${vFormat}-object-1`;
                async.series([
                    next => s3.putBucketVersioning(versioningParams, next),
                    next => s3.putObject(paramsObj1, next),
                    next => s3.putObject(paramsObj1, next),
                    next => s3.putObject(paramsObj2, next),
                    // check if data stored in the correct version format
                    next => getObject(`${vFormat}-bucket`, masterKey, (err, doc) => {
                        assert.ifError(err);
                        assert.strictEqual(doc.key, `${vFormat}-object-1`);
                        return next();
                    }),
                    // test if we can get object
                    next => s3.getObject(paramsObj1, next),
                    // test if we can list objects
                    next => s3.listObjects({ Bucket: `${vFormat}-bucket` }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.Contents.length, 2);
                        const keys = data.Contents.map(obj => obj.Key);
                        assert(keys.includes(`${vFormat}-object-1`));
                        assert(keys.includes(`${vFormat}-object-2`));
                        return next();
                    }),
                    // test if we can list object versions
                    next => s3.listObjectVersions({ Bucket: `${vFormat}-bucket` }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.Versions.length, 3);
                        const versionPerObject = {};
                        data.Versions.forEach(version => {
                            versionPerObject[version.Key] = (versionPerObject[version.Key] || 0) + 1;
                        });
                        assert.strictEqual(versionPerObject[`${vFormat}-object-1`], 2);
                        assert.strictEqual(versionPerObject[`${vFormat}-object-2`], 1);
                        return next();
                    })
                ], done);
            });
        });
    });
});
