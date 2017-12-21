/*
 * we assume good default setting of write concern is good for all
 * bulk writes. Note that bulk writes are not transactions but ordered
 * writes. They may fail in between. To some extend those situations
 * may generate orphans but not alter the proper conduct of operations
 * (what he user wants and what we acknowledge to the user).
 *
 * Orphan situations may be recovered by the Lifecycle.
 *
 * We use proper atomic operations when needed.
 */
const async = require('async');
const arsenal = require('arsenal');

const logger = require('../../utilities/logger');

const constants = require('../../../constants');
const { config } = require('../../Config');

const errors = arsenal.errors;
const versioning = arsenal.versioning;
const BucketInfo = arsenal.models.BucketInfo;

const MongoClient = require('mongodb').MongoClient;
const Uuid = require('uuid');
const diskusage = require('diskusage');

const genVID = versioning.VersionID.generateVersionId;

const MongoReadStream = require('./readStream');
const MongoUtils = require('./utils');

const USERSBUCKET = '__usersbucket';
const METASTORE = '__metastore';
const INFOSTORE = '__infostore';
const __UUID = 'uuid';
const ASYNC_REPAIR_TIMEOUT = 15000;

let uidCounter = 0;

const VID_SEP = versioning.VersioningConstants.VersionId.Separator;

function generateVersionId() {
    // generate a unique number for each member of the nodejs cluster
    return genVID(`${process.pid}.${uidCounter++}`,
                  config.replicationGroupId);
}

function formatVersionKey(key, versionId) {
    return `${key}${VID_SEP}${versionId}`;
}

function inc(str) {
    return str ? (str.slice(0, str.length - 1) +
            String.fromCharCode(str.charCodeAt(str.length - 1) + 1)) : str;
}

const VID_SEPPLUS = inc(VID_SEP);

function generatePHDVersion(versionId) {
    return {
        isPHD: true,
        versionId,
    };
}

class MongoClientInterface {
    constructor() {
        const mongoUrl =
              `mongodb://${config.mongodb.host}:${config.mongodb.port}`;
        this.logger = logger;
        this.client = null;
        this.db = null;
        this.logger.debug(`connecting to ${mongoUrl}`);
        // FIXME: constructors shall not have side effect so there
        // should be an async_init(cb) method in the wrapper to
        // initialize this backend
        MongoClient.connect(mongoUrl, (err, client) => {
            if (err) {
                throw (errors.InternalError);
            }
            this.logger.debug('connected to mongodb');
            this.client = client;
            this.db = client.db(config.mongodb.database, {
                ignoreUndefined: true,
            });
            this.usersBucketHack();
        });
    }

    usersBucketHack() {
        /* FIXME: Since the bucket creation API is expecting the
           usersBucket to have attributes, we pre-create the
           usersBucket attributes here (see bucketCreation.js line
           36)*/
        const usersBucketAttr = new BucketInfo(constants.usersBucket,
            'admin', 'admin', new Date().toJSON(),
            BucketInfo.currentModelVersion());
        this.createBucket(
            constants.usersBucket,
            usersBucketAttr, {}, err => {
                if (err) {
                    this.logger.fatal('error writing usersBucket ' +
                                      'attributes to metastore',
                                      { error: err });
                    throw (errors.InternalError);
                }
            });
    }

    getCollection(name) {
        /* mongo has a problem with .. in collection names */
        const newName = (name === constants.usersBucket) ?
              USERSBUCKET : name;
        return this.db.collection(newName);
    }

    createBucket(bucketName, bucketMD, log, cb) {
        // FIXME: there should be a version of BucketInfo.serialize()
        // that does not JSON.stringify()
        const bucketInfo = BucketInfo.fromObj(bucketMD);
        const bucketMDStr = bucketInfo.serialize();
        const newBucketMD = JSON.parse(bucketMDStr);
        const m = this.getCollection(METASTORE);
        // we don't have to test bucket existence here as it is done
        // on the upper layers
        m.update({
            _id: bucketName,
        }, {
            _id: bucketName,
            value: newBucketMD,
        }, {
            upsert: true,
        }, () => cb());
    }

    getBucketAttributes(bucketName, log, cb) {
        const m = this.getCollection(METASTORE);
        m.findOne({
            _id: bucketName,
        }, {}, (err, doc) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (!doc) {
                return cb(errors.NoSuchBucket);
            }
            // FIXME: there should be a version of BucketInfo.deserialize()
            // that properly inits w/o JSON.parse()
            const bucketMDStr = JSON.stringify(doc.value);
            const bucketMD = BucketInfo.deSerialize(bucketMDStr);
            return cb(null, bucketMD);
        });
    }

    getBucketAndObject(bucketName, objName, params, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            this.getObject(bucketName, objName, params, log, (err, obj) => {
                if (err) {
                    if (err === errors.NoSuchKey) {
                        return cb(null,
                                  { bucket:
                                    BucketInfo.fromObj(bucket).serialize(),
                                  });
                    }
                    return cb(err);
                }
                return cb(null, {
                    bucket: BucketInfo.fromObj(bucket).serialize(),
                    obj: JSON.stringify(obj),
                });
            });
            return undefined;
        });
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        // FIXME: there should be a version of BucketInfo.serialize()
        // that does not JSON.stringify()
        const bucketInfo = BucketInfo.fromObj(bucketMD);
        const bucketMDStr = bucketInfo.serialize();
        const newBucketMD = JSON.parse(bucketMDStr);
        const m = this.getCollection(METASTORE);
        m.update({
            _id: bucketName,
        }, {
            _id: bucketName,
            value: newBucketMD,
        }, {
            upsert: true,
        }, () => cb());
    }

    /*
     * Delete bucket from metastore
     */
    deleteBucketStep2(bucketName, log, cb) {
        const m = this.getCollection(METASTORE);
        m.findOneAndDelete({
            _id: bucketName,
        }, {}, (err, result) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (result.ok !== 1) {
                return cb(errors.InternalError);
            }
            return cb(null);
        });
    }

    /*
     * Drop the bucket then process to step 2. Checking
     * the count is already done by the upper layer. We don't need to be
     * atomic because the call is protected by a delete_pending flag
     * in the upper layer.
     * 2 cases here:
     * 1) the collection may not yet exist (or being already dropped
     * by a previous call)
     * 2) the collection may exist.
     */
    deleteBucket(bucketName, log, cb) {
        const c = this.getCollection(bucketName);
        c.drop({}, err => {
            if (err) {
                if (err.codeName === 'NamespaceNotFound') {
                    return this.deleteBucketStep2(bucketName, log, cb);
                }
                return cb(errors.InternalError);
            }
            return this.deleteBucketStep2(bucketName, log, cb);
        });
    }

    /*
     * In this case we generate a versionId and
     * sequentially create the object THEN update the master
     */
    putObjectVerCase1(c, bucketName, objName, objVal, params, log, cb) {
        const versionId = generateVersionId();
        // eslint-disable-next-line
        objVal.versionId = versionId;
        const vObjName = formatVersionKey(objName, versionId);
        c.bulkWrite([{
            updateOne: {
                filter: {
                    _id: vObjName,
                },
                update: {
                    _id: vObjName, value: objVal,
                },
                upsert: true,
            },
        }, {
            updateOne: {
                filter: {
                    _id: objName,
                },
                update: {
                    _id: objName, value: objVal,
                },
                upsert: true,
            },
        }], {
            ordered: 1,
        }, () => cb(null, `{"versionId": "${versionId}"}`));
    }

    /*
     * Case used when versioning has been disabled after objects
     * have been created with versions
     */
    putObjectVerCase2(c, bucketName, objName, objVal, params, log, cb) {
        const versionId = generateVersionId();
        // eslint-disable-next-line
        objVal.versionId = versionId;
        c.update({
            _id: objName,
        }, {
            _id: objName,
            value: objVal,
        }, {
            upsert: true,
        }, () => cb(null, `{"versionId": "${objVal.versionId}"}`));
    }

    /*
     * In this case the aller provides a versionId. This function will
     * sequentially update the object with given versionId THEN the
     * master iff the provided versionId matches the one of the master
     */
    putObjectVerCase3(c, bucketName, objName, objVal, params, log, cb) {
        // eslint-disable-next-line
        objVal.versionId = params.versionId;
        const vObjName = formatVersionKey(objName, params.versionId);
        c.bulkWrite([{
            updateOne: {
                filter: {
                    _id: vObjName,
                },
                update: {
                    _id: vObjName, value: objVal,
                },
                upsert: true,
            },
        }, {
            updateOne: {
                // eslint-disable-next-line
                filter: {
                    _id: objName,
                    'value.versionId': params.versionId,
                },
                update: {
                    _id: objName, value: objVal,
                },
                upsert: true,
            },
        }], {
            ordered: 1,
        }, () => cb(null, `{"versionId": "${objVal.versionId}"}`));
    }

    /*
     * Put object when versioning is not enabled
     */
    putObjectNoVer(c, bucketName, objName, objVal, params, log, cb) {
        c.update({
            _id: objName,
        }, {
            _id: objName,
            value: objVal,
        }, {
            upsert: true,
        }, () => cb());
    }

    putObject(bucketName, objName, objVal, params, log, cb) {
        MongoUtils.serialize(objVal);
        const c = this.getCollection(bucketName);
        if (params && params.versioning) {
            return this.putObjectVerCase1(c, bucketName, objName, objVal,
                                          params, log, cb);
        } else if (params && params.versionId === '') {
            return this.putObjectVerCase2(c, bucketName, objName, objVal,
                                          params, log, cb);
        } else if (params && params.versionId) {
            return this.putObjectVerCase3(c, bucketName, objName, objVal,
                                          params, log, cb);
        }
        return this.putObjectNoVer(c, bucketName, objName, objVal,
                                   params, log, cb);
    }

    getObject(bucketName, objName, params, log, cb) {
        const c = this.getCollection(bucketName);
        if (params && params.versionId) {
            // eslint-disable-next-line
            objName = formatVersionKey(objName, params.versionId);
        }
        c.findOne({
            _id: objName,
        }, {}, (err, doc) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (!doc) {
                return cb(errors.NoSuchKey);
            }
            if (doc.value.isPHD) {
                this.getLatestVersion(c, objName, log, (err, value) => {
                    if (err) {
                        log.error('getting latest version', err);
                        return cb(err);
                    }
                    return cb(null, value);
                });
                return undefined;
            }
            MongoUtils.unserialize(doc.value);
            return cb(null, doc.value);
        });
    }

    /*
     * This function return the latest version
     */
    getLatestVersion(c, objName, log, cb) {
        c.find({
            _id: {
                $gt: objName,
                $lt: `${objName}${VID_SEPPLUS}`,
            },
        }, {}).
            sort({
                _id: 1,
            }).
            limit(1).
            toArray(
                (err, keys) => {
                    if (err) {
                        return cb(errors.InternalError);
                    }
                    if (keys.length === 0) {
                        return cb(errors.NoSuchKey);
                    }
                    MongoUtils.unserialize(keys[0].value);
                    return cb(null, keys[0].value);
                });
    }

    /*
     * repair the master with a new value. There can be
     * race-conditions or legit updates so place an atomic condition
     * on PHD flag and mst version.
     */
    repair(c, objName, objVal, mst, log, cb) {
        MongoUtils.serialize(objVal);
        // eslint-disable-next-line
        c.findOneAndReplace({
            _id: objName,
            'value.isPHD': true,
            'value.versionId': mst.versionId,
        }, {
            _id: objName,
            value: objVal,
        }, {
            upsert: true,
        }, (err, result) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (result.ok !== 1) {
                return cb(errors.InternalError);
            }
            return cb(null);
        });
    }

    /*
     * Get the latest version and repair. The process is safe because
     * we never replace a non-PHD master
     */
    asyncRepair(c, objName, mst, log) {
        this.getLatestVersion(c, objName, log, (err, value) => {
            if (err) {
                log.error('async-repair: getting latest version', err);
                return undefined;
            }
            this.repair(c, objName, value, mst, log, err => {
                if (err) {
                    log.error('async-repair failed', err);
                    return undefined;
                }
                log.debug('async-repair success');
                return undefined;
            });
            return undefined;
        });
    }

    /*
     * the master is a PHD so we try to see if it is the latest of its
     * kind to get rid of it, otherwise we asynchronously repair it
     */
    deleteOrRepairPHD(c, bucketName, objName, mst, log, cb) {
        // Check if there are other versions available
        this.getLatestVersion(c, objName, log, err => {
            if (err) {
                if (err === errors.NoSuchKey) {
                    // We try to delete the master. A race condition
                    // is possible here: another process may recreate
                    // a master or re-delete it in between so place an
                    // atomic condition on the PHD flag and the mst
                    // version:
                    // eslint-disable-next-line
                    c.findOneAndDelete({
                        _id: objName,
                        'value.isPHD': true,
                        'value.versionId': mst.versionId,
                    }, {}, err => {
                        if (err) {
                            return cb(errors.InternalError);
                        }
                        // do not test result.ok === 1 because
                        // both cases are expected
                        return cb(null);
                    });
                    return undefined;
                }
                return cb(err);
            }
            // We have other versions available so repair:
            setTimeout(() => {
                this.asyncRepair(c, objName, mst, log);
            }, ASYNC_REPAIR_TIMEOUT);
            return cb(null);
        });
    }

    /*
     * Delete object when versioning is enabled and the version is
     * master. In this case we sequentially update the master with a
     * PHD flag (placeholder) and a unique non-existing version THEN
     * we delete the specified versioned object. THEN we try to delete
     * or repair the PHD we just created
     */
    deleteObjectVerMaster(c, bucketName, objName, params, log, cb) {
        const vObjName = formatVersionKey(objName, params.versionId);
        const _vid = generateVersionId();
        const mst = generatePHDVersion(_vid);
        c.bulkWrite([{
            updateOne: {
                filter: {
                    _id: objName,
                },
                update: {
                    _id: objName, value: mst,
                },
                upsert: true,
            },
        }, {
            deleteOne: {
                filter: {
                    _id: vObjName,
                },
            },
        }], {
            ordered: 1,
        }, () => this.deleteOrRepairPHD(c, bucketName, objName, mst, log, cb));
    }

    /*
     * Delete object when versioning is enabled and the version is
     * not master. It is a straight-forward atomic delete
     */
    deleteObjectVerNotMaster(c, bucketName, objName, params, log, cb) {
        const vObjName = formatVersionKey(objName, params.versionId);
        c.findOneAndDelete({
            _id: vObjName,
        }, {}, (err, result) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (result.ok !== 1) {
                return cb(errors.InternalError);
            }
            return cb(null);
        });
    }

    /*
     * Delete object when versioning is enabled. We first find the
     * master, if it is already a PHD we have a special processing,
     * then we check if it matches the master versionId in such case
     * we will create a PHD, otherwise we delete it
     */
    deleteObjectVer(c, bucketName, objName, params, log, cb) {
        c.findOne({
            _id: objName,
        }, {}, (err, mst) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (!mst) {
                return cb(errors.NoSuchKey);
            }
            if (mst.value.isPHD ||
                mst.value.versionId === params.versionId) {
                return this.deleteObjectVerMaster(c, bucketName, objName,
                                                  params, log, cb);
            }
            return this.deleteObjectVerNotMaster(c, bucketName, objName,
                                                 params, log, cb);
        });
    }

    /*
     * Atomically delete an object when versioning is not enabled
     */
    deleteObjectNoVer(c, bucketName, objName, params, log, cb) {
        c.findOneAndDelete({
            _id: objName,
        }, {}, (err, result) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (result.ok !== 1) {
                return cb(errors.InternalError);
            }
            return cb(null);
        });
    }

    deleteObject(bucketName, objName, params, log, cb) {
        const c = this.getCollection(bucketName);
        if (params && params.versionId) {
            return this.deleteObjectVer(c, bucketName, objName,
                                        params, log, cb);
        }
        return this.deleteObjectNoVer(c, bucketName, objName,
                                      params, log, cb);
    }

    internalListObject(bucketName, params, log, cb) {
        const extName = params.listingType;
        const extension = new arsenal.algorithms.list[extName](params, log);
        const requestParams = extension.genMDParams();
        const c = this.getCollection(bucketName);
        let cbDone = false;
        const stream = new MongoReadStream(c, requestParams);
        stream
            .on('data', e => {
                if (extension.filter(e) < 0) {
                    stream.emit('end');
                    stream.destroy();
                }
            })
            .on('error', err => {
                if (!cbDone) {
                    cbDone = true;
                    const logObj = {
                        rawError: err,
                        error: err.message,
                        errorStack: err.stack,
                    };
                    log.error('error listing objects', logObj);
                    cb(errors.InternalError);
                }
            })
            .on('end', () => {
                if (!cbDone) {
                    cbDone = true;
                    const data = extension.result();
                    cb(null, data);
                }
            });
        return undefined;
    }

    listObject(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }

    listMultipartUploads(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }

    readUUID(log, cb) {
        const i = this.getCollection(INFOSTORE);
        i.findOne({
            _id: __UUID,
        }, {}, (err, doc) => {
            if (err) {
                return cb(errors.InternalError);
            }
            if (!doc) {
                return cb(errors.NoSuchKey);
            }
            return cb(null, doc.value);
        });
    }

    writeUUIDIfNotExists(uuid, log, cb) {
        const i = this.getCollection(INFOSTORE);
        i.insert({
            _id: __UUID,
            value: uuid,
        }, {}, err => {
            if (err) {
                if (err.code === 11000) {
                    // duplicate key error
                    // FIXME: define a KeyAlreadyExists error in Arsenal
                    return cb(errors.EntityAlreadyExists);
                }
                return cb(errors.InternalError);
            }
            // FIXME: shoud we check for result.ok === 1 ?
            return cb(null);
        });
    }

    /*
     * we always try to generate a new UUID in order to be atomic in
     * case of concurrency. The write will fail if it already exists.
     */
    getUUID(log, cb) {
        const uuid = Uuid.v4();
        this.writeUUIDIfNotExists(uuid, log, err => {
            if (err) {
                if (err === errors.InternalError) {
                    return cb(err);
                }
                return this.readUUID(log, cb);
            }
            return cb(null, uuid);
        });
    }

    getDiskUsage(cb) {
        // FIXME: for basic one server deployment the infrastructure
        // configurator shall set a path to the actual MongoDB volume.
        // For Kub/cluster deployments there should be a more sophisticated
        // way for guessing free space.
        diskusage.check(config.mongodb.path !== undefined ?
                        config.mongodb.path : '/', cb);
    }

    countItems(log, cb) {
        const res = {
            objects: 0,
            versions: 0,
            buckets: 0,
        };
        this.db.listCollections().toArray((err, collInfos) => {
            async.eachLimit(collInfos, 10, (value, next) => {
                if (value.name === METASTORE ||
                    value.name === INFOSTORE ||
                    value.name === USERSBUCKET) {
                    // skip
                    return next();
                }
                res.buckets++;
                const c = this.getCollection(value.name);
                // FIXME: there is currently no way of distinguishing
                // master from versions and searching for VID_SEP
                // does not work because there cannot be null bytes
                // in $regex
                c.count({
                    // eslint-disable-next-line
                    'value.versionId': {
                        '$exists': false,
                    },
                }, {}, (err, result) => {
                    if (err) {
                        return next(errors.InternalError);
                    }
                    res.objects += result;
                    c.count({
                        // eslint-disable-next-line
                        'value.versionId': {
                            '$exists': true,
                        },
                    }, {}, (err, result) => {
                        if (err) {
                            return next(errors.InternalError);
                        }
                        res.versions += result;
                        return next();
                    });
                    return undefined;
                });
                return undefined;
            }, err => {
                if (err) {
                    return cb(err);
                }
                return cb(null, res);
            });
        });
    }
}

module.exports = MongoClientInterface;
