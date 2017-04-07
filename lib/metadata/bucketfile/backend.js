import cluster from 'cluster';

import arsenal from 'arsenal';

import { logger } from '../../utilities/logger';
import BucketInfo from '../BucketInfo';
import constants from '../../../constants';
import config from '../../Config';

const errors = arsenal.errors;
const MetadataClient = arsenal.storage.metadata.client;

const METASTORE = '__metastore';
const OPTIONS = { sync: true };

class BucketFileInterface {

    constructor() {
        this.logger = logger;
        this.mdClient = new MetadataClient(
            { metadataHost: 'localhost',
              metadataPort: config.metadataDaemon.port,
              log: config.log });
        this.mdDB = this.mdClient.openDB();
        // the metastore sublevel is used to store bucket attributes
        this.metastore = this.mdDB.openSub(METASTORE);
        if (cluster.isMaster) {
            this.setupMetadataServer();
        }
    }

    setupMetadataServer() {
        /* Since the bucket creation API is expecting the
           usersBucket to have attributes, we pre-create the
           usersBucket attributes here */
        this.mdClient.logger.debug('setting up metadata server');
        const usersBucketAttr = new BucketInfo(constants.usersBucket,
            'admin', 'admin', new Date().toJSON(),
            BucketInfo.currentModelVersion());
        this.metastore.put(
            constants.usersBucket,
            usersBucketAttr.serialize(), err => {
                if (err) {
                    this.logger.fatal('error writing usersBucket ' +
                                      'attributes to metadata',
                                      { error: err });
                    throw (errors.InternalError);
                }
            });
    }

    /**
     * Load DB if exists
     * @param {String} bucketName - name of bucket
     * @param {Object} log - logger
     * @param {function} cb - callback(err, db, attr)
     * @return {undefined}
     */
    loadDBIfExists(bucketName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, attr) => {
            if (err) {
                return cb(err);
            }
            try {
                const db = this.mdDB.openSub(bucketName);
                return cb(null, db, attr);
            } catch (err) {
                return cb(errors.InternalError);
            }
        });
        return undefined;
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err && err !== errors.NoSuchBucket) {
                return cb(err);
            }
            if (err === undefined) {
                return cb(errors.BucketAlreadyExists);
            }
            this.putBucketAttributes(bucketName,
                                     bucketMD,
                                     log, cb);
            return undefined;
        });
    }

    getBucketAttributes(bucketName, log, cb) {
        this.metastore.get(bucketName, (err, data) => {
            if (err) {
                if (err.notFound) {
                    return cb(errors.NoSuchBucket);
                }
                const logObj = {
                    rawError: err,
                    error: err.message,
                    errorStack: err.stack,
                };
                log.error('error getting db attributes', logObj);
                return cb(errors.InternalError);
            }
            return cb(null, BucketInfo.deSerialize(data));
        });
        return undefined;
    }

    getBucketAndObject(bucketName, objName, params, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db, bucketAttr) => {
            if (err) {
                return cb(err);
            }
            db.get(objName, (err, objAttr) => {
                if (err) {
                    if (err.notFound) {
                        return cb(null, {
                            bucket: bucketAttr.serialize(),
                        });
                    }
                    const logObj = {
                        rawError: err,
                        error: err.message,
                        errorStack: err.stack,
                    };
                    log.error('error getting object', logObj);
                    return cb(errors.InternalError);
                }
                return cb(null, {
                    bucket: bucketAttr.serialize(),
                    obj: objAttr,
                });
            });
            return undefined;
        });
        return undefined;
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.metastore.put(bucketName, bucketMD.serialize(),
                           OPTIONS,
                           err => {
                               if (err) {
                                   const logObj = {
                                       rawError: err,
                                       error: err.message,
                                       errorStack: err.stack,
                                   };
                                   log.error('error putting db attributes',
                                             logObj);
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    deleteBucket(bucketName, log, cb) {
        this.metastore.del(bucketName,
                           err => {
                               if (err) {
                                   const logObj = {
                                       rawError: err,
                                       error: err.message,
                                       errorStack: err.stack,
                                   };
                                   log.error('error deleting bucket',
                                             logObj);
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    putObject(bucketName, objName, objVal, params, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.put(objName, JSON.stringify(objVal),
                   OPTIONS, err => {
                       // TODO: implement versioning for file backend
                       const data = undefined;
                       if (err) {
                           const logObj = {
                               rawError: err,
                               error: err.message,
                               errorStack: err.stack,
                           };
                           log.error('error putting object',
                                     logObj);
                           return cb(errors.InternalError);
                       }
                       return cb(err, data);
                   });
            return undefined;
        });
    }

    getObject(bucketName, objName, params, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.get(objName, (err, data) => {
                if (err) {
                    if (err.notFound) {
                        return cb(errors.NoSuchKey);
                    }
                    const logObj = {
                        rawError: err,
                        error: err.message,
                        errorStack: err.stack,
                    };
                    log.error('error getting object', logObj);
                    return cb(errors.InternalError);
                }
                return cb(null, JSON.parse(data));
            });
            return undefined;
        });
    }

    deleteObject(bucketName, objName, params, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.del(objName, OPTIONS, err => {
                if (err) {
                    const logObj = {
                        rawError: err,
                        error: err.message,
                        errorStack: err.stack,
                    };
                    log.error('error deleting object', logObj);
                    return cb(errors.InternalError);
                }
                return cb();
            });
            return undefined;
        });
    }

    /**
     *  This complex function deals with different extensions of bucket listing:
     *  Delimiter based search or MPU based search.
     *  @param {String} bucketName - The name of the bucket to list
     *  @param {Object} params - The params to search
     *  @param {Object} log - The logger object
     *  @param {function} cb - Callback when done
     *  @return {undefined}
     */
    internalListObject(bucketName, params, log, cb) {
        const extName = params.listingType || 'Basic';
        const extension = new arsenal.algorithms.list[extName](params, log);
        const requestParams = extension.genMDParams();
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            let cbDone = false;
            db.createReadStream(requestParams, (err, stream) => {
                if (err) {
                    return cb(err);
                }
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
            });
            return undefined;
        });
    }

    listObject(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }

    listMultipartUploads(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }
}

export default BucketFileInterface;
