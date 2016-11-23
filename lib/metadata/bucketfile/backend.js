import net from 'net';
import fs from 'fs';
import cluster from 'cluster';
import events from 'events';
import assert from 'assert';

import level from 'level';
import multilevel from 'multilevel';
import sublevel from 'level-sublevel';
import arsenal from 'arsenal';

import { logger } from '../../utilities/logger';
import BucketInfo from '../BucketInfo';
import constants from '../../../constants';
import config from '../../Config';

const errors = arsenal.errors;

const METADATA_PORT = 9990;
const METADATA_PATH = `${config.filePaths.metadataPath}/`;
const MANIFEST_JSON = 'manifest.json';
const MANIFEST_JSON_TMP = 'manifest.json.tmp';
const ROOT_DB = 'rootDB';
const METASTORE = '__metastore';
const OPTIONS = { sync: true };

class BucketFileInterface {

    constructor() {
        this.logger = logger;
        if (cluster.isMaster) {
            return this.startServer();
        }
        this.refcnt = 0;
        this.waitreco = 0;
        this.realReConnect();
        this.notifier = new events.EventEmitter();
        this.recoCbDone = false;
        /*
        * We need to wait for somebody to wake us up either by
        * recodone or refdecr
        */
        this.notifier.on('recodone', cb => {
            if (this.recoCbDone) {
                return undefined;
            }
            this.recoCbDone = true;
            return process.nextTick(() => cb());
        });
        this.notifier.on('refdecr', () => {
            /*
            * We need to recheck for the condition as
            * somebody might issue a command before we
            * get the notification
            */
            if (this.recoCbDone) {
                return undefined;
            }
            this.recoCbDone = true;
            return process.nextTick(() => this.reConnect(this.reConnectCb));
        });
        return this;
    }

    /**
     * Start the server
     * @return {undefined}
     */
    startServer() {
        const rootDB = level(METADATA_PATH + ROOT_DB);
        const sub = sublevel(rootDB);
        sub.methods = sub.methods || {};
        sub.methods.createSub = { type: 'async' };
        sub.createSub = (subName, cb) => {
            try {
                sub.sublevel(subName);
                multilevel.writeManifest(sub,
                                         METADATA_PATH +
                                         MANIFEST_JSON_TMP);
                fs.renameSync(METADATA_PATH + MANIFEST_JSON_TMP,
                              METADATA_PATH + MANIFEST_JSON);
                cb();
            } catch (err) {
                cb(err);
            }
        };
        const metastore = sub.sublevel(METASTORE);
        /* Since the bucket creation API is expecting the
           usersBucket to have attributes, we pre-create the
           usersBucket here */
        sub.sublevel(constants.usersBucket);
        const usersBucketAttr = new BucketInfo(constants.usersBucket,
            'admin', 'admin', new Date().toJSON(),
            BucketInfo.currentModelVersion());
        metastore.put(constants.usersBucket, usersBucketAttr.serialize());
        const stream = metastore.createKeyStream();
        stream
            .on('data', e => {
                // automatically recreate existing sublevels
                sub.sublevel(e);
            })
            .on('error', err => {
                this.logger.fatal('error listing metastore', { error: err });
                throw (errors.InternalError);
            })
            .on('end', () => {
                multilevel.writeManifest(sub, METADATA_PATH + MANIFEST_JSON);
                this.logger.info('starting metadata file backend server');
                /* We start a server that will server the sublevel
                   capable rootDB to clients */
                net.createServer(con => {
                    con.pipe(multilevel.server(sub)).pipe(con);
                }).listen(METADATA_PORT);
            });
    }

    /**
     * Reconnect to the server
     * @return {undefined}
     */
    realReConnect() {
        if (this.client !== undefined) {
            this.client.close();
        }
        delete require.cache[require.resolve(METADATA_PATH + MANIFEST_JSON)];
        const manifest = require(METADATA_PATH + MANIFEST_JSON);
        this.client = multilevel.client(manifest);
        const con = net.connect(METADATA_PORT);
        con.pipe(this.client.createRpcStream()).pipe(con);
        this.metastore = this.client.sublevel(METASTORE);
    }

    /**
     * Wait for reconnect do be done
     * @param {function} cb - callback()
     * @return {undefined}
     */
    reConnect(cb) {
        this.reConnectCb = cb;
        if (this.refcnt === this.waitreco) {
            /* Either we are alone waiting for reconnect or all
               operations are waiting for reconnect, then force
               a reco and notify others */
            this.realReConnect();
            if (this.waitreco > 1) {
                this.notifier.emit('recodone', cb);
            }
            return cb();
        }
        this.recoCbDone = false;
        return undefined;
    }

    /**
     * Take a reference on the client
     * @return {undefined}
     */
    ref() {
        this.refcnt++;
    }

    /**
     * Unreference the client
     * @return {undefined}
     */
    unRef() {
        this.refcnt--;
        assert(this.refcnt >= 0);
        if (this.waitreco > 0) {
            /* give a change to wake up waiters */
            this.notifier.emit('refdecr', () => {});
        }
    }

    /**
     * Load DB if exists
     * @param {String} bucketName - name of bucket
     * @param {Object} log - logger
     * @param {function} cb - callback(err, db, attr)
     * @return {undefined}
     */
    loadDBIfExistsNoRef(bucketName, log, cb) {
        this.getBucketAttributesNoRef(bucketName, log, (err, attr) => {
            if (err) {
                return cb(err, null);
            }
            let db;
            try {
                db = this.client.sublevel(bucketName);
                return cb(null, db, attr);
            } catch (err) {
                /* if the bucket is newly created the
                   client cannot create sublevels without
                   re-reading the manifest */
                this.waitreco++;
                this.reConnect(() => {
                    this.waitreco--;
                    try {
                        db = this.client.sublevel(bucketName);
                    } catch (err) {
                        log.error('cannot make sublevel usable',
                                  { error: err.stack });
                        return cb(errors.InternalError, null);
                    }
                    return cb(null, db, attr);
                });
                return undefined;
            }
        });
        return undefined;
    }

    loadDBIfExists(bucketName, log, cb) {
        this.ref();
        this.loadDBIfExistsNoRef(bucketName, log, (err, db, attr) => {
            if (err) {
                this.unRef();
                return cb(err);
            }
            // we hold a ref here
            return cb(err, db, attr);
        });
        return undefined;
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.ref();
        this.getBucketAttributesNoRef(bucketName, log, err => {
            if (err && err !== errors.NoSuchBucket) {
                this.unRef();
                return cb(err);
            }
            if (err === undefined) {
                this.unRef();
                return cb(errors.BucketAlreadyExists);
            }
            this.client.createSub(bucketName, err => {
                if (err) {
                    log.error('error creating sublevel', { error: err });
                    this.unRef();
                    return cb(errors.InternalError);
                }
                // we hold a ref here
                this.putBucketAttributesNoRef(bucketName,
                                              bucketMD,
                                              log,
                                              err => {
                                                  this.unRef();
                                                  return cb(err);
                                              });
                return undefined;
            });
            return undefined;
        });
        return undefined;
    }

    getBucketAttributesNoRef(bucketName, log, cb) {
        this.metastore.get(bucketName, (err, data) => {
            if (err) {
                if (err.notFound) {
                    return cb(errors.NoSuchBucket);
                }
                log.error('error getting db attributes',
                          { error: err });
                return cb(errors.InternalError, null);
            }
            return cb(null, BucketInfo.deSerialize(data));
        });
        return undefined;
    }

    getBucketAttributes(bucketName, log, cb) {
        this.ref();
        this.getBucketAttributesNoRef(bucketName, log,
                                      (err, data) => {
                                          this.unRef();
                                          return cb(err, data);
                                      });
        return undefined;
    }

    getBucketAndObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db, bucketAttr) => {
            if (err) {
                return cb(err);
            }
            db.get(objName, (err, objAttr) => {
                this.unRef();
                if (err) {
                    if (err.notFound) {
                        return cb(null, {
                            bucket: bucketAttr.serialize(),
                        });
                    }
                    log.error('error getting object', { error: err });
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

    putBucketAttributesNoRef(bucketName, bucketMD, log, cb) {
        this.metastore.put(bucketName, bucketMD.serialize(),
                           OPTIONS,
                           err => {
                               if (err) {
                                   log.error('error putting db attributes',
                                             { error: err });
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.ref();
        this.putBucketAttributesNoRef(bucketName, bucketMD, log,
                                      err => {
                                          this.unRef();
                                          return cb(err);
                                      });
        return undefined;
    }

    deleteBucket(bucketName, log, cb) {
        // we could remove bucket from manifest but it is not a problem
        this.ref();
        this.metastore.del(bucketName,
                           err => {
                               this.unRef();
                               if (err) {
                                   log.error('error deleting bucket',
                                             { error: err });
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    putObject(bucketName, objName, objVal, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.put(objName, JSON.stringify(objVal),
                   OPTIONS, err => {
                       this.unRef();
                       if (err) {
                           log.error('error putting object',
                                     { error: err });
                           return cb(errors.InternalError);
                       }
                       return cb();
                   });
            return undefined;
        });
    }

    getObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.get(objName, (err, data) => {
                this.unRef();
                if (err) {
                    if (err.notFound) {
                        return cb(errors.NoSuchKey);
                    }
                    log.error('error getting object',
                              { error: err });
                    return cb(errors.InternalError);
                }
                return cb(null, JSON.parse(data));
            });
            return undefined;
        });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.del(objName, OPTIONS, err => {
                this.unRef();
                if (err) {
                    log.error('error deleting object',
                              { error: err });
                    return cb(errors.InternalError);
                }
                return cb();
            });
            return undefined;
        });
    }

    /**
     *  This function checks if params have a property name
     *  If there is add it to the finalParams
     *  Else do nothing
     *  @param {String} name - The parameter name
     *  @param {Object} params - The params to search
     *  @param {Object} extParams - The params sent to the extension
     *  @return {undefined}
     */
    addExtensionParam(name, params, extParams) {
        if (params.hasOwnProperty(name)) {
            // eslint-disable-next-line no-param-reassign
            extParams[name] = params[name];
        }
    }

    /**
     * Used for advancing the last character of a string for setting upper/lower
     * bounds
     * For e.g., _setCharAt('demo1') results in 'demo2',
     * _setCharAt('scality') results in 'scalitz'
     * @param {String} str - string to be advanced
     * @return {String} - modified string
     */
    _setCharAt(str) {
        let chr = str.charCodeAt(str.length - 1);
        chr = String.fromCharCode(chr + 1);
        return str.substr(0, str.length - 1) + chr;
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
        const requestParams = {};
        let Ext;
        const extParams = {};
        // multipart upload listing
        if (params.listingType === 'multipartuploads') {
            Ext = arsenal.algorithms.list.MPU;
            this.addExtensionParam('queryPrefixLength', params, extParams);
            this.addExtensionParam('splitter', params, extParams);
            if (params.keyMarker) {
                requestParams.gt = `overview${params.splitter}` +
                    `${params.keyMarker}${params.splitter}`;
                if (params.uploadIdMarker) {
                    requestParams.gt += `${params.uploadIdMarker}`;
                }
                // advance so that lower bound does not include the supplied
                // markers
                requestParams.gt = this._setCharAt(requestParams.gt);
            }
        } else {
            Ext = arsenal.algorithms.list.Delimiter;
            if (params.marker) {
                requestParams.gt = params.marker;
                this.addExtensionParam('gt', requestParams, extParams);
            }
        }
        this.addExtensionParam('delimiter', params, extParams);
        this.addExtensionParam('maxKeys', params, extParams);
        if (params.prefix) {
            requestParams.start = params.prefix;
            requestParams.lt = this._setCharAt(params.prefix);
            this.addExtensionParam('start', requestParams, extParams);
        }
        const extension = new Ext(extParams, log);
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            let cbDone = false;
            const stream = db.createReadStream(requestParams);
            stream
                .on('data', e => {
                    if (extension.filter(e) === false) {
                        stream.emit('end');
                        stream.destroy();
                    }
                })
                .on('error', err => {
                    if (!cbDone) {
                        this.unRef();
                        cbDone = true;
                        log.error('error listing objects',
                                  { error: err });
                        cb(errors.InternalError);
                    }
                })
                .on('end', () => {
                    if (!cbDone) {
                        this.unRef();
                        cbDone = true;
                        const data = extension.result();
                        cb(null, data);
                    }
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
