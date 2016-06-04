import { errors } from 'arsenal';
import fs from 'fs';
import Levelup from 'level';
import BucketInfo from '../BucketInfo';
import arsenal from 'arsenal';

const METADATA_PATH = '/metadata/';
const METASTORE = '__metastore';
const OPTIONS = { sync: false };

class BucketFileInterface {
    constructor() {
        this.dbs = [];
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

    loadMetastore(cb) {
        if (this.metastore === undefined) {
            this.metastore = new Levelup(METADATA_PATH + METASTORE, err => {
                if (err) {
                    return cb(errors.InternalError);
                }
                return cb(null);
            });
            return undefined;
        }
        return cb(null);
    }

    checkDbExists(bucketName, cb) {
        fs.stat(METADATA_PATH + bucketName, err => {
            if (err && err.errno === -2) {
                return cb(false);
            }
            return cb(true);
        });
    }

    /**
     * Load DB if exists
     * @param {String} bucketName - name of bucket
     * @param {Object} log - logger
     * @param {function} cb - callback
     * @return {function} cb - callback
     */
    loadDBIfExists(bucketName, log, cb) {
        if (this.dbs[bucketName] !== undefined) {
            return cb(null);
        }
        this.checkDbExists(bucketName, exists => {
            if (exists) {
                this.dbs[bucketName] =
                    new Levelup(METADATA_PATH + bucketName, err => {
                        if (err) {
                            return cb(errors.InternalError);
                        }
                        return cb(null);
                    });
            }
            return cb(errors.NoSuchBucket);
        });
        return undefined;
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.loadMetastore(err => {
            if (err) {
                return cb(err);
            }
            this.checkDbExists(bucketName, exists => {
                if (exists) {
                    return cb(errors.BucketAlreadyExists);
                }
                this.dbs[bucketName] =
                    new Levelup(METADATA_PATH + bucketName,
                                { errorIfExists: true },
                                err => {
                                    if (err) {
                                        return cb(errors.InternalError);
                                    }
                                    this.putBucketAttributes(bucketName,
                                                             bucketMD, log, cb);
                                    return undefined;
                                });
                return undefined;
            });
            return undefined;
        });
    }

    getBucketAttributes(bucketName, log, cb) {
        this.loadMetastore(err => {
            if (err) {
                return cb(err);
            }
            this.metastore.get(bucketName, (err, data) => {
                if (err && err.notFound) {
                    return cb(errors.NoSuchBucket);
                }
                return cb(null, BucketInfo.deSerialize(data));
            });
            return undefined;
        });
    }

    getBucketAndObject(bucketName, objName, log, cb) {
        this.loadMetastore(err => {
            if (err) {
                return cb(err);
            }
            this.getBucketAttributes(bucketName, log, (err, bucketAttr) => {
                if (err) {
                    return cb(err);
                }
                this.loadDBIfExists(bucketName, log, err => {
                    if (err) {
                        return cb(err);
                    }
                    this.dbs[bucketName].get(objName, (err, objAttr) => {
                        if (err) {
                            if (err.notFound) {
                                return cb(null, {
                                    bucket: bucketAttr.serialize(),
                                });
                            }
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
            });
            return undefined;
        });
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.loadMetastore(err => {
            if (err) {
                return cb(err);
            }
            this.metastore.put(bucketName, bucketMD.serialize(), OPTIONS, cb);
            return undefined;
        });
    }

    deleteBucket(bucketName, log, cb) {
        this.loadMetastore(err => {
            if (err) {
                return cb(err);
            }
            return cb(null);
        });
    }

    putObject(bucketName, objName, objVal, log, cb) {
        this.loadDBIfExists(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            this.dbs[bucketName].put(objName, JSON.stringify(objVal),
                                     OPTIONS, cb);
            return undefined;
        });
    }

    getObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            this.dbs[bucketName].get(objName, (err, data) => {
                if (err && err.notFound) {
                    return cb(errors.NoSuchObject);
                }
                return cb(null, JSON.parse(data));
            });
            return undefined;
        });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            this.dbs[bucketName].del(objName, OPTIONS, cb);
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
            extParams[name] = params[name];
        }
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
        let extParams = {};
        // multipart upload listing
        if (params.listingType === 'multipartuploads') {
            Ext = arsenal.listMPU.ListMultipartUploads;
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
            Ext = arsenal.delimiter.Delimiter;
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
        this.loadDBIfExists(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            const stream = this.dbs[bucketName].createReadStream(requestParams);
            stream
                .on('data', e => {
                    if (extension.filter(e) === false) {
                        stream.emit('end');
                        stream.destroy();
                    }
                })
                .on('error', () => {
                    const error = errors.InternalError;
                    cb(error);
                    log.error('error while listing objects', { error });
                })
                .on('end', () => {
                    const data = extension.result();
                    cb(null, data);
                    log.info('finished listing objects');
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
