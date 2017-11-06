const assert = require('assert');
const bucketclient = require('bucketclient');

const BucketInfo = require('../BucketInfo');
const logger = require('../../utilities/logger');
const { config } = require('../../Config');

class BucketClientInterface {
    constructor() {
        assert(config.bucketd.bootstrap.length > 0,
               'bucketd bootstrap list is empty');
        const { bootstrap, log } = config.bucketd;
        if (config.https) {
            const { key, cert, ca } = config.https;
            logger.info('bucketclient configuration', {
                bootstrap,
                log,
                https: true,
            });
            this.client = new bucketclient.RESTClient(bootstrap, log, true,
                key, cert, ca);
        } else {
            logger.info('bucketclient configuration', {
                bootstrap,
                log,
                https: false,
            });
            this.client = new bucketclient.RESTClient(bootstrap, log);
        }
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.client.createBucket(bucketName, log.getSerializedUids(),
                                 bucketMD.serialize(), cb);
        return null;
    }

    getBucketAttributes(bucketName, log, cb) {
        this.client.getBucketAttributes(bucketName, log.getSerializedUids(),
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, BucketInfo.deSerialize(data));
            });
        return null;
    }

    getBucketAndObject(bucketName, objName, params, log, cb) {
        this.client.getBucketAndObject(bucketName, objName,
            log.getSerializedUids(), (err, data) => {
                if (err && (!err.NoSuchKey && !err.ObjNotFound)) {
                    return cb(err);
                }
                return cb(null, JSON.parse(data));
            }, params);
        return null;
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.client.putBucketAttributes(bucketName, log.getSerializedUids(),
                                        bucketMD.serialize(), cb);
        return null;
    }

    deleteBucket(bucketName, log, cb) {
        this.client.deleteBucket(bucketName, log.getSerializedUids(), cb);
        return null;
    }

    putObject(bucketName, objName, objVal, params, log, cb) {
        this.client.putObject(bucketName, objName, JSON.stringify(objVal),
            log.getSerializedUids(), cb, params);
        return null;
    }

    getObject(bucketName, objName, params, log, cb) {
        this.client.getObject(bucketName, objName, log.getSerializedUids(),
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, JSON.parse(data));
            }, params);
        return null;
    }

    deleteObject(bucketName, objName, params, log, cb) {
        this.client.deleteObject(bucketName, objName, log.getSerializedUids(),
                                 cb, params);
        return null;
    }

    listObject(bucketName, params, log, cb) {
        this.client.listObject(bucketName, log.getSerializedUids(), params,
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, JSON.parse(data));
            });
        return null;
    }

    listMultipartUploads(bucketName, params, log, cb) {
        this.client.listObject(bucketName, log.getSerializedUids(), params,
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, JSON.parse(data));
            });
        return null;
    }

    checkHealth(implName, log, cb) {
        return this.client.healthcheck(log, (err, result) => {
            const respBody = {};
            if (err) {
                log.error(`error from ${implName}`, { error: err });
                respBody[implName] = {
                    error: err,
                };
                // error returned as null so async parallel doesn't return
                // before all backends are checked
                return cb(null, respBody);
            }
            const parseResult = JSON.parse(result);
            respBody[implName] = {
                code: 200,
                message: 'OK',
                body: parseResult,
            };
            return cb(null, respBody);
        });
    }
}

module.exports = BucketClientInterface;
