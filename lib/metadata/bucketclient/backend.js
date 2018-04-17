const assert = require('assert');
const bucketclient = require('bucketclient');

const BucketInfo = require('arsenal').models.BucketInfo;
const logger = require('../../utilities/logger');
const { config } = require('../../Config');

function analyzeHealthFailure(log, callback) {
    let doFail = false;
    const reason = { msg: 'Map is available and failure ratio is acceptable' };

    // The healthCheck exposed by Bucketd is a light one, we need
    // to inspect all the RaftSession's statuses to make sense of
    // it:
    return this.client.getAllRafts(undefined, (error, payload) => {
        let statuses = null;
        try {
            statuses = JSON.parse(payload);
        } catch (e) {
            doFail = true;
            reason.msg = 'could not interpret status: invalid payload';
            // Can't do anything anymore if we fail here. return.
            return callback(doFail, reason);
        }

        const reducer = (acc, payload) => acc + !payload.connectedToLeader;
        reason.ratio = statuses.reduce(reducer, 0) / statuses.length;
        /* NOTE FIXME/TODO: acceptableRatio could be configured later on */
        reason.acceptableRatio = 0.5;
        /* If the RaftSession 0 (map) does not work, fail anyways */
        if (!doFail && !statuses[0].connectedToLeader) {
            doFail = true;
            reason.msg = 'Bucket map unavailable';
        }
        if (!doFail && reason.ratio > reason.acceptableRatio) {
            doFail = true;
            reason.msg = 'Ratio of failing Raft Sessions is too high';
        }
        return callback(doFail, reason);
    }, log);
}

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

    /*
     * Bucketd offers a behavior that diverges from other sub-components on the
     * healthCheck: If any of the pieces making up the bucket storage fail (ie:
     * if any Raft Session is down), bucketd returns a 500 for the healthCheck.
     *
     * As seen in S3C-1412, this may become an issue for S3, whenever the
     * system is only partly failing.
     *
     * This means that S3 needs to analyze the situation, and interpret this
     * status depending on the analysis. S3 will then assess the situation as
     * critical (and consider it a failure), or not (and consider it a success
     * anyways, thus not diverging from the healthCheck behavior of other
     * components).
     */
    checkHealth(implName, log, cb) {
        return this.client.healthcheck(log, (err, result) => {
            const respBody = {};
            if (err) {
                return analyzeHealthFailure(log, (failure, reason) => {
                    const message = reason.msg;
                    // Remove 'msg' from the reason payload.
                    // eslint-disable-next-line no-param-reassign
                    reason.msg = undefined;
                    respBody[implName] = {
                        code: 200,
                        message,        // Provide interpreted reason msg
                        body: reason,   // Provide analysis data
                    };
                    if (failure) {
                        respBody[implName].code = err.code; // original error
                    }
                    // error returned as null so async parallel doesn't return
                    // before all backends are checked
                    return cb(null, respBody);
                }, log);
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
