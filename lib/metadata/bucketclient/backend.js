import assert from 'assert';

import bucketclient from 'bucketclient';
import { errors } from 'arsenal';
import BucketInfo from '../BucketInfo';
import { logger } from '../../utilities/logger';
import config from '../../Config';

const METADATA_DEFAULT_VERSION = 1;

class BucketClientInterface {
    constructor() {
        assert(config.bucketd.bootstrap.length > 0,
               'bucketd bootstrap list is empty');
        const { bootstrap, log } = config.bucketd;
        this.metadataVersion = METADATA_DEFAULT_VERSION; // 6.3.0 or earlier
        this.gotMetadataInfo = false; // got metadata information ?
                                      //     go ahead sending requests : wait
        // TODO remove when getURIComponents has been fixed
        this.extendedVersion = false;
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
        this.init();
    }

    init() {
        // TODO add a specific getMetadataInformation function in bucketclient
        const beginPath = '/default/metadataInformation/foo';
        const log = logger.newRequestLogger();
        this.client.request('GET', beginPath, log, null, null, (err, data) => {
            if (!err) {
                try {
                    const info = JSON.parse(data);
                    this.metadataVersion =
                        Number.parseInt(info.metadataVersion, 10);
                    this.extendedVersion =
                        this.metadataVersion > METADATA_DEFAULT_VERSION;
                } catch (error) {
                    log.error('error parsing metadata information',
                    { error: error.stack });
                }
            } else if (!err.RouteNotFound) {
                // retry on InternalError and DBAPINotReady
                return setTimeout(() => { this.init(); }, 0);
            }
            this.gotMetadataInfo = true;
            return null;
        });
    }

    createBucket(bucketName, bucketMD, log, cb) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        this.client.createBucket(bucketName, log.getSerializedUids(),
                                 bucketMD.serialize(), cb);
        return null;
    }

    getBucketAttributes(bucketName, log, cb) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        this.client.getBucketAttributes(bucketName, log.getSerializedUids(),
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, BucketInfo.deSerialize(data));
            });
        return null;
    }

    getBucketAndObject(bucketName, objName, log, cb, params) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        const _params = this.extendedVersion ? params : undefined;
        this.client.getBucketAndObject(bucketName, objName,
            log.getSerializedUids(), (err, data) => {
                if (err && (!err.NoSuchKey && !err.ObjNotFound)) {
                    return cb(err);
                }
                return cb(null, JSON.parse(data));
            }, _params);
        return null;
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        this.client.putBucketAttributes(bucketName, log.getSerializedUids(),
                                        bucketMD.serialize(), cb);
        return null;
    }

    deleteBucket(bucketName, log, cb) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        this.client.deleteBucket(bucketName, log.getSerializedUids(), cb);
        return null;
    }

    putObject(bucketName, objName, objVal, log, cb, params) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        const _params = this.extendedVersion ? params : undefined;
        this.client.putObject(bucketName, objName, JSON.stringify(objVal),
            log.getSerializedUids(), cb, _params);
        return null;
    }

    getObject(bucketName, objName, log, cb, params) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        const _params = this.extendedVersion ? params : undefined;
        this.client.getObject(bucketName, objName, log.getSerializedUids(),
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, JSON.parse(data));
            }, _params);
        return null;
    }

    deleteObject(bucketName, objName, log, cb, params) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        const _params = this.extendedVersion ? params : undefined;
        this.client.deleteObject(bucketName, objName, log.getSerializedUids(),
                                 cb, _params);
        return null;
    }

    listObject(bucketName, params, log, cb) {
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
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
        if (this.gotMetadataInfo === false) {
            return cb(errors.ServiceUnavailable);
        }
        this.client.listObject(bucketName, log.getSerializedUids(), params,
            (err, data) => cb(err, JSON.parse(data)));
        return null;
    }
}

export default BucketClientInterface;
