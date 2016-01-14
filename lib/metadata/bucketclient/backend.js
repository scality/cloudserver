import assert from 'assert';

import bucketclient from 'bucketclient';

import Config from '../../Config';

class BucketClientInterface {
    constructor() {
        const config = new Config();
        assert(config.bucketd.bootstrap.length > 0,
               'bucketd bootstrap list is empty');
        this.client = new bucketclient.RESTClient(config.bucketd.bootstrap);
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.client.createBucket(bucketName, log.getSerializedUids(),
            JSON.stringify(bucketMD), err => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err);
            });
    }

    getBucketAttributes(bucketName, log, cb) {
        this.client.getBucketAttributes(bucketName, log.getSerializedUids(),
            (err, data) => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err, JSON.parse(data));
            });
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.client.putBucketAttributes(bucketName, log.getSerializedUids(),
            JSON.stringify(bucketMD), (err, data) => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err, data);
            });
    }

    deleteBucket(bucketName, log, cb) {
        this.client.deleteBucket(bucketName, log.getSerializedUids(), err => {
            if (err instanceof Error) {
                return cb(err.message);
            }
            return cb(err);
        });
    }

    putObject(bucketName, objName, objVal, log, cb) {
        this.client.putObject(bucketName, objName, JSON.stringify(objVal),
            log.getSerializedUids(), err => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err);
            });
    }

    getObject(bucketName, objName, log, cb) {
        this.client.getObject(bucketName, objName, log.getSerializedUids(),
            (err, data) => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err, JSON.parse(data));
            });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.client.deleteObject(bucketName, objName, log.getSerializedUids(),
            err => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err);
            });
    }

    listObject(bucketName, params, log, cb) {
        this.client.listObject(bucketName, log.getSerializedUids(), params,
            (err, data) => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err, JSON.parse(data));
            });
    }
}

export default BucketClientInterface;
