import assert from 'assert';

import bucketclient from 'bucketclient';
import BucketInfo from '../BucketInfo';

import Config from '../../Config';

class BucketClientInterface {
    constructor() {
        const config = new Config();
        assert(config.bucketd.bootstrap.length > 0,
               'bucketd bootstrap list is empty');
        this.client = new bucketclient.RESTClient(config.bucketd.bootstrap,
                                                  config.log);
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.client.createBucket(bucketName, log.getSerializedUids(),
                                 bucketMD.serialize(), cb);
    }

    getBucketAttributes(bucketName, log, cb) {
        this.client.getBucketAttributes(bucketName, log.getSerializedUids(),
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, BucketInfo.deSerialize(data));
            });
    }

    getBucketAndObject(bucketName, objName, log, cb) {
        this.client.getBucketAndObject(bucketName, objName,
            log.getSerializedUids(), (err, data) => {
                if (err && (!err.NoSuchKey && !err.ObjNotFound)) {
                    return cb(err);
                }
                return cb(null, JSON.parse(data));
            });
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.client.putBucketAttributes(bucketName, log.getSerializedUids(),
                                        bucketMD.serialize(), cb);
    }

    deleteBucket(bucketName, log, cb) {
        this.client.deleteBucket(bucketName, log.getSerializedUids(), cb);
    }

    putObject(bucketName, objName, objVal, log, cb) {
        this.client.putObject(bucketName, objName, JSON.stringify(objVal),
            log.getSerializedUids(), cb);
    }

    getObject(bucketName, objName, log, cb) {
        this.client.getObject(bucketName, objName, log.getSerializedUids(),
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, JSON.parse(data));
            });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.client.deleteObject(bucketName, objName, log.getSerializedUids(),
                                 cb);
    }

    listObject(bucketName, params, log, cb) {
        this.client.listObject(bucketName, log.getSerializedUids(), params,
            (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb(err, JSON.parse(data));
            });
    }

    listMultipartUploads(bucketName, params, log, cb) {
        this.client.listObject(bucketName, log.getSerializedUids(), params,
            (err, data) => {
                return cb(err, JSON.parse(data));
            });
    }
}

export default BucketClientInterface;
