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

    createBucket(bucketName, bucketMD, cb) {
        this.client.createBucket(bucketName, JSON.stringify(bucketMD), err => {
            if (err instanceof Error) {
                return cb(err.message);
            }
            return cb(err);
        });
    }

    getBucketAttributes(bucketName, cb) {
        this.client.getBucketAttributes(bucketName, (err, data) => {
            if (err instanceof Error) {
                return cb(err.message);
            }
            return cb(err, JSON.parse(data));
        });
    }

    putBucketAttributes(bucketName, bucketMD, cb) {
        this.client.putBucketAttributes(bucketName, JSON.stringify(bucketMD),
            (err, data) => {
                if (err instanceof Error) {
                    return cb(err.message);
                }
                return cb(err, data);
            });
    }

    deleteBucket(bucketName, cb) {
        this.client.deleteBucket(bucketName, err => {
            if (err instanceof Error) {
                return cb(err.message);
            }
            return cb(err);
        });
    }

    putObject(bucketName, objName, objVal, cb) {
        this.client.putObject(bucketName, objName, JSON.stringify(objVal),
                              err => {
                                  if (err instanceof Error) {
                                      return cb(err.message);
                                  }
                                  return cb(err);
                              });
    }

    getObject(bucketName, objName, cb) {
        this.client.getObject(bucketName, objName, (err, data) => {
            if (err instanceof Error) {
                return cb(err.message);
            }
            return cb(err, JSON.parse(data));
        });
    }

    deleteObject(bucketName, objName, cb) {
        this.client.deleteObject(bucketName, objName, err => {
            if (err instanceof Error) {
                return cb(err.message);
            }
            return cb(err);
        });
    }

    listObject(bucketName, params, cb) {
        this.client.listObject(bucketName, params, (err, data) => {
            if (err instanceof Error) {
                return cb(err.message);
            }
            return cb(err, JSON.parse(data));
        });
    }
}

export default BucketClientInterface;
