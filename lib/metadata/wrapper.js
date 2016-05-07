import BucketClientInterface from './bucketclient/backend';
import inMemory from './in_memory/backend';
import Logger from 'werelogs';
import Config from '../Config';
import bunyanLogstash from 'bunyan-logstash';
import Memcached from 'memcached';
import { errors } from 'arsenal';
import crypto from 'crypto';

const _config = new Config();
const cacheLogger = new Logger('S3', {
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
    streams: [
        { stream: process.stdout },
        {
            type: 'raw',
            stream: bunyanLogstash.createStream({
                host: _config.log.logstash.host,
                port: _config.log.logstash.port,
            }),
        },
    ],
});


let cacheServers;
let caching;
let memcached;
Memcached.config.maxKeySize = 512;
Memcached.config.poolSize = 10;
Memcached.config.timeout = 1000; /* in milliseconds */
Memcached.config.idle = 1000; /* in milliseconds */
const cacheExpiry = 300; /* in seconds */

if (process.env.cacheServers) {
    // process.env.cacheServers should be comma separated hostname:port
    // if running locally: 'localhost:11211'
    cacheServers = process.env.cacheServers.split(',');
    if (!Array.isArray(cacheServers)) {
        throw new Error('cacheServers must be a string of locations in the ' +
        'form of hostname:port separated by commas if more than one');
    }
    caching = true;
    memcached = new Memcached(cacheServers);
    memcached.multi(cacheServers, server => {
        memcached.connect(server, (err, conn) => {
            if (err) throw new Error(err);
            cacheLogger.info('memcache server connected:',
            { server: conn.memcached.servers });
        });
    });
    memcached.on('failure', details => {
        cacheLogger.warn('memcache server went down',
        { server: details.server, reason: details.messages });
    });
    memcached.on('reconnecting', details => {
        cacheLogger.info('total downtime in ms of memcache server',
        { server: details.server, downTime: details.totalDownTime });
    });
}

let client;
let implName;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
    client = inMemory;
    implName = 'memorybucket';
} else {
    client = new BucketClientInterface();
    implName = 'bucketclient';
}

function _createHash(string) {
    const hash = crypto.createHash('sha256');
    return hash.update(string).digest('hex');
}

const metadata = {
    createBucket: (bucketName, bucketMD, log, cb) => {
        log.debug('creating bucket in metadata');
        client.createBucket(bucketName, bucketMD, log, err => {
            if (err) {
                log.info('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('bucket created in metadata');
            if (caching) {
                const cacheKey = _createHash(bucketName);
                return memcached.set(cacheKey, bucketMD, cacheExpiry,
                    cacheErr => {
                        if (cacheErr) {
                            log.info('error adding bucket metadata to cache',
                            { error: cacheErr });
                        } else {
                            log.debug('bucket metadata added to cache');
                        }
                        return cb(err);
                    });
            }
            return cb(err);
        });
    },

    updateBucket: (bucketName, bucketMD, log, cb) => {
        log.debug('updating bucket in metadata');
        if (caching) {
            const cacheKey = _createHash(bucketName);
            return memcached.replace(cacheKey, bucketMD, cacheExpiry,
                cacheErr => {
                    if (cacheErr) {
                        log.info('error updating bucket metadata in cache',
                        { error: cacheErr });
                        return cb(errors.InternalError);
                    }
                    log.debug('bucket medata updated in cache');
                    return client.putBucketAttributes(bucketName, bucketMD,
                        log, err => {
                            if (err) {
                                log.info('error from metadata',
                                { implName, error: err });
                                return cb(err);
                            }
                            log.debug('bucket updated in metadata');
                            return cb(err);
                        });
                });
        }
        return client.putBucketAttributes(bucketName, bucketMD, log, err => {
            if (err) {
                log.info('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('bucket updated in metadata');
            return cb(err);
        });
    },

    getBucket: (bucketName, log, cb) => {
        log.debug('checking cache for bucketmedata');
        if (caching) {
            const cacheKey = _createHash(bucketName);
            return memcached.get(cacheKey, (cacheErr, data) => {
                if (cacheErr || !data) {
                    log.debug('unable to retrieve bucket metadata from cache',
                    { error: cacheErr, data });
                    log.debug('getting bucket from metadata');
                    return client.getBucketAttributes(bucketName, log,
                        (err, dataFromMD) => {
                            if (err) {
                                log.info('error from metadata',
                                { implName, error: err });
                                return cb(err);
                            }
                            log.debug('bucket retrieved from metadata');
                            const cacheData = typeof dataFromMD === 'string' ?
                                JSON.parse(dataFromMD) : dataFromMD;
                            memcached.set(cacheKey, cacheData,
                                cacheExpiry, cacheErr => {
                                    if (cacheErr) {
                                        log.info('error adding bucket md ' +
                                        'to cache', { error: cacheErr });
                                    }
                                });
                            return cb(err, dataFromMD);
                        });
                }
                log.debug('retrieved bucket metadata from cache');
                memcached.touch(cacheKey, cacheExpiry, cacheErr => {
                    if (cacheErr) {
                        log.info('error touching bucket cache',
                        { error: cacheErr });
                    }
                });
                return cb(null, data);
            });
        }
        log.debug('getting bucket from metadata');
        return client.getBucketAttributes(bucketName, log,
            (err, data) => {
                if (err) {
                    log.info('error from metadata',
                    { implName, error: err });
                    return cb(err);
                }
                log.debug('bucket retrieved from metadata');
                return cb(err, data);
            });
    },

    deleteBucket: (bucketName, log, cb) => {
        log.debug('deleting bucket from metadata');
        if (caching) {
            const cacheKey = _createHash(bucketName);
            return memcached.del(cacheKey, cacheErr => {
                if (cacheErr) {
                    log.info('error deleting bucket metadata in cache',
                    { error: cacheErr });
                    return cb(errors.InternalError);
                }
                return client.deleteBucket(bucketName, log, err => {
                    if (err) {
                        log.info('error from metadata',
                        { implName, error: err });
                        return cb(err);
                    }
                    log.debug('Deleted bucket from Metadata');
                    return cb(err);
                });
            });
        }
        return client.deleteBucket(bucketName, log, err => {
            if (err) {
                log.info('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('Deleted bucket from Metadata');
            return cb(err);
        });
    },

    putObjectMD: (bucketName, objName, objVal, log, cb) => {
        log.debug('putting object in metadata');
        if (caching) {
            // Use ".." as separator because bucketName cannot contain ".."
            const cacheKey = _createHash(`${bucketName}..${objName}`);
            return memcached.set(cacheKey,
                objVal, cacheExpiry,
                cacheErr => {
                    if (cacheErr) {
                        log.info('error adding obj metadata to cache',
                        { error: cacheErr });
                        return cb(errors.InternalError);
                    }
                    log.debug('obj metadata added to cache');
                    return client.putObject(bucketName, objName, objVal, log,
                        err => {
                            if (err) {
                                log.warn('error from metadata',
                                { implName, error: err });
                                return cb(err);
                            }
                            log.debug('object successfully put in metadata');
                            return cb(err);
                        });
                });
        }
        return client.putObject(bucketName, objName, objVal, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('object successfully put in metadata');
            return cb(err);
        });
    },

    getBucketAndObjectMD: (bucketName, objName, log, cb) => {
        log.debug('getting bucket and object from metadata',
                  { database: bucketName, object: objName });
        if (caching) {
            const bucketCacheKey = _createHash(bucketName);
            const objCacheKey = _createHash(`${bucketName}..${objName}`);
            return memcached.getMulti([bucketCacheKey, objCacheKey],
                (cacheErr, data) => {
                    // Could optimise by just getting whichever of bucket or
                    // obj is not obtained from the cache rather than both
                    // if there is a failure for either
                    if (cacheErr || !data || Object.keys(data).length < 2) {
                        log.debug('unable to retrieve metadata from cache',
                        { error: cacheErr, data });
                        log.debug('getting bucket and obj from metadata');
                        return client.getBucketAndObject(bucketName, objName,
                            log, (err, data) => {
                                if (err) {
                                    log.info('error from metadata',
                                    { implName, error: err });
                                    return cb(err);
                                }
                                log.debug('bucket and obj retrieved from ' +
                                'metadata', { database: bucketName,
                                    object: objName });
                                if (data.bucket) {
                                    const cacheData = typeof data.bucket ===
                                        'string' ? JSON.parse(data.bucket) :
                                        data.bucket;
                                    memcached.set(bucketCacheKey,
                                        cacheData,
                                        cacheExpiry, cacheErr => {
                                            if (cacheErr) {
                                                log.info('error adding bucket' +
                                                'md to cache',
                                                { error: cacheErr });
                                            }
                                        });
                                }
                                if (data.obj) {
                                    const cacheData = typeof data.obj ===
                                        'string' ? JSON.parse(data.obj) :
                                        data.obj;
                                    memcached.set(objCacheKey,
                                        cacheData, cacheExpiry,
                                        cacheErr => {
                                            if (cacheErr) {
                                                log.info('error adding object' +
                                                'md to cache',
                                                { error: cacheErr });
                                            }
                                        });
                                }
                                return cb(err, data);
                            });
                    }
                    log.debug('retrieved bucket and obj metadata from cache');
                    const cached = {
                        bucket: data[bucketCacheKey],
                        obj: data[objCacheKey],
                    };
                    memcached.touch(bucketCacheKey, cacheExpiry, cacheErr => {
                        if (cacheErr) {
                            log.info('error touching bucket cache',
                            { error: cacheErr });
                        }
                    });
                    memcached.touch(objCacheKey, cacheExpiry,
                        cacheErr => {
                            if (cacheErr) {
                                log.info('error touching object cache',
                                { error: cacheErr });
                            }
                        });
                    return cb(null, cached);
                });
        }
        return client.getBucketAndObject(bucketName, objName, log,
            (err, data) => {
                if (err) {
                    log.debug('error from metadata', { implName, err });
                    return cb(err);
                }
                log.debug('bucket and object retrieved from metadata',
                          { database: bucketName, object: objName });
                return cb(err, data);
            });
    },

    getObjectMD: (bucketName, objName, log, cb) => {
        log.debug('getting object from metadata');
        if (caching) {
            const cacheKey = _createHash(`${bucketName}..${objName}`);
            return memcached.get(cacheKey,
                (cacheErr, data) => {
                    if (cacheErr || !data) {
                        log.debug('unable to retrieve obj metadata from cache',
                        { error: cacheErr, data });
                        log.debug('getting obj from metadata');
                        return client.getObject(bucketName, objName, log,
                            (err, data) => {
                                if (err) {
                                    log.info('error from metadata',
                                    { implName, error: err });
                                    return cb(err);
                                }
                                log.debug('obj retrieved from metadata');
                                if (data) {
                                    const cacheData = typeof data === 'string'
                                        ? JSON.parse(data) : data;
                                    memcached.set(cacheKey,
                                        cacheData, cacheExpiry,
                                        cacheErr => {
                                            if (cacheErr) {
                                                log.info('error adding object' +
                                                'md to cache',
                                                { error: cacheErr });
                                            }
                                        });
                                }
                                return cb(err, data);
                            });
                    }
                    log.debug('retrieved obj metadata from cache');
                    memcached.touch(cacheKey, cacheExpiry,
                        cacheErr => {
                            if (cacheErr) {
                                log.info('error touching object cache',
                                { error: cacheErr });
                            }
                        });
                    return cb(null, data);
                });
        }
        return client.getObject(bucketName, objName, log, (err, data) => {
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object retrieved from metadata');
            return cb(err, data);
        });
    },

    deleteObjectMD: (bucketName, objName, log, cb) => {
        log.debug('deleting object from metadata');
        if (caching) {
            const cacheKey = _createHash(`${bucketName}..${objName}`);
            return memcached.del(cacheKey, cacheErr => {
                if (cacheErr) {
                    log.info('error deleting obj metadata in cache',
                    { error: cacheErr });
                    return cb(errors.InternalError);
                }
                return client.deleteObject(bucketName, objName, log, err => {
                    if (err) {
                        log.warn('error from metadata', { implName, err });
                        return cb(err);
                    }
                    log.debug('object deleted from metadata');
                    return cb(err);
                });
            });
        }
        return client.deleteObject(bucketName, objName, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object deleted from metadata');
            return cb(err);
        });
    },

    listObject: (bucketName, prefix, marker, delimiter, maxKeys, log, cb) => {
        client
            .listObject(bucketName, { prefix, marker, maxKeys, delimiter },
                    log, (err, data) => {
                        log.debug('getting object listing from metadata');
                        if (err) {
                            log.warn('error from metadata', { implName, err });
                            return cb(err);
                        }
                        log.debug('object listing retrieved from metadata');
                        return cb(err, data);
                    });
    },

    listMultipartUploads: (bucketName, listingParams, log, cb) => {
        client.listMultipartUploads(bucketName, listingParams, log,
            (err, data) => {
                log.debug('getting mpu listing from metadata');
                if (err) {
                    log.warn('error from metadata', { implName, err });
                    return cb(err);
                }
                log.debug('mpu listing retrieved from metadata');
                return cb(err, data);
            });
    },

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
