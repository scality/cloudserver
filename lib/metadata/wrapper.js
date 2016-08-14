import BucketClientInterface from './bucketclient/backend';
import BucketFileInterface from './bucketfile/backend';
import BucketInfo from './BucketInfo';
import inMemory from './in_memory/backend';
import config from '../Config';
import bitmap from "node-bitmap-ewah";

let client;
let implName;

if (config.backends.metadata === 'mem') {
    client = inMemory;
    implName = 'memorybucket';
} else if (config.backends.metadata === 'file') {
    client = new BucketFileInterface();
    implName = 'bucketfile';
} else if (config.backends.metadata === 'scality') {
    client = new BucketClientInterface();
    implName = 'bucketclient';
}

function intersection(x, y) {
    return x.and(y);
}

function union(x, y) {
    return x.or(y);
}

function storeToBitmap(stored) {
    const bm = bitmap.createObject();
    if (stored) {
        stored[2] = new Buffer(stored[2], "binary");
        bm.read(stored);
    }
    return bm;
}

function bitmapToStore(bitmap) {
    let toStore = bitmap.write();
    toStore[2] = toStore[2].toString("binary");
    return toStore;
}

function initObjectCounter(bucketName, log, cb) {
    client.putObject(bucketName, "T|objectCounter", JSON.stringify({}), log, err => {
        if (err) {
            return cb(err);
        }
        client.putObject(bucketName, "T|bitmapIndex", JSON.stringify({counter:0}), log, err => {
            if (err) {
                return cb(err);
            }
            return cb(err);
        });
    });
}

function updateObjectCounter(bucketName, objName, objVal, log, cb) {
    if (objName.indexOf("..|..") !== -1)
        return cb(null);
    client.getObject(bucketName, "T|objectCounter", log, (err, data) => {
        if (err) {
            return cb(err);
        }
        data = JSON.parse(data);
        let rowId = 0;
        if (Object.keys(data)) {
            if (typeof data[objName] === "number")
                rowId = data[objName];
            else
                rowId = Object.keys(data).length;
        }
        data[rowId] = objName;
        client.putObject(bucketName, "T|objectCounter", JSON.stringify(data), log, err => {
            if (err) {
                return cb(err);
            }
            const tags = [];
            Object.keys(objVal).forEach(elem => {
                if (elem.indexOf("x-amz-meta") != -1 && elem != "x-amz-meta-s3cmd-attrs")
                    tags.push("T|"+elem+"/"+objVal[elem]);
            });
            return updateIndex(tags, bucketName, objName, objVal, rowId, log, cb);
        });
    });
}

function updateIndex(tags, bucketName, objName, objVal, rowId, log, cb) {
    client.getObject(bucketName, "T|bitmapIndex", log, (err, data) => {
        if (err) {
            return cb(err);
        }
        data = JSON.parse(data);
        tags.forEach(elem => {
            const index = storeToBitmap(data[elem]);
            index.push(rowId);
            data[elem] = bitmapToStore(index);
        });
        if (rowId > data.counter)
            data.counter = rowId;
        client.putObject(bucketName, "T|bitmapIndex", JSON.stringify(data), log, err => {
            if (err) {
                return cb(err);
            }
            return cb(err);
        });
    });
}

function traverseTree (array, index, bucketName, prefix, marker, maxKeys, delimiter, log, cb) {
    if (index === -1)
        return constructResponse([], array[0], bucketName, prefix, marker, delimiter, maxKeys, log, cb);
    if (array[index] === "T|x-amz-meta-op/AND") {
        array[index] = intersection(array[index+1], array[index+2]);
        traverseTree(array, index-1, bucketName, prefix, marker, maxKeys, delimiter, log, cb);
    }
    else if (array[index] === "T|x-amz-meta-op/OR") {
        array[index] = union(array[index+1], array[index+2]);
        traverseTree(array, index-1, bucketName, prefix, marker, maxKeys, delimiter, log, cb);
    }
    else
        getObjects(array, index, bucketName, prefix, marker, maxKeys, delimiter, log, cb)
}

function getObjects(array, index, bucketName, prefix, marker, maxKeys, delimiter, log, cb) {
    prefix = array[index];
    if (prefix.indexOf("T|x-amz-meta-op/NOT") !== -1)
        prefix = prefix.split(":")[1]
    client.getObject(bucketName, "T|bitmapIndex", log, (err, data) => {
        if (err) {
             return cb(err);
        }
        data = JSON.parse(data);
        let value = data[prefix];
        value[2] = new Buffer(value[2], "binary");
        let bm = bitmap.createObject();
        bm.read(value);
        if (array[index].indexOf("T|x-amz-meta-op/NOT") !== -1) {
            bm.push(data.counter+1);
            bm = bm.not();
        }
        array[index] = bm;
        traverseTree(array, index-1, bucketName, prefix, marker, maxKeys, delimiter, log, cb);
    });
}

function constructResponse(res, keys, bucketName, prefix, marker, delimiter, maxKeys, log, cb) {
    keys = keys.toString(":").split(":");
    client.getObject(bucketName, "T|objectCounter", log, (err, data) => {
        if (err) {
            return cb(err);
        }
        data = JSON.parse(data);
        const result = [];
        keys.forEach(elem =>{
            result.push(data[elem]);
        });
        client.listObject(bucketName, { prefix:"", marker, maxKeys, delimiter },
                log, (err, data) => {
                    if (err) {
                        return cb(err);
                    }
                    data.Contents = data.Contents.filter(function(elem) {
                        return result.indexOf(elem.key) !== -1;
                    });
                    return cb(err, data);
                });
    });
}

const metadata = {
    createBucket: (bucketName, bucketMD, log, cb) => {
        log.debug('creating bucket in metadata');
        client.createBucket(bucketName, bucketMD, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket created in metadata');
            return initObjectCounter(bucketName, log, cb);
        });
    },

    updateBucket: (bucketName, bucketMD, log, cb) => {
        log.debug('updating bucket in metadata');
        client.putBucketAttributes(bucketName, bucketMD, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket updated in metadata');
            return cb(err);
        });
    },

    getBucket: (bucketName, log, cb) => {
        log.debug('getting bucket from metadata');
        client.getBucketAttributes(bucketName, log, (err, data) => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket retrieved from metadata');
            return cb(err, BucketInfo.fromObj(data));
        });
    },

    deleteBucket: (bucketName, log, cb) => {
        log.debug('deleting bucket from metadata');
        client.deleteBucket(bucketName, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('Deleted bucket from Metadata');
            return cb(err);
        });
    },

    putObjectMD: (bucketName, objName, objVal, log, cb) => {
        log.debug('putting object in metdata');
        client.putObject(bucketName, objName, objVal, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('object successfully put in metadata');
            return updateObjectCounter(bucketName, objName, objVal, log, cb);
        });
    },

    getBucketAndObjectMD: (bucketName, objName, log, cb) => {
        log.debug('getting bucket and object from metadata',
                  { database: bucketName, object: objName });
        client.getBucketAndObject(bucketName, objName, log, (err, data) => {
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
        client.getObject(bucketName, objName, log, (err, data) => {
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
        client.deleteObject(bucketName, objName, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object deleted from metadata');
            return cb(err);
        });
    },

    listObject: (bucketName, prefix, marker, delimiter, maxKeys, log, cb) => {
        const localPrefix = prefix;
        prefix = null;
        if (typeof localPrefix === "object")
            return traverseTree(localPrefix, localPrefix.length-1, bucketName, prefix, marker, maxKeys, delimiter, log, cb);
        client
            .listObject(bucketName, { prefix, marker, maxKeys, delimiter },
                    log, (err, data) => {
                        log.debug('getting object listing from metadata');
                        if (err) {
                            log.warn('error from metadata', { implName, err });
                            return cb(err);
                        }
                        log.debug('object listing retrieved from metadata');
                        data.Contents = data.Contents.filter(function(elem) {
                            return elem.key.indexOf("T|") === -1;
                        });
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
