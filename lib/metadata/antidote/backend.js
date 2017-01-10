import { errors } from 'arsenal';

import { markerFilter, prefixFilter } from '../in_memory/bucket_utilities';
import { ListBucketResult } from './ListBucketResult';
import getMultipartUploadListing from '../in_memory/getMultipartUploadListing';
import config from '../../Config';
import async from 'async';
import antidoteClient from 'antidote_ts_client';

const defaultMaxKeys = 1000;

var replacer = function(key, value) {
    if (value === undefined){
        return null;
    }
    return value;
};

var reviver = function(key, value) {
    if (value === null){
        return undefined;
    }
    return value;
};

class AntidoteInterface {
    constructor() {
        this.antidote = antidoteClient.connect(config.antidote.port, config.antidote.host);
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            // TODO Check whether user already owns the bucket,
            // if so return "BucketAlreadyOwnedByYou"
            // If not owned by user, return "BucketAlreadyExists"
            if (bucket) {
                return cb(errors.BucketAlreadyExists);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_MD = this.antidote.map(`${bucketName}/md`)
            this.antidote.update([
            bucket_MD.register('md').set(bucketMD)
            ]).then( (resp) => {
                return cb();
            });
        });
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_MD = this.antidote.map(`${bucketName}/md`)
            this.antidote.update([
                bucket_MD.register('md').set(bucketMD)
            ]).then( (resp) => {
                return cb();
            });
        });
    }

    getBucketAttributes(bucketName, log, cb) {
        this.antidote.defaultBucket = `storage/${bucketName}`;
        let bucket_MD = this.antidote.map(`${bucketName}/md`)
        bucket_MD.read().then(bucketMD => {
            bucketMD = bucketMD.toJsObject();
            if (Object.keys(bucketMD).length === 0) {
                return cb(errors.NoSuchBucket);
            }
            return cb(null, bucketMD['md']);
        });
    }

    deleteBucket(bucketName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket)  => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
            bucket_Objs.read().then(objects => {
                if (bucket && objects.length > 0) {
                    return cb(errors.BucketNotEmpty);
                }
                let bucket_MD = this.antidote.map(`${bucketName}/md`)
                this.antidote.update([
                     bucket_MD.remove(bucket_MD.register('md')),
                ]).then( (resp) => {
                    return cb(null);
                });
            });
        });
    }

    putObject(bucketName, objName, objVal, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
                this.antidote.defaultBucket = `storage/${bucketName}`;
                let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
                let object_MD = this.antidote.map(`${objName}`);
                this.antidote.update([
                    bucket_Objs.add(objName),
                    object_MD.register('md').set(objVal)
                ]).then( (resp) => {
                    return cb();
                });
            });
    }

    getBucketAndObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err, { bucket });
            }
            const bucket_MD = {}
            Object.keys(bucket).map(function(key) {
                bucket_MD[key.substr(1)] = bucket[key]
            });
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidote.map(`${objName}`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();

                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(null, { bucket: JSON.stringify(bucket_MD) });
                }
                return cb(null, {
                    bucket: JSON.stringify(bucket_MD),
                    obj: JSON.stringify(objectMD['md']),
                });
            });
        });
    }

    getObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidote.map(`${objName}`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();
                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(errors.NoSuchKey);
                }
                return cb(null, objectMD['md']);
            });
        });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidote.map(`${objName}`);
            let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();
                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(errors.NoSuchKey);
                }
                this.antidote.update([
                    object_MD.remove(object_MD.register('md')),
                    bucket_Objs.remove(objName)
                ]).then( (resp) => {
                    return cb();
                });
            });
        });
    }

    getObjectMD(antidote, bucketName, key, callback) {
        antidote.defaultBucket = `storage/${bucketName}`;
        let object_MD = antidote.map(`${key}`);
        object_MD.read().then(objectMD => {
            objectMD = objectMD.toJsObject();
            if (Object.keys(objectMD).length === 0) {
                return callback(error.NoSuchKey, null);
            }
            return callback(null, objectMD['md']);
        });
    }

    listObject(bucketName, params, log, cb) {
        const { prefix, marker, delimiter, maxKeys } = params;
        if (prefix && typeof prefix !== 'string') {
            return cb(errors.InvalidArgument);
        }

        if (marker && typeof marker !== 'string') {
            return cb(errors.InvalidArgument);
        }

        if (delimiter && typeof delimiter !== 'string') {
            return cb(errors.InvalidArgument);
        }

        if (maxKeys && typeof maxKeys !== 'number') {
            return cb(errors.InvalidArgument);
        }

        let numKeys = maxKeys;
        // If paramMaxKeys is undefined, the default parameter will set it.
        // However, if it is null, the default parameter will not set it.
        if (numKeys === null) {
            numKeys = defaultMaxKeys;
        }

        this.antidote.defaultBucket = `storage/${bucketName}`;
        let bucket_MD = this.antidote.map(`${bucketName}/md`)
        bucket_MD.read().then(bucketMD => {
            bucketMD = bucketMD.toJsObject();
            if (Object.keys(bucketMD).length === 0) {
                return cb(errors.NoSuchBucket);
            }
            const response = new ListBucketResult();

            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
            bucket_Objs.read().then(keys => {

                async.map(keys, this.getObjectMD.bind(null, this.antidote, bucketName), function(err, objectMeta) {

                    // If marker specified, edit the keys array so it
                    // only contains keys that occur alphabetically after the marker
                    if (marker) {
                        keys = markerFilter(marker, keys);
                        response.Marker = marker;
                    }
                    // If prefix specified, edit the keys array so it only
                    // contains keys that contain the prefix
                    if (prefix) {
                        keys = prefixFilter(prefix, keys);
                        response.Prefix = prefix;
                    }
                    // Iterate through keys array and filter keys containing
                    // delimiter into response.CommonPrefixes and filter remaining
                    // keys into response.Contents
                    for (let i = 0; i < keys.length; ++i) {
                        const currentKey = keys[i];
                        // Do not list object with delete markers
                        if (response.hasDeleteMarker(currentKey,
                            objectMeta[i])) {
                            continue;
                        }
                        // If hit numKeys, stop adding keys to response
                        if (response.MaxKeys >= numKeys) {
                            response.IsTruncated = true;
                            response.NextMarker = keys[i - 1];
                            break;
                        }
                        // If a delimiter is specified, find its index in the
                        // current key AFTER THE OCCURRENCE OF THE PREFIX
                        let delimiterIndexAfterPrefix = -1;
                        let prefixLength = 0;
                        if (prefix) {
                            prefixLength = prefix.length;
                        }
                        const currentKeyWithoutPrefix = currentKey
                            .slice(prefixLength);
                        let sliceEnd;
                        if (delimiter) {
                            delimiterIndexAfterPrefix = currentKeyWithoutPrefix
                                .indexOf(delimiter);
                            sliceEnd = delimiterIndexAfterPrefix + prefixLength;
                            response.Delimiter = delimiter;
                        }
                        // If delimiter occurs in current key, add key to
                        // response.CommonPrefixes.
                        // Otherwise add key to response.Contents
                        if (delimiterIndexAfterPrefix > -1) {
                            const keySubstring = currentKey.slice(0, sliceEnd + 1);
                            response.addCommonPrefix(keySubstring);
                        } else {
                            response.addContentsKey(currentKey,
                                objectMeta[i]);
                        }
                    }
                    return cb(null, response);
                });
            });
        });
    }

    listMultipartUploads(bucketName, listingParams, log, cb) {
        process.nextTick(() => {
            this.getBucketAttributes(bucketName, log, (err, bucket) => {
                if (bucket === undefined) {
                    // no on going multipart uploads, return empty listing
                    return cb(null, {
                        IsTruncated: false,
                        NextMarker: undefined,
                        MaxKeys: 0,
                    });
                }
                return getMultipartUploadListing(bucket, listingParams, cb);
            });
        });
    }
};

export default AntidoteInterface;
