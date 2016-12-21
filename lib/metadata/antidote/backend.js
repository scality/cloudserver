import { errors } from 'arsenal';

import { markerFilter, prefixFilter } from '../in_memory/bucket_utilities';
import { ListBucketResult } from './ListBucketResult';
import getMultipartUploadListing from '../in_memory/getMultipartUploadListing';
import config from '../../Config';
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
        this.antidote.defaultBucket = 'storage';
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            // TODO Check whether user already owns the bucket,
            // if so return "BucketAlreadyOwnedByYou"
            // If not owned by user, return "BucketAlreadyExists"
            if (bucket) {
                return cb(errors.BucketAlreadyExists);
            }
            let map = this.antidote.map('buckets')
            this.antidote.update([
                map.register(bucketName).set(JSON.stringify(bucketMD, replacer))
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
            let map = this.antidote.map('buckets')
            this.antidote.update([
                map.register(bucketName).set(JSON.stringify(bucketMD, replacer))
            ]).then( (resp) => {
                return cb();
            });
        });
    }

    getBucketAttributes(bucketName, log, cb) {
        let map = this.antidote.map('buckets')
        map.read().then(objs => {
            objs = objs.toJsObject();
            if (objs[bucketName] === undefined) {
                return cb(errors.NoSuchBucket);
            }
            const bucketMD = JSON.parse(objs[bucketName], reviver);
            return cb(null, bucketMD);
        });
    }

    deleteBucket(bucketName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket)  => {
            if (err) {
                return cb(err);
            }
            let map = this.antidote.map(bucketName)
            map.read().then(objs => {
                objs = objs.toJsObject();
                if (bucket[bucketName] && Object.keys(objs).length > 0) {
                    return cb(errors.BucketNotEmpty);
                }
                let map = this.antidote.map('buckets');
                this.antidote.update([
                     map.remove(map.register(bucketName)),
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
            let map = this.antidote.map(bucketName);
            this.antidote.update([
                map.register(objName).set(JSON.stringify(objVal))
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
            let map = this.antidote.map(bucketName);
            map.read().then(objs => {
                objs = objs.toJsObject();
                const obj = {}
                Object.keys(bucket).map(function(key) {
                        obj[key.substr(1)] = bucket[key]
                    });
                if (!bucket || !objs[objName]) {
                    return cb(null, { bucket: JSON.stringify(obj) });
                }
                return cb(null, {
                    bucket: JSON.stringify(obj),
                    obj: objs[objName],
                });
            });
        });
    }

    getObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            let map = this.antidote.map(bucketName);
            map.read().then(objs => {
                objs = objs.toJsObject();
                if (!bucket || !objs[objName]) {
                    return cb(errors.NoSuchKey);
                }
                let map = this.antidote.map(bucketName)
                map.read().then(objs => {
                    objs = objs.toJsObject();
                    return cb(null, JSON.parse(objs[objName]));
                });
            });
        });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            let map = this.antidote.map(bucketName);
            map.read().then(objs => {
                objs = objs.toJsObject();
                if (!objs[objName]) {
                    return cb(errors.NoSuchKey);
                }
                this.antidote.update([
                     map.remove(map.register(objName)),
                ]).then( (resp) => {
                    return cb();
                });
            });
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

        let map = this.antidote.map('buckets')
        map.read().then(objs => {
            objs = objs.toJsObject();
            if (!objs[bucketName]) {
                return cb(errors.NoSuchBucket);
            }
            const response = new ListBucketResult();
            let keys = [];
            map = this.antidote.map(bucketName)
            map.read().then(objs => {
                objs = objs.toJsObject();
                keys = Object.keys(objs);
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
                        JSON.parse(objs[currentKey]))) {
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
                            JSON.parse(objs[currentKey]));
                    }
                }
                return cb(null, response);
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
