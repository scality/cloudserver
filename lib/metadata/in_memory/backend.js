import { errors } from 'arsenal';
import naturalCompare from 'natural-compare-lite';

import { markerFilter, prefixFilter } from './bucket_utilities';
import { ListBucketResult } from './ListBucketResult';
import getMultipartUploadListing from './getMultipartUploadListing';
import metadata from './metadata';

const defaultMaxKeys = 1000;

const metastore = {
    createBucket: (bucketName, bucketMD, log, cb) => {
        metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
            // TODO Check whether user already owns the bucket,
            // if so return "BucketAlreadyOwnedByYou"
            // If not owned by user, return "BucketAlreadyExists"
            if (bucket) {
                return cb(errors.BucketAlreadyExists);
            }
            metadata.buckets[bucketName] = bucketMD;
            cb();
        });
    },

    putBucketAttributes: (bucketName, bucketMD, log, cb) => {
        metastore.getBucketAttributes(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            metadata.buckets[bucketName] = bucketMD;
            cb();
        });
    },

    getBucketAttributes: (bucketName, log, cb) => {
        if (!metadata.buckets[bucketName]) {
            return cb(errors.NoSuchBucket);
        }
        cb(null, metadata.buckets[bucketName]);
    },

    deleteBucket: (bucketName, log, cb) => {
        metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            if (bucket.keyMap.length > 0) {
                return cb(errors.BucketNotEmpty);
            }
            delete metadata.buckets[bucketName];
            cb(null);
        });
    },

    putObject: (bucketName, objName, objVal, log, cb) => {
        metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            bucket.keyMap[objName] = objVal;
            cb();
        });
    },

    getObject: (bucketName, objName, log, cb) => {
        metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            if (!bucket.keyMap[objName]) {
                return cb(errors.NoSuchKey);
            }
            return cb(null, bucket.keyMap[objName]);
        });
    },

    deleteObject: (bucketName, objName, log, cb) => {
        metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            if (bucket.keyMap[objName] === undefined) {
                return cb(errors.NoSuchKey);
            }
            delete bucket.keyMap[objName];
            cb();
        });
    },

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

        metastore.getBucketAttributes(bucketName, log,
            function getList(err, bucket) {
                if (err) {
                    return cb(err);
                }
                const response = new ListBucketResult();
                let keys = Object.keys(bucket.keyMap).sort(naturalCompare);
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
                for (let i = 0; i < keys.length; i++) {
                    const currentKey = keys[i];
                    // Do not list object with delete markers
                    if (response.hasDeleteMarker(currentKey, bucket.keyMap)) {
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
                        response.addContentsKey(currentKey, bucket.keyMap);
                    }
                }
                return cb(null, response);
            });
    },

    listMultipartUploads(bucketName, listingParams, log, cb) {
        metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (bucket === undefined) {
                // no on going multipart uploads, return empty listing
                return cb(null, {
                    IsTruncated: false,
                    NextMarker: undefined,
                    MaxKeys: 0,
                });
            }
            getMultipartUploadListing(bucket, listingParams, cb);
        });
    },
};

export default metastore;
