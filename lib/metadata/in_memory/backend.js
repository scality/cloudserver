import { errors, algorithms, versioning } from 'arsenal';

import getMultipartUploadListing from './getMultipartUploadListing';
import { metadata } from './metadata';

const genVID = versioning.VersionID.generateVersionId;

const defaultMaxKeys = 1000;
let uidCounter = 0;

function generateVersionId() {
    return genVID(uidCounter++);
}

function formatVersionKey(key, versionId) {
    return `${key}\0${versionId}`;
}

function inc(str) {
    return str ? (str.slice(0, str.length - 1) +
            String.fromCharCode(str.charCodeAt(str.length - 1) + 1)) : str;
}

const metastore = {
    createBucket: (bucketName, bucketMD, log, cb) => {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
                // TODO Check whether user already owns the bucket,
                // if so return "BucketAlreadyOwnedByYou"
                // If not owned by user, return "BucketAlreadyExists"
                if (bucket) {
                    return cb(errors.BucketAlreadyExists);
                }
                metadata.buckets.set(bucketName, bucketMD);
                metadata.keyMaps.set(bucketName, new Map);
                return cb();
            });
        });
    },

    putBucketAttributes: (bucketName, bucketMD, log, cb) => {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                metadata.buckets.set(bucketName, bucketMD);
                return cb();
            });
        });
    },

    getBucketAttributes: (bucketName, log, cb) => {
        process.nextTick(() => {
            if (!metadata.buckets.has(bucketName)) {
                return cb(errors.NoSuchBucket);
            }
            return cb(null, metadata.buckets.get(bucketName));
        });
    },

    deleteBucket: (bucketName, log, cb) => {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                if (metadata.keyMaps.has(bucketName)
                    && metadata.keyMaps.get(bucketName).length > 0) {
                    return cb(errors.BucketNotEmpty);
                }
                metadata.buckets.delete(bucketName);
                metadata.keyMaps.delete(bucketName);
                return cb(null);
            });
        });
    },

    putObject: (bucketName, objName, objVal, params, log, cb) => {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                if (params && params.versioning) {
                    const versionId = generateVersionId();
                    objVal.versionId = versionId; // eslint-disable-line
                    metadata.keyMaps.get(bucketName).set(objName, objVal);
                    // eslint-disable-next-line
                    objName = formatVersionKey(objName, versionId);
                    metadata.keyMaps.get(bucketName).set(objName, objVal);
                    return cb(null, `{"versionId":"${versionId}"}`);
                }
                if (params && params.versionId === '') {
                    const versionId = generateVersionId();
                    objVal.versionId = versionId; // eslint-disable-line
                    metadata.keyMaps.get(bucketName).set(objName, objVal);
                    return cb(null, `{"versionId":"${objVal.versionId}"}`);
                } else if (params && params.versionId) {
                    objVal.versionId = params.versionId; // eslint-disable-line
                    const mst = metadata.keyMaps.get(bucketName).get(objName);
                    if (mst && mst.versionId === params.versionId) {
                        metadata.keyMaps.get(bucketName).set(objName, objVal);
                    }
                    // eslint-disable-next-line
                    objName = formatVersionKey(objName, params.versionId);
                    metadata.keyMaps.get(bucketName).set(objName, objVal);
                    return cb(null, `{"versionId":"${objVal.versionId}"}`);
                }
                metadata.keyMaps.get(bucketName).set(objName, objVal);
                return cb(null);
            });
        });
    },

    getBucketAndObject: (bucketName, objName, params, log, cb) => {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
                if (err) {
                    return cb(err, { bucket });
                }
                if (params && params.versionId) {
                    // eslint-disable-next-line
                    objName = formatVersionKey(objName, params.versionId);
                }
                if (!metadata.keyMaps.has(bucketName)
                    || !metadata.keyMaps.get(bucketName).has(objName)) {
                    return cb(null, { bucket: bucket.serialize() });
                }
                return cb(null, {
                    bucket: bucket.serialize(),
                    obj: JSON.stringify(
                        metadata.keyMaps.get(bucketName).get(objName)
                    ),
                });
            });
        });
    },

    getObject: (bucketName, objName, params, log, cb) => {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                if (params && params.versionId) {
                    // eslint-disable-next-line
                    objName = formatVersionKey(objName, params.versionId);
                }
                if (!metadata.keyMaps.has(bucketName)
                    || !metadata.keyMaps.get(bucketName).has(objName)) {
                    return cb(errors.NoSuchKey);
                }
                return cb(null, metadata.keyMaps.get(bucketName).get(objName));
            });
        });
    },

    deleteObject: (bucketName, objName, params, log, cb) => {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                if (!metadata.keyMaps.get(bucketName).has(objName)) {
                    return cb(errors.NoSuchKey);
                }
                if (params && params.versionId) {
                    const baseKey = inc(formatVersionKey(objName, ''));
                    const vobjName = formatVersionKey(objName,
                            params.versionId);
                    metadata.keyMaps.get(bucketName).delete(vobjName);
                    const mst = metadata.keyMaps.get(bucketName).get(objName);
                    if (mst.versionId === params.versionId) {
                        const keys = [];
                        metadata.keyMaps.get(bucketName).forEach((val, key) => {
                            if (key < baseKey && key > vobjName) {
                                keys.push(key);
                            }
                        });
                        if (keys.length === 0) {
                            metadata.keyMaps.get(bucketName).delete(objName);
                            return cb();
                        }
                        const key = keys.sort()[0];
                        const value = metadata.keyMaps.get(bucketName).get(key);
                        metadata.keyMaps.get(bucketName).set(objName, value);
                    }
                    return cb();
                }
                metadata.keyMaps.get(bucketName).delete(objName);
                return cb();
            });
        });
    },

    _hasDeleteMarker(key, keyMap) {
        const objectMD = keyMap.get(key);
        if (objectMD['x-amz-delete-marker'] !== undefined) {
            return (objectMD['x-amz-delete-marker'] === true);
        }
        return false;
    },

    listObject(bucketName, params, log, cb) {
        process.nextTick(() => {
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

            // If paramMaxKeys is undefined, the default parameter will set it.
            // However, if it is null, the default parameter will not set it.
            let numKeys = maxKeys;
            if (numKeys === null) {
                numKeys = defaultMaxKeys;
            }

            if (!metadata.keyMaps.has(bucketName)) {
                return cb(errors.NoSuchBucket);
            }

            // If marker specified, edit the keys array so it
            // only contains keys that occur alphabetically after the marker
            const listingType = params.listingType || 'Delimiter';
            const extension = new algorithms.list[listingType](params, log);
            const listingParams = extension.genMDParams();

            const keys = [];
            metadata.keyMaps.get(bucketName).forEach((val, key) => {
                if (listingParams.gt && listingParams.gt >= key) {
                    return null;
                }
                if (listingParams.gte && listingParams.gte > key) {
                    return null;
                }
                if (listingParams.lt && key >= listingParams.lt) {
                    return null;
                }
                if (listingParams.lte && key > listingParams.lte) {
                    return null;
                }
                return keys.push(key);
            });
            keys.sort();

            // Iterate through keys array and filter keys containing
            // delimiter into response.CommonPrefixes and filter remaining
            // keys into response.Contents
            for (let i = 0; i < keys.length; ++i) {
                const currentKey = keys[i];
                // Do not list object with delete markers
                if (this._hasDeleteMarker(currentKey,
                    metadata.keyMaps.get(bucketName))) {
                    continue;
                }
                const objMD = metadata.keyMaps.get(bucketName).get(currentKey);
                const value = JSON.stringify(objMD);
                const obj = {
                    key: currentKey,
                    value,
                };
                // calling Ext.filter(obj) adds the obj to the Ext result if
                // not filtered.
                // Also, Ext.filter returns false when hit max keys.
                // What a nifty function!
                if (extension.filter(obj) < 0) {
                    break;
                }
            }
            return cb(null, extension.result());
        });
    },

    listMultipartUploads(bucketName, listingParams, log, cb) {
        process.nextTick(() => {
            metastore.getBucketAttributes(bucketName, log, (err, bucket) => {
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
    },
};

export default metastore;
