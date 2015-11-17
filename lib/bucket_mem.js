import { markerFilter, prefixFilter, findNextMarker } from './bucket_utilities';
const defaultMaxKeys = 1000;

class _ListBucketResult {
    constructor() {
        this.IsTruncated = false;
        this.NextMarker = undefined;
        this.CommonPrefixes = [];
        this.Contents = [];
        /*
        Note:  this.MaxKeys will get incremented as
        keys are added so that when response is returned,
        this.MaxKeys will equal total keys in response
        (with each CommonPrefix counting as 1 key)
        */
        this.MaxKeys = 0;
    }

    addContentsKey(key, keyMap) {
        const objectMetadata = keyMap[key];
        this.Contents.push({
            "Key": decodeURIComponent(key),
            "LastModified": objectMetadata['last-modified'],
            "ETag": objectMetadata['content-md5'],
            "StorageClass": objectMetadata['x-amz-storage-class'],
            "Owner": {
                "DisplayName": objectMetadata['owner-display-name'],
                "ID": objectMetadata['owner-id']
            },
            "Size": objectMetadata['content-length']
        });
        this.MaxKeys += 1;
    }

    hasDeleteMarker(key, keyMap) {
        const objectMD = keyMap[key];
        return (objectMD['x-amz-delete-marker'] === true);
    }

    addCommonPrefix(prefix) {
        if (!this.hasCommonPrefix(prefix)) {
            this.CommonPrefixes.push(prefix);
            this.MaxKeys += 1;
        }
    }

    hasCommonPrefix(prefix) {
        return (this.CommonPrefixes.indexOf(prefix) !== -1);
    }
}

export default class Bucket {
    constructor() {
        this.keyMap = {};
        this.multipartObjectKeyMap = {};
        this.acl = {
            'Canned': 'private',
            'FULL_CONTROL': [],
            'WRITE': [],
            'WRITE_ACP': [],
            'READ': [],
            'READ_ACP': [],
        };
        this.policy = {};
        this.owner = '';
        this.name = '';
        this.creationDate = new Date;
    }
    putObjectMD(key, value, callback) {
        process.nextTick(() => {
            this.keyMap[key] = value;
            if (callback) {
            // If error could arise in operation return error
            // as first argument in callback
                callback();
            }
        });
    }

    // TODO: Add this method to design document
    putMPobjectMD(uploadId, multipartObjectMD, callback) {
        process.nextTick(() => {
            this.multipartObjectKeyMap[uploadId] = multipartObjectMD;
            if (callback) {
            // If error could arise in operation return error
            // as first argument in callback
                callback();
            }
        });
    }

    // TODO: Add this method to design document
    putPartLocation(partNumber, contentMD5, size,
        newLocation, multipartMetadata, callback) {
        process.nextTick(() => {
            const uploadId = multipartMetadata[uploadId];
            multipartMetadata.partLocations[partNumber] = {
                location: newLocation,
                etag: contentMD5,
                size: size,
            };
            if (callback) {
            // If error could arise in operation return error
            // as first argument in callback
                callback();
            }
        });
    }

    getMultipartUploadMD(uploadId, callback) {
        process.nextTick(() => {
            const hasMultipartUpload =
                this.multipartObjectKeyMap.hasOwnProperty(uploadId);
            if (callback) {
                callback(!hasMultipartUpload,
                    this.multipartObjectKeyMap[uploadId], uploadId);
            }
        });
    }

    getObjectMD(key, callback) {
        process.nextTick(() => {
            const hasKey = this.keyMap.hasOwnProperty(key);
            if (callback) {
                callback(!hasKey, this.keyMap[key], key);
            }
        });
    }

    // TODO: Add this method to design document
    // TODO: Consider whether there should be a bucket
    // method that creates a new object metadata entry
    // and deletes the old multipart metadata so there
    // is no possibility that the delete fails silently
    deleteMultipartUploadMD(uploadId, callback) {
        process.nextTick(() => {
            delete this.multipartObjectKeyMap[uploadId];
            if (callback) {
                callback(null);
            }
        });
    }

    deleteObjectMD(key, callback) {
        process.nextTick(() => {
            delete this.keyMap[key];
            if (callback) {
              // If error could arise in delete operation return
              // error as first argument in callback
                callback(null);
            }
        });
    }

    deleteBucketMD(callback) {
        process.nextTick(() => {
            // TODO: Move delete functionality from services to here
            if (callback) {
                callback(null);
            }
        });
    }

    getBucketListObjects(
        prefix, marker, delimiter, paramMaxKeys = defaultMaxKeys, callback) {
        if (prefix && typeof prefix !== 'string') {
            return callback('InvalidArgument');
        }

        if (marker && typeof marker !== 'string') {
            return callback('InvalidArgument');
        }

        if (delimiter && typeof delimiter !== 'string') {
            return callback('InvalidArgument');
        }

        if (paramMaxKeys && typeof paramMaxKeys !== 'number') {
            return callback('InvalidArgument');
        }

        // If paramMaxKeys is undefined, the default parameter will set it.
        // However, if it is null, the default parameter will not set it.
        let maxKeys = paramMaxKeys;
        if (maxKeys === null) {
            maxKeys = defaultMaxKeys;
        }

        const response = new _ListBucketResult();
        let keys = Object.keys(this.keyMap).sort();
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
        // Iterate through keys array and filter keys containing delimeter
        // into response.CommonPrefixes and filter remaining
        // keys into response.Contents
        const keysLength = keys.length;
        let currentKey;
        for (let i = 0; i < keysLength; i++) {
            currentKey = keys[i];
            // Do not list object with delete markers
            if (response.hasDeleteMarker(currentKey, this.keyMap) === true) {
                continue;
            }
            // If hit maxKeys, stop adding keys to response
            if (response.MaxKeys >= maxKeys) {
                response.IsTruncated = true;
                response.NextMarker = findNextMarker(i, keys, response);
                break;
            }
            // If a delimiter is specified, find its
            // index in the current key AFTER THE OCCURRENCE OF THE PREFIX
            let delimiterIndexAfterPrefix = -1;
            let prefixLength = 0;
            if (prefix) {
                prefixLength = prefix.length;
            }
            const currentKeyWithoutPrefix = currentKey.slice(prefixLength);
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
                response.addContentsKey(currentKey, this.keyMap);
            }
        }
        return callback(null, response);
    }
}
