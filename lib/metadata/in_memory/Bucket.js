import { markerFilter, prefixFilter, findNextMarker } from './bucket_utilities';
import { ListBucketResult } from './ListBucketResult';
import { ListMultipartUploadsResult } from './ListMultipartUploadsResult';
const defaultMaxKeys = 1000;

export default class Bucket {
    constructor(name, owner) {
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
        this.name = name;
        this.owner = owner;
        this.creationDate = new Date;
    }

    putObjectMD(key, value, callback) {
        process.nextTick(() => {
            this.keyMap[key] = value;
            if (typeof callback === 'function') {
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
            if (typeof callback === 'function') {
            // If error could arise in operation return error
            // as first argument in callback
                callback();
            }
        });
    }

    // TODO: Add this method to design document
    putPartLocation(partNumber, etag, size, location, multipartMetadata, cb) {
        process.nextTick(() => {
            multipartMetadata.partLocations[partNumber] = {
                size,
                location,
                etag,
                lastModified: new Date().toISOString(),
            };
            if (typeof cb === 'function') {
            // If error could arise in operation return error
            // as first argument in callback
                cb();
            }
        });
    }

    getMultipartUploadMD(uploadId, callback) {
        process.nextTick(() => {
            const hasMultipartUpload =
                this.multipartObjectKeyMap.hasOwnProperty(uploadId);
            if (typeof callback === 'function') {
                callback(!hasMultipartUpload,
                    this.multipartObjectKeyMap[uploadId], uploadId);
            }
        });
    }

    getObjectMD(key, callback) {
        process.nextTick(() => {
            const hasKey = this.keyMap.hasOwnProperty(key);
            if (typeof callback === 'function') {
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
            if (typeof callback === 'function') {
                callback(null);
            }
        });
    }

    deleteObjectMD(key, callback) {
        process.nextTick(() => {
            delete this.keyMap[key];
            if (typeof callback === 'function') {
              // If error could arise in delete operation return
              // error as first argument in callback
                callback(null);
            }
        });
    }

    deleteBucketMD(callback) {
        process.nextTick(() => {
            // TODO: Move delete functionality from services to here
            if (typeof callback === 'function') {
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

        const response = new ListBucketResult();
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
        // Iterate through keys array and filter keys containing delimiter
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

    getMultipartUploadListing(params, callback) {
        const { delimiter, keyMarker,
            uploadIdMarker, prefix } = params;
        const maxKeys = Number.parseInt(params.maxKeys, 10) || defaultMaxKeys;
        const response = new ListMultipartUploadsResult();

        if (prefix) {
            response.Prefix = prefix;
            if (typeof prefix !== 'string') {
                return callback('InvalidArgument');
            }
        }

        if (keyMarker) {
            response.KeyMarker = keyMarker;
            if (typeof keyMarker !== 'string') {
                return callback('InvalidArgument');
            }
        }

        if (uploadIdMarker) {
            response.UploadIdMarker = uploadIdMarker;
            if (typeof uploadIdMarker !== 'string') {
                return callback('InvalidArgument');
            }
        }

        if (delimiter) {
            response.Delimiter = delimiter;
            if (typeof delimiter !== 'string') {
                return callback('InvalidArgument');
            }
        }

        if (maxKeys && typeof maxKeys !== 'number') {
            return callback('InvalidArgument');
        }

        const uploadArray = [];
        for (const i in this.multipartObjectKeyMap) {
            if (this.multipartObjectKeyMap.hasOwnProperty(i)) {
                const uploadObjectKey = this.multipartObjectKeyMap[i].key;
                // If keyMarker specified, only include uploads
                // with a key alphabetically following the keyMarker
                if (keyMarker && keyMarker >= uploadObjectKey) {
                    continue;
                }
                // If both keyMarker and uploadIdMarker specified,
                // only include uploads
                // with uploadId following specified uploadIdMarker
                if (keyMarker && uploadIdMarker
                    && uploadIdMarker >=
                    this.multipartObjectKeyMap[i].uploadId) {
                    continue;
                }
                // If prefix specified, only include uploads
                // with a key that starts with the prefix
                if (prefix && uploadObjectKey.indexOf(prefix) !== 0) {
                    continue;
                }
                // If an upload got through the hurdles above, add it to the
                // array to be sorted
                uploadArray.push(this.multipartObjectKeyMap[i]);
            }
        }

        // Sort uploads alphatebetically by objectKey and if same objectKey,
        // then sort in ascending order by time initiated
        uploadArray.sort((a, b) => {
            if (a.key > b.key) {
                return 1;
            }
            if (a.key < b.key) {
                return -1;
            }
            if (a.key === b.key) {
                if (Date.parse(a.initiated) >= Date.parse(b.initated)) {
                    return 1;
                }
                if (Date.parse(a.initiated) < Date.parse(b.initiated)) {
                    return -1;
                }
            }
        });

        const prefixLength = prefix ? prefix.length : 0;
        // Iterate through uploadArray and filter uploads
        // with keys containing delimiter
        // into response.CommonPrefixes and filter remaining uploads
        // into response.Uploads
        for (let i = 0; i < uploadArray.length; i++) {
            const currentUpload = uploadArray[i];
            // If hit maxKeys, stop adding keys to response
            if (response.MaxKeys >= maxKeys) {
                response.IsTruncated = true;
                const markerResults = findNextMarker(i, uploadArray, response);
                response.NextKeyMarker = markerResults[0];
                response.NextUploadIdMarker = markerResults[1];
                break;
            }
            // If a delimiter is specified, find its
            // index in the current key AFTER THE OCCURRENCE OF THE PREFIX
            let delimiterIndexAfterPrefix = -1;

            const currentKeyWithoutPrefix =
                currentUpload.key.slice(prefixLength);
            let sliceEnd;
            if (delimiter) {
                delimiterIndexAfterPrefix = currentKeyWithoutPrefix
                    .indexOf(delimiter);
                sliceEnd = delimiterIndexAfterPrefix + prefixLength;
            }
            // If delimiter occurs in current key, add key to
            // response.CommonPrefixes.
            // Otherwise add upload to response.Uploads
            if (delimiterIndexAfterPrefix > -1) {
                const keySubstring = currentUpload.key.slice(0, sliceEnd + 1);
                response.addCommonPrefix(keySubstring);
            } else {
                response.addUpload(currentUpload.uploadId,
                    this.multipartObjectKeyMap);
            }
        }
        return callback(null, response);
    }
}
