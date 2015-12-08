import naturalCompare from 'natural-compare-lite';

import { markerFilter, markerFilterMPU,
    prefixFilter, findNextMarker } from './bucket_utilities';
import { ListBucketResult } from './ListBucketResult';
import { ListMultipartUploadsResult } from './ListMultipartUploadsResult';
import config from '../../../config';
const splitter = config.splitter;

const defaultMaxKeys = 1000;

export default class Bucket {
    constructor(name, owner) {
        this.keyMap = {};
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
    putMPobjectMD(longMPUIdentifier, multipartObjectMD, callback) {
        process.nextTick(() => {
            this.keyMap[longMPUIdentifier] = multipartObjectMD;
            if (typeof callback === 'function') {
            // If error could arise in operation return error
            // as first argument in callback
                callback();
            }
        });
    }

    getMultipartUploadMD(uploadId, callback) {
        process.nextTick(() => {
            const hasMultipartUpload =
                this.keyMap.hasOwnProperty(uploadId);
            if (typeof callback === 'function') {
                callback(!hasMultipartUpload,
                    this.keyMap[uploadId], uploadId);
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
        let keys = Object.keys(this.keyMap).sort(naturalCompare);
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
        for (let i = 0; i < keys.length; i++) {
            const currentKey = keys[i];
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
            uploadIdMarker, prefix, queryPrefixLength } = params;
        const maxKeys = Number.parseInt(params.maxKeys, 10) ?
            Number.parseInt(params.maxKeys, 10) : defaultMaxKeys;
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

        // Sort uploads alphatebetically by objectKey and if same objectKey,
        // then sort in ascending order by time initiated
        let uploads = Object.keys(this.keyMap).sort((a, b) => {
            const aInfo = a.split(splitter);
            const bInfo = b.split(splitter);
            const aObjectKey = aInfo[1];
            const bObjectKey = bInfo[1];
            const aInitiated = aInfo[9];
            const bInitiated = bInfo[9];
            if (aObjectKey === bObjectKey) {
                if (Date.parse(aInitiated) >= Date.parse(bInitiated)) {
                    return 1;
                }
                if (Date.parse(aInitiated) < Date.parse(bInitiated)) {
                    return -1;
                }
            }
            return naturalCompare(a, b);
        });
        // Edit the uploads array so it only
        // contains keys that contain the prefix
        uploads = prefixFilter(prefix, uploads);

        uploads = uploads.map((stringKey) => {
            const arrayKey = stringKey.split(splitter);
            return {
                key: arrayKey[1],
                uploadId: arrayKey[2],
                bucket: arrayKey[3],
                initiatorID: arrayKey[4],
                initiatorDisplayName: arrayKey[5],
                ownerID: arrayKey[6],
                onwerDisplayName: arrayKey[7],
                storageClass: arrayKey[8],
                initiated: arrayKey[9],
            };
        });
        // If keyMarker specified, edit the uploads array so it
        // only contains keys that occur alphabetically after the marker.
        // If there is also an uploadIdMarker specified, filter to eliminate
        // any uploads that share the keyMarker and have an uploadId before
        //  the uploadIdMarker.
        if (keyMarker) {
            const allMarkers = {
                keyMarker,
                uploadIdMarker,
            };
            uploads = markerFilterMPU(allMarkers, uploads);
        }

        // Iterate through uploads and filter uploads
        // with keys containing delimiter
        // into response.CommonPrefixes and filter remaining uploads
        // into response.Uploads
        for (let i = 0; i < uploads.length; i++) {
            const currentUpload = uploads[i];
            // If hit maxKeys, stop adding keys to response
            if (response.MaxKeys >= maxKeys) {
                response.IsTruncated = true;
                const markerResults = findNextMarker(i, uploads, response);
                response.NextKeyMarker = markerResults[0];
                response.NextUploadIdMarker = markerResults[1];
                break;
            }
            // If a delimiter is specified, find its
            // index in the current key AFTER THE OCCURRENCE OF THE PREFIX
            // THAT WAS SENT IN THE QUERY (not the prefix including the splitter
            // and other elements)
            let delimiterIndexAfterPrefix = -1;
            const currentKeyWithoutPrefix =
                currentUpload.key.slice(queryPrefixLength);
            let sliceEnd;
            if (delimiter) {
                delimiterIndexAfterPrefix = currentKeyWithoutPrefix
                    .indexOf(delimiter);
                sliceEnd = delimiterIndexAfterPrefix + queryPrefixLength;
            }
            // If delimiter occurs in current key, add key to
            // response.CommonPrefixes.
            // Otherwise add upload to response.Uploads
            if (delimiterIndexAfterPrefix > -1) {
                const keySubstring = currentUpload.key.slice(0, sliceEnd + 1);
                response.addCommonPrefix(keySubstring);
            } else {
                response.addUpload(currentUpload);
            }
        }
        return callback(null, response);
    }
}
