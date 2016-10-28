import { errors } from 'arsenal';

import { markerFilterMPU, prefixFilter }
    from './bucket_utilities';
import { ListMultipartUploadsResult } from './ListMultipartUploadsResult';
import { metadata } from './metadata';

const defaultMaxKeys = 1000;
export default function getMultipartUploadListing(bucket, params, callback) {
    const { delimiter, keyMarker,
        uploadIdMarker, prefix, queryPrefixLength, splitter } = params;
    const splitterLen = splitter.length;
    const maxKeys = Number.parseInt(params.maxKeys, 10) ?
        Number.parseInt(params.maxKeys, 10) : defaultMaxKeys;
    const response = new ListMultipartUploadsResult();
    const keyMap = metadata.keyMaps.get(bucket.getName());
    if (prefix) {
        response.Prefix = prefix;
        if (typeof prefix !== 'string') {
            return callback(errors.InvalidArgument);
        }
    }

    if (keyMarker) {
        response.KeyMarker = keyMarker;
        if (typeof keyMarker !== 'string') {
            return callback(errors.InvalidArgument);
        }
    }

    if (uploadIdMarker) {
        response.UploadIdMarker = uploadIdMarker;
        if (typeof uploadIdMarker !== 'string') {
            return callback(errors.InvalidArgument);
        }
    }

    if (delimiter) {
        response.Delimiter = delimiter;
        if (typeof delimiter !== 'string') {
            return callback(errors.InvalidArgument);
        }
    }

    if (maxKeys && typeof maxKeys !== 'number') {
        return callback(errors.InvalidArgument);
    }

    // Sort uploads alphatebetically by objectKey and if same objectKey,
    // then sort in ascending order by time initiated
    let uploads = [];
    keyMap.forEach((val, key) => {
        uploads.push(key);
    });
    uploads.sort((a, b) => {
        const aIndex = a.indexOf(splitter);
        const bIndex = b.indexOf(splitter);
        const aObjectKey = a.substring(aIndex + splitterLen);
        const bObjectKey = b.substring(bIndex + splitterLen);
        const aInitiated = keyMap.get(a).initiated;
        const bInitiated = keyMap.get(b).initiated;
        if (aObjectKey === bObjectKey) {
            if (Date.parse(aInitiated) >= Date.parse(bInitiated)) {
                return 1;
            }
            if (Date.parse(aInitiated) < Date.parse(bInitiated)) {
                return -1;
            }
        }
        return (aObjectKey < bObjectKey) ? -1 : 1;
    });
    // Edit the uploads array so it only
    // contains keys that contain the prefix
    uploads = prefixFilter(prefix, uploads);
    uploads = uploads.map(stringKey => {
        const index = stringKey.indexOf(splitter);
        const index2 = stringKey.indexOf(splitter, index + splitterLen);
        const storedMD = keyMap.get(stringKey);
        return {
            key: stringKey.substring(index + splitterLen, index2),
            uploadId: stringKey.substring(index2 + splitterLen),
            bucket: storedMD.eventualStorageBucket,
            initiatorID: storedMD.initiator.ID,
            initiatorDisplayName: storedMD.initiator.DisplayName,
            ownerID: storedMD['owner-id'],
            ownerDisplayName: storedMD['owner-display-name'],
            storageClass: storedMD['x-amz-storage-class'],
            initiated: storedMD.initiated,
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
            response.NextKeyMarker = currentUpload.key;
            response.NextUploadIdMarker = currentUpload.uploadId;
            response.addUpload(currentUpload);
        }
    }
    return callback(null, response);
}
