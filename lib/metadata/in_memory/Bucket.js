import naturalCompare from 'natural-compare-lite';

import constants from '../../../constants';
import { markerFilterMPU, prefixFilter, findNextMarker, }
    from './bucket_utilities';
import { ListMultipartUploadsResult } from './ListMultipartUploadsResult';

const splitter = constants.splitter;
const defaultMaxKeys = 1000;

export default class Bucket {
    constructor(name, ownerId, ownerDisplayName) {
        this.keyMap = {};
        this.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
        this.policy = {};
        this.name = name;
        this.owner = ownerId;
        this.ownerDisplayName = ownerDisplayName;
        this.creationDate = new Date;
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
            const aInitiated = this.keyMap[a].initiated;
            const bInitiated = this.keyMap[b].initiated;
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

        uploads = uploads.map(stringKey => {
            const arrayKey = stringKey.split(splitter);
            const storedMD = this.keyMap[stringKey];
            return {
                key: arrayKey[1],
                uploadId: arrayKey[2],
                bucket: storedMD.eventualStorageBucket,
                initiatorID: storedMD.initiator.ID,
                initiatorDisplayName: storedMD.initiator.DisplayName,
                ownerID: storedMD['owner-id'],
                onwerDisplayName: storedMD['owner-display-name'],
                storageClass: storedMD.storageClass,
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
