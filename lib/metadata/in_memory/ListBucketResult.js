import ListResult from './ListResult';

export class ListBucketResult extends ListResult {
    constructor() {
        super();
        this.Contents = [];
    }

    addContentsKey(key, keyMap) {
        const objectMD = keyMap.get(key);
        this.Contents.push({
            key,
            value: {
                LastModified: objectMD['last-modified'],
                ETag: objectMD['content-md5'],
                StorageClass: objectMD['x-amz-storage-class'],
                Owner: {
                    DisplayName: objectMD['owner-display-name'],
                    ID: objectMD['owner-id'],
                },
                Size: objectMD['content-length'],
                //  Initiated is used for overview of MPU
                Initiated: objectMD.initiated,
                //  Initiator is used for overview of MPU.
                //  It is an object containing DisplayName
                //  and ID
                Initiator: objectMD.initiator,
                //  EventualStorageBucket is used for overview of MPU
                EventualStorageBucket: objectMD.eventualStorageBucket,
                // Used for parts of MPU
                partLocations: objectMD.partLocations,
                // creationDate is just used for serviceGet
                creationDate: objectMD.creationDate,
            },
        });
        this.MaxKeys += 1;
    }

    hasDeleteMarker(key, keyMap) {
        const objectMD = keyMap.get(key);
        if (objectMD['x-amz-delete-marker'] !== undefined) {
            return (objectMD['x-amz-delete-marker'] === true);
        }
        return false;
    }
}
