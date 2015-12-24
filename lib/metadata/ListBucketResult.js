import ListResult from './ListResult';

export class ListBucketResult extends ListResult {
    constructor() {
        super();
        this.Contents = [];
    }

    addContentsKey(key, keyMap) {
        const objectMetadata = keyMap[key];
        this.Contents.push({
            Key: decodeURIComponent(key),
            LastModified: objectMetadata['last-modified'],
            ETag: `"${objectMetadata['content-md5']}"`,
            StorageClass: objectMetadata['x-amz-storage-class'],
            Owner: {
                DisplayName: objectMetadata['owner-display-name'],
                ID: objectMetadata['owner-id']
            },
            Size: objectMetadata['content-length']
        });
        this.MaxKeys += 1;
    }

    hasDeleteMarker(key, keyMap) {
        const objectMD = keyMap[key];
        return (objectMD['x-amz-delete-marker'] === true);
    }
}
