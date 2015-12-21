import ListResult from './ListResult';

export class ListBucketResult extends ListResult {
    constructor() {
        super();
        this.Contents = [];
    }

    addContentsKey(key, keyMap) {
        const objectMD = typeof keyMap[key] === 'string' ?
            JSON.parse(keyMap[key]) : keyMap[key];
        this.Contents.push({
            key: decodeURIComponent(key),
            value: {
                LastModified: objectMD['last-modified'],
                ETag: `"${objectMD['content-md5']}"`,
                StorageClass: objectMD['x-amz-storage-class'],
                Owner: {
                    DisplayName: objectMD['owner-display-name'],
                    ID: objectMD['owner-id']
                },
                Size: objectMD['content-length']
            },
        });
        this.MaxKeys += 1;
    }

    hasDeleteMarker(key, keyMap) {
        const objectMD = keyMap[key];
        return (objectMD['x-amz-delete-marker'] === true);
    }
}
