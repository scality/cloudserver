import { ListResult } from './ListResult';

export class ListMultipartUploadsResult extends ListResult {
    constructor() {
        super();
        this.Uploads = [];
    }

    addUpload(uploadId, keyMap) {
        const uploadMetadata = keyMap[uploadId];
        this.Uploads.push({
            "Key": decodeURIComponent(uploadMetadata.key),
            "UploadId": uploadMetadata.uploadId,
            "Initiator": {
                "ID": uploadMetadata.initiator.id,
                "DisplayName": uploadMetadata.initiator.displayName
            },
            "Owner": {
                "ID": uploadMetadata.owner.id,
                "DisplayName": uploadMetadata.owner.displayName
            },
            "StorageClass": uploadMetadata['x-amz-storage-class'],
            "Initiated": uploadMetadata.initiated,
        });
        this.MaxKeys += 1;
    }
}
