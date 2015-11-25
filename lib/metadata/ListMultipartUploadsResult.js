import ListResult from './ListResult';

export class ListMultipartUploadsResult extends ListResult {
    constructor() {
        super();
        this.Uploads = [];
    }

    addUpload(uploadInfo) {
        this.Uploads.push({
            Key: decodeURIComponent(uploadInfo.key),
            UploadId: uploadInfo.uploadId,
            Initiator: {
                ID: uploadInfo.initiatorID,
                DisplayName: uploadInfo.initiatorDisplayName,
            },
            Owner: {
                ID: uploadInfo.ownerID,
                DisplayName: uploadInfo.ownerDisplayName,
            },
            StorageClass: uploadInfo.storageClass,
            Initiated: uploadInfo.initiated,
        });
        this.MaxKeys += 1;
    }
}
