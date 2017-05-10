const ListResult = require('./ListResult');

class ListMultipartUploadsResult extends ListResult {
    constructor() {
        super();
        this.Uploads = [];
        this.NextKeyMarker = undefined;
        this.NextUploadIdMarker = undefined;
    }

    addUpload(uploadInfo) {
        this.Uploads.push({
            key: decodeURIComponent(uploadInfo.key),
            value: {
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
            },
        });
        this.MaxKeys += 1;
    }
}

module.exports = {
    ListMultipartUploadsResult,
};
