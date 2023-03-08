const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;

const CURRENT_TYPE = 'current';
const NON_CURRENT_TYPE = 'noncurrent';
const ORPHAN_TYPE = 'orphan';

function _makeTags(tags) {
    const res = [];
    Object.entries(tags).forEach(([key, value]) =>
        res.push(
            {
                Key: key,
                Value: value,
            }
        ));
    return res;
}

function processCurrents(bucketName, listParams, list) {
    const data = {
        Name: bucketName,
        Prefix: listParams.prefix,
        MaxKeys: listParams.maxKeys,
        IsTruncated: !!list.IsTruncated,
        KeyMarker: listParams.marker,
        BeforeDate: listParams.beforeDate,
        NextKeyMarker: list.NextKeyMarker,
        Contents: [],
    };

    list.Contents.forEach(item => {
        const v = item.value;

        const content = {
            Key: item.key,
            LastModified: v.LastModified,
            Etag: v.ETag,
            Size: v.Size,
            Owner: {
                ID: v.Owner.ID,
                DisplayName: v.Owner.DisplayName
            },
            StorageClass: v.StorageClass,
            TagSet: _makeTags(v.tags),
            IsLatest: true, // for compatibily
            DataStoreName: v.dataStoreName,
            ListType: CURRENT_TYPE,
        };
        data.Contents.push(content);
    });

    return data;
}

function processNonCurrents(bucketName, listParams, list) {
    let nextVersionIdMarker = list.NextVersionIdMarker;
    if (nextVersionIdMarker && nextVersionIdMarker !== 'null') {
        nextVersionIdMarker = versionIdUtils.encode(nextVersionIdMarker);
    }

    let versionIdMarker = listParams.versionIdMarker;
    if (versionIdMarker && versionIdMarker !== 'null') {
        versionIdMarker = versionIdUtils.encode(versionIdMarker);
    }

    const data = {
        Name: bucketName,
        Prefix: listParams.prefix,
        MaxKeys: listParams.maxKeys,
        IsTruncated: !!list.IsTruncated,
        KeyMarker: listParams.keyMarker,
        // need to have VersionIdMarker encoded.
        VersionIdMarker: versionIdMarker,
        BeforeDate: listParams.beforeDate,
        NextKeyMarker: list.NextKeyMarker,
        NextVersionIdMarker: nextVersionIdMarker,
        Contents: [],
    };

    list.Contents.forEach(item => {
        const v = item.value;
        const versionId = (v.IsNull || v.VersionId === undefined) ?
            'null' : versionIdUtils.encode(v.VersionId)

        const content = {
            Key: item.key,
            LastModified: v.LastModified,
            Etag: v.ETag,
            Size: v.Size,
            Owner: {
                ID: v.Owner.ID,
                DisplayName: v.Owner.DisplayName
            },
            StorageClass: v.StorageClass,
            TagSet: _makeTags(v.tags),
            staleDate: v.staleDate, // lowerCamelCase to be compatible with existing lifecycle.
            VersionId: versionId,
            DataStoreName: v.dataStoreName,
            ListType: NON_CURRENT_TYPE,
        };

        data.Contents.push(content);
    });

    return data;
}

function processOrphans(bucketName, listParams, list) {
    const data = {
        Name: bucketName,
        Prefix: listParams.prefix,
        MaxKeys: listParams.maxKeys,
        IsTruncated: !!list.IsTruncated,
        KeyMarker: listParams.keyMarker,
        BeforeDate: listParams.beforeDate,
        NextKeyMarker: list.NextKeyMarker,
        Contents: [],
    };

    list.Contents.forEach(item => {
        const v = item.value;
        const versionId = (v.IsNull || v.VersionId === undefined) ?
            'null' : versionIdUtils.encode(v.VersionId);
        data.Contents.push({
            Key: item.key,
            LastModified: v.LastModified,
            Etag: v.ETag,
            Size: v.Size,
            Owner: {
                ID: v.Owner.ID,
                DisplayName: v.Owner.DisplayName
            },
            StorageClass: v.StorageClass,
            VersionId: versionId,
            IsLatest: true, // for compatibily
            DataStoreName: v.dataStoreName,
            ListType: ORPHAN_TYPE,
        });
    });

    return data;
}

module.exports = {
    processCurrents,
    processNonCurrents,
    processOrphans,
};
