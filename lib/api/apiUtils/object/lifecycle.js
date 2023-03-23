const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;

const { lifecycleListing } = require('../../../../constants');
const { CURRENT_TYPE, NON_CURRENT_TYPE, ORPHAN_DM_TYPE } = lifecycleListing;

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
        Marker: listParams.marker,
        BeforeDate: listParams.beforeDate,
        NextMarker: list.NextMarker,
        Contents: [],
    };

    list.Contents.forEach(item => {
        const v = item.value;

        const content = {
            Key: item.key,
            LastModified: v.LastModified,
            ETag: v.ETag,
            Size: v.Size,
            Owner: {
                ID: v.Owner.ID,
                DisplayName: v.Owner.DisplayName
            },
            StorageClass: v.StorageClass,
            TagSet: _makeTags(v.tags),
            IsLatest: true, // for compatibility with AWS ListObjectVersions.
            DataStoreName: v.dataStoreName,
            ListType: CURRENT_TYPE,
        };
        data.Contents.push(content);
    });

    return data;
}

function _encodeVersionId(vid) {
    let versionId = vid;
    if (versionId && versionId !== 'null') {
        versionId = versionIdUtils.encode(versionId);
    }
    return versionId;
}

function processNonCurrents(bucketName, listParams, list) {
    const nextVersionIdMarker = _encodeVersionId(list.NextVersionIdMarker);
    const versionIdMarker = _encodeVersionId(listParams.versionIdMarker);

    const data = {
        Name: bucketName,
        Prefix: listParams.prefix,
        MaxKeys: listParams.maxKeys,
        IsTruncated: !!list.IsTruncated,
        KeyMarker: listParams.keyMarker,
        VersionIdMarker: versionIdMarker,
        BeforeDate: listParams.beforeDate,
        NextKeyMarker: list.NextKeyMarker,
        NextVersionIdMarker: nextVersionIdMarker,
        Contents: [],
    };

    list.Contents.forEach(item => {
        const v = item.value;
        const versionId = (v.IsNull || v.VersionId === undefined) ?
            'null' : versionIdUtils.encode(v.VersionId);

        const content = {
            Key: item.key,
            LastModified: v.LastModified,
            ETag: v.ETag,
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
        Marker: listParams.marker,
        BeforeDate: listParams.beforeDate,
        NextMarker: list.NextMarker,
        Contents: [],
    };

    list.Contents.forEach(item => {
        const v = item.value;
        const versionId = (v.IsNull || v.VersionId === undefined) ?
            'null' : versionIdUtils.encode(v.VersionId);
        data.Contents.push({
            Key: item.key,
            LastModified: v.LastModified,
            ETag: v.ETag,
            Size: v.Size,
            Owner: {
                ID: v.Owner.ID,
                DisplayName: v.Owner.DisplayName
            },
            StorageClass: v.StorageClass,
            VersionId: versionId,
            IsLatest: true, // for compatibility with AWS ListObjectVersions.
            DataStoreName: v.dataStoreName,
            ListType: ORPHAN_DM_TYPE,
        });
    });

    return data;
}

module.exports = {
    processCurrents,
    processNonCurrents,
    processOrphans,
};
