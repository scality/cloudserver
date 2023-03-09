const CURRENT_TYPE = 'current';

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

module.exports = {
    processCurrents,
};
