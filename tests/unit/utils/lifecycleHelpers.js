function getLifecycleRequest(bucketName, xml) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
    };
    if (xml) {
        request.post = xml;
    }
    return request;
}

function getLifecycleXml() {
    const id1 = 'test-id1';
    const id2 = 'test-id2';
    const id3 = 'test-id3';
    const prefix = 'test-prefix';
    const tags = [
        {
            key: 'test-key1',
            value: 'test-value1',
        },
        {
            key: 'test-key2',
            value: 'test-value2',
        },
    ];
    const action1 = 'Expiration';
    const days1 = 365;
    const action2 = 'NoncurrentVersionExpiration';
    const days2 = 1;
    const action3 = 'AbortIncompleteMultipartUpload';
    const days3 = 30;
    return '<LifecycleConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<Rule>' +
        `<ID>${id1}</ID>` +
        '<Status>Enabled</Status>' +
        `<Prefix>${prefix}</Prefix>` +
        `<${action3}><DaysAfterInitiation>${days3}` +
        `</DaysAfterInitiation></${action3}>` +
        '</Rule>' +
        '<Rule>' +
        `<ID>${id2}</ID>` +
        '<Status>Enabled</Status>' +
        '<Filter><And>' +
        `<Prefix>${prefix}</Prefix>` +
        `<Tag><Key>${tags[0].key}</Key>` +
        `<Value>${tags[0].value}</Value>` +
        `<Key>${tags[1].key}</Key>` +
        `<Value>${tags[1].value}</Value></Tag>` +
        '</And></Filter>' +
        `<${action2}><NoncurrentDays>${days2}</NoncurrentDays></${action2}>` +
        '</Rule>' +
        '<Rule>' +
        `<ID>${id3}</ID>` +
        '<Status>Disabled</Status>' +
        `<Filter><Tag><Key>${tags[0].key}</Key>` +
        `<Value>${tags[0].value}</Value></Tag></Filter>` +
        `<${action1}><Days>${days1}</Days></${action1}>` +
        '</Rule>' +
        '</LifecycleConfiguration>';
}

module.exports = {
    getLifecycleRequest,
    getLifecycleXml,
};
