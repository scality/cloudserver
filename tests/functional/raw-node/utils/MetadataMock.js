const BucketInfo = require('arsenal').models.BucketInfo;

const dummyBucketMD = {
    bucket1: {
        _acl: {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [] },
        _name: 'xxxfriday10',
        _owner:
            '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
        _ownerDisplayName: 'test_1518720219',
        _creationDate: '2018-02-16T21:55:16.415Z',
        _mdBucketModelVersion: 5,
        _transient: false,
        _deleted: false,
        _serverSideEncryption: null,
        _versioningConfiguration: null,
        _locationConstraint: 'us-east-1',
        _websiteConfiguration: null,
        _replicationConfiguration: null,
        _cors: null,
        _lifecycleConfiguration: null,
        _uid: undefined,
    },
    bucket2: {
        _acl: {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [] },
        _name: 'xxxfriday11',
        _owner:
            '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
        _ownerDisplayName: 'test_1518720219',
        _creationDate: '2018-02-16T21:55:16.415Z',
        _mdBucketModelVersion: 5,
        _transient: false,
        _deleted: false,
        _serverSideEncryption: null,
        _versioningConfiguration: null,
        _locationConstraint: 'us-east-1',
        _websiteConfiguration: null,
        _replicationConfiguration: null,
        _cors: null,
        _lifecycleConfiguration: null,
        _uid: undefined,
    },
};

const objectList = {
    Contents: [
        { key: 'testobject1',
        value: JSON.stringify({
            'owner-display-name': 'test_1518720219',
            'owner-id':
            '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
            'content-length': 0,
            'content-md5': 'd41d8cd98f00b204e9800998ecf8427e',
            'x-amz-version-id': 'null',
            'x-amz-server-version-id': '',
            'x-amz-storage-class': 'STANDARD',
            'x-amz-server-side-encryption': '',
            'x-amz-server-side-encryption-aws-kms-key-id': '',
            'x-amz-server-side-encryption-customer-algorithm': '',
            'x-amz-website-redirect-location': '',
            'acl': {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
            'key': '',
            'location': null,
            'isDeleteMarker': false,
            'tags': {},
            'replicationInfo': {
                status: '',
                backends: [],
                content: [],
                destination: '',
                storageClass: '',
                role: '',
                storageType: '',
                dataStoreVersionId: '',
            },
            'dataStoreName': 'us-east-1',
            'last-modified': '2018-02-16T22:43:37.174Z',
            'md-model-version': 3,
        }) },
    ],
};

const mockLogs = {
    info: { start: 1, cseq: 7, prune: 1 },
    log: [
        { db: 'friday', method: 0, entries: [
            { value: '{\"attributes\":\"{\\\"name\\\":\\\"friday\\\",' +
            '\\\"owner\\\":\\\"94224c921648ada653f584f3caf42654ccf3f1cb' +
            'd2e569a24e88eb460f2f84d8\\\",\\\"ownerDisplayName\\\":' +
            '\\\"test_1518720219\\\",\\\"creationDate\\\":' +
            '\\\"2018-02-16T19:59:31.664Z\\\",\\\"mdBucketModelVersion\\\":5,' +
            '\\\"transient\\\":true,\\\"deleted\\\":false,' +
            '\\\"serverSideEncryption\\\":null,\\\"versioningConfiguration' +
            '\\\":null,\\\"locationConstraint\\\":\\\"us-east-1\\\",\\\"cors' +
            '\\\":null,\\\"replicationConfiguration\\\":null}\"}' },
        ] },
        { db: 'friday', method: 7, entries: [
            { value: '{\"attributes\":\"{\\\"name\\\":\\\"friday\\\",' +
            '\\\"owner\\\":\\\"94224c921648ada653f584f3caf42654ccf3f1cb' +
            'd2e569a24e88eb460f2f84d8\\\",\\\"ownerDisplayName\\\":' +
            '\\\"test_1518720219\\\",\\\"creationDate\\\":' +
            '\\\"2018-02-16T19:59:31.664Z\\\",\\\"mdBucketModelVersion\\\":5,' +
            '\\\"transient\\\":false,\\\"deleted\\\":false,' +
            '\\\"serverSideEncryption\\\":null,\\\"versioningConfiguration' +
            '\\\":null,\\\"locationConstraint\\\":\\\"us-east-1\\\",\\\"cors' +
            '\\\":null,\\\"replicationConfiguration\\\":null}\",' +
            '\"raftSession\":1}' },
        ] },
        { db: 'friday7', method: 0, entries: [
            { value: '{\"attributes\":\"{\\\"name\\\":\\\"friday7\\\",' +
            '\\\"owner\\\":\\\"94224c921648ada653f584f3caf42654ccf3f1cb' +
            'd2e569a24e88eb460f2f84d8\\\",\\\"ownerDisplayName\\\":' +
            '\\\"test_1518720219\\\",\\\"creationDate\\\":' +
            '\\\"2018-02-16T20:41:34.253Z\\\",\\\"mdBucketModelVersion\\\":5,' +
            '\\\"transient\\\":true,\\\"deleted\\\":false,' +
            '\\\"serverSideEncryption\\\":null,\\\"versioningConfiguration' +
            '\\\":null,\\\"locationConstraint\\\":\\\"us-east-1\\\",\\\"cors' +
            '\\\":null,\\\"replicationConfiguration\\\":null}\"}' },
        ] },
        { db: 'friday7', method: 7, entries: [
            { value: '{\"attributes\":\"{\\\"name\\\":\\\"friday7\\\",' +
            '\\\"owner\\\":\\\"94224c921648ada653f584f3caf42654ccf3f1cb' +
            'd2e569a24e88eb460f2f84d8\\\",\\\"ownerDisplayName\\\":' +
            '\\\"test_1518720219\\\",\\\"creationDate\\\":' +
            '\\\"2018-02-16T20:41:34.253Z\\\",\\\"mdBucketModelVersion\\\":5,' +
            '\\\"transient\\\":false,\\\"deleted\\\":false,' +
            '\\\"serverSideEncryption\\\":null,\\\"versioningConfiguration' +
            '\\\":null,\\\"locationConstraint\\\":\\\"us-east-1\\\",\\\"cors' +
            '\\\":null,\\\"replicationConfiguration\\\":null}\",' +
            '\"raftSession\":1}' },
        ] },
        { db: 'xxxfriday10', method: 0, entries: [
            { value: '{\"attributes\":\"{\\\"name\\\":\\\"xxxfriday10\\\",' +
            '\\\"owner\\\":\\\"94224c921648ada653f584f3caf42654ccf3f1cb' +
            'd2e569a24e88eb460f2f84d8\\\",\\\"ownerDisplayName\\\":' +
            '\\\"test_1518720219\\\",\\\"creationDate\\\":' +
            '\\\"2018-02-16T21:55:16.415Z\\\",\\\"mdBucketModelVersion\\\":5,' +
            '\\\"transient\\\":true,\\\"deleted\\\":false,' +
            '\\\"serverSideEncryption\\\":null,\\\"versioningConfiguration' +
            '\\\":null,\\\"locationConstraint\\\":\\\"us-east-1\\\",\\\"cors' +
            '\\\":null,\\\"replicationConfiguration\\\":null}\"}' },
        ] },
        { db: 'xxxfriday10', method: 7, entries: [
            { value: '{\"attributes\":\"{\\\"name\\\":\\\"xxxfriday10\\\",' +
            '\\\"owner\\\":\\\"94224c921648ada653f584f3caf42654ccf3f1cb' +
            'd2e569a24e88eb460f2f84d8\\\",\\\"ownerDisplayName\\\":' +
            '\\\"test_1518720219\\\",\\\"creationDate\\\":' +
            '\\\"2018-02-16T21:55:16.415Z\\\",\\\"mdBucketModelVersion\\\":5,' +
            '\\\"transient\\\":false,\\\"deleted\\\":false,' +
            '\\\"serverSideEncryption\\\":null,\\\"versioningConfiguration' +
            '\\\":null,\\\"locationConstraint\\\":\\\"us-east-1\\\",\\\"cors' +
            '\\\":null,\\\"replicationConfiguration\\\":null}\",' +
            '\"raftSession\":1}' },
        ] },
        { db: 'xxxfriday10', method: 8, entries: [
            {
                key: 'afternoon',
                value: '{\"owner-display-name\":\"test_1518720219\",' +
                    '\"owner-id\":\"94224c921648ada653f584f3caf42654ccf3f1cb' +
                    'd2e569a24e88eb460f2f84d8\",\"content-length\":0,' +
                    '\"content-md5\":\"d41d8cd98f00b204e9800998ecf8427e\",' +
                    '\"x-amz-version-id\":\"null\",' +
                    '\"x-amz-server-version-id\":\"\",\"x-amz-storage-class' +
                    '\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",' +
                    '\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",' +
                    '\"x-amz-server-side-encryption-customer-algorithm\":' +
                    '\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":' +
                    '{\"Canned\":\"private\",\"FULL_CONTROL\":[],' +
                    '\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":' +
                    '\"\",\"location\":null,\"isDeleteMarker\":false,\"tags' +
                    '\":{},\"replicationInfo\":{\"status\":\"\",\"backends\":' +
                    '[],\"content\":[],\"destination\":\"\",\"storageClass\":' +
                    '\"\",\"role\":\"\",\"storageType\":\"\",' +
                    '\"dataStoreVersionId\":\"\"},\"dataStoreName\":' +
                    '\"us-east-1\",\"last-modified\":\"2018-02-16T21:56:52.' +
                    '690Z\",\"md-model-version\":3}',
            },
        ] },
    ] };

const mockLogString = '\\/_\\/raft_sessions\\/[\\d]*\\/log\\?begin=' +
    '[\\d]*&limit=[\\d]*&targetLeader=false';
const mockLogURLRegex = new RegExp(mockLogString);

class MetadataMock {
    onRequest(req, res) {
        if (req.method !== 'GET') {
            res.writeHead(501);
            return res.end(JSON.stringify({
                error: 'mock server only supports GET requests',
            }));
        }
        if (/\/_\/raft_sessions\/[1-8]\/bucket/.test(req.url)) {
            const value = ['bucket1', 'bucket2'];
            res.writeHead(200, { 'content-type': 'application/json' });
            return res.end(JSON.stringify(value));
        } else if (/\/default\/attributes\/[a-z0-9]/.test(req.url)) {
            const bucketName = req.url.split('/');
            const bucketMd = dummyBucketMD[bucketName[bucketName.length - 1]];
            const dummyBucketMdObj = new BucketInfo(bucketMd._name,
                bucketMd._owner, bucketMd._ownerDisplayName,
                bucketMd._creationDate, bucketMd._mdBucketModelVersion,
                bucketMd._acl, bucketMd._transient, bucketMd._deleted,
                bucketMd._serverSideEncryption,
                bucketMd.versioningConfiguration, bucketMd._locationContraint,
                bucketMd._websiteConfiguration, bucketMd._cors,
                bucketMd._lifeCycle);
            return res.end(dummyBucketMdObj.serialize());
        } else if
            (/\/default\/bucket\/.*?listingType=Delimiter/.test(req.url)) {
            return res.end(JSON.stringify(objectList));
        } else if (/\/default\/bucket\/.*\/.*?/.test(req.url)) {
            return res.end(JSON.stringify({
                metadata: 'dogsAreGood',
            }));
        } else if (mockLogURLRegex.test(req.url)) {
            return res.end(JSON.stringify(mockLogs));
        }
        return res.end(JSON.stringify({
            error: 'invalid path',
        }));
    }
}

module.exports = MetadataMock;
