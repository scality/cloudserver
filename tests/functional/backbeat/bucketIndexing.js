const assert = require('assert');
const async = require('async');

const { makeRequest } = require('../../functional/raw-node/utils/makeRequest');
const BucketUtility =
      require('../../functional/aws-node-sdk/lib/utility/bucket-util');
const { runIfMongo } = require('./utils');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

const backbeatAuthCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const TEST_BUCKET = 'backbeatbucket';

function indexDeleteRequest(payload, bucket, cb) {
    makeRequest({
        authCredentials: backbeatAuthCredentials,
        hostname: ipAddress,
        port: 8000,
        method: 'POST',
        path:
            `/_/backbeat/index/${bucket}`,
        headers: {},
        jsonResponse: true,
        requestBody: JSON.stringify(payload),
        queryObj: { operation: 'delete' },
    }, cb);
}

function indexPutRequest(payload, bucket, cb) {
    makeRequest({
        authCredentials: backbeatAuthCredentials,
        hostname: ipAddress,
        port: 8000,
        method: 'POST',
        path:
            `/_/backbeat/index/${bucket}`,
        headers: {},
        jsonResponse: true,
        requestBody: JSON.stringify(payload),
        queryObj: { operation: 'add' },
    }, cb);
}

function indexGetRequest(bucket, cb) {
    makeRequest({
        authCredentials: backbeatAuthCredentials,
        hostname: ipAddress,
        port: 8000,
        method: 'GET',
        path:
            `/_/backbeat/index/${bucket}`,
        headers: {},
        jsonResponse: true,
    }, cb);
}

const indexReqObject = [
    {
        keys: [
            { key: 'value.last-modified', order: 1 },
            { key: '_id', order: 1 },
        ],
        name: 'lifecycleLastModifiedPrefixed',
    },
    {
        keys: [
            { key: 'value.dataStoreName', order: 1 },
            { key: 'value.last-modified', order: 1 },
            { key: '_id', order: 1 },
        ],
        name: 'lifecycleDataStoreNamePrefixed',
    },
];

const indexRespObject = [
    {
        name: '_id_',
        keys: [
            { key: '_id', order: 1 },
        ]
    },
    {
        keys: [
            { key: 'value.last-modified', order: 1 },
            { key: '_id', order: 1 },
        ],
        name: 'lifecycleLastModifiedPrefixed',
    },
    {
        keys: [
            { key: 'value.dataStoreName', order: 1 },
            { key: 'value.last-modified', order: 1 },
            { key: '_id', order: 1 },
        ],
        name: 'lifecycleDataStoreNamePrefixed',
    },
];

runIfMongo('Indexing Routes', () => {
    let bucketUtil;
    let s3;

    before(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.createBucket({ Bucket: TEST_BUCKET }).promise()
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });

    // after(done => {
    //     bucketUtil.empty(TEST_BUCKET)
    //         .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
    //         .then(() => done());
    // });

    it('should reject non-authenticated requests', done => {
        makeRequest({
            hostname: ipAddress,
            port: 8000,
            method: 'GET',
            path:
                '/_/backbeat/index/testbucket',
            headers: {},
            jsonResponse: true,
        }, err => {
            assert(err);
            assert.strictEqual(err.code, 'AccessDenied');
            done();
        });
    });

    it('should return error: invalid payload - empty', done => {
        indexPutRequest([], TEST_BUCKET, err => {
            assert(err);
            assert.strictEqual(err.code, 'BadRequest');
            done();
        });
    });

    it('should return error: invalid payload - missing name', done => {
        indexPutRequest([{ key: [['test', 1]] }], TEST_BUCKET, err => {
            assert(err);
            assert.strictEqual(err.code, 'BadRequest');
            done();
        });
    });

    it('should return error: invalid payload - missing key', done => {
        indexPutRequest([{ name: 'test' }], TEST_BUCKET, err => {
            assert(err);
            assert.strictEqual(err.code, 'BadRequest');
            done();
        });
    });

    it('should successfully add indexes', done => {
        async.series([
            next => {
                indexPutRequest(indexReqObject, TEST_BUCKET, err => {
                    assert.ifError(err);
                    next();
                });
            },
            next => {
                indexGetRequest(TEST_BUCKET, (err, data) => {
                    assert.ifError(err);
                    const res = JSON.parse(data.body);
                    assert.deepStrictEqual(res.Indexes, indexRespObject);
                    next();
                });
            },
        ], done);
    });

    it('should successfully delete indexes', done => {
        async.series([
            next => {
                indexPutRequest(indexReqObject, TEST_BUCKET, err => {
                    assert.ifError(err);
                    next();
                });
            },
            next => {
                indexGetRequest(TEST_BUCKET, (err, data) => {
                    assert.ifError(err);
                    const res = JSON.parse(data.body);
                    assert.deepStrictEqual(res.Indexes, indexRespObject);
                    next();
                });
            },
            next => {
                indexDeleteRequest(indexReqObject, TEST_BUCKET, err => {
                    assert.ifError(err);
                    next();
                });
            },
            next => {
                indexGetRequest(TEST_BUCKET, (err, data) => {
                    assert.ifError(err);
                    const res = JSON.parse(data.body);
                    assert.deepStrictEqual(res.Indexes, [
                        {
                            name: '_id_',
                            keys: [{ key: '_id', order: 1 }],
                        }
                    ]);
                    next();
                });
            },
        ], done);
    });
});

