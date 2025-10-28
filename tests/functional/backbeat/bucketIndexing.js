const assert = require('assert');
const async = require('async');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const { makeRequest } = require('../../functional/raw-node/utils/makeRequest');
const BucketUtility =
      require('../../functional/aws-node-sdk/lib/utility/bucket-util');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

const bucketUtil = new BucketUtility('default', {});
const s3 = bucketUtil.s3;

let credentials = null;
let backbeatAuthCredentials = null;

async function getCredentials() {
        const creds = await s3.config.credentials();
        credentials = {
            accessKey: creds.accessKeyId,
            secretKey: creds.secretAccessKey,
        };
    return credentials;
}

const TEST_BUCKET = 'bucket-for-bucket-indexing';

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

const describeIfMongo = process.env.S3METADATA === 'mongodb' ? describe : describe.skip;
const describeIfNotMongo = process.env.S3METADATA !== 'mongodb' ? describe : describe.skip;

describe('Indexing Routes', () => {
    before(done => {
        getCredentials()
            .then(creds => {
                backbeatAuthCredentials = creds;
                return s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }));
            })
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                done(err);
            });
    });

    after(async () => {
        await bucketUtil.empty(TEST_BUCKET);
        await s3.send(new DeleteBucketCommand({ Bucket: TEST_BUCKET }));
    });

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

    describeIfMongo('with mongodb metadata', () => {
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

    describeIfNotMongo('without mongodb metadata', () => {
        it('should return NotImplemented add indexes', done => {
            indexPutRequest(indexReqObject, TEST_BUCKET, err => {
                assert(err);
                assert.strictEqual(err.code, 'NotImplemented');
                assert.strictEqual(err.statusCode, 501);
                done();
            });
        });

        it('should return NotImplemented get indexes', done => {
            indexGetRequest(TEST_BUCKET, err => {
                assert(err);
                assert.strictEqual(err.code, 'NotImplemented');
                assert.strictEqual(err.statusCode, 501);
                done();
            });
        });

        it('should return NotImplemented delete indexes', done => {
            indexDeleteRequest(indexReqObject, TEST_BUCKET, err => {
                assert(err);
                assert.strictEqual(err.code, 'NotImplemented');
                assert.strictEqual(err.statusCode, 501);
                done();
            });
        });
    });
});

