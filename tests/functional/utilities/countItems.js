const assert = require('assert');
const async = require('async');
const uuid = require('uuid/v4');

const BucketUtility =
    require('../aws-node-sdk/lib/utility/bucket-util');
const metadata = require('../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../unit/helpers');
const { mongoClient } = require('./mongoClient');

const runIfMongo = process.env.S3METADATA === 'mongodb' ?
    describe : describe.skip;

const logger = new DummyRequestLogger();

const objCnt = Math.floor(Math.random() * 10) + 10;
const bodySize = Math.floor(Math.random() * 100) + 10;
const outBucketCnt = Math.floor(Math.random() * 5) + 1;
const body = Buffer.alloc(bodySize);

const genUniqID = () => uuid().replace(/-/g, '');
const testBuckets = [
    {
        bucketName: `non-version-bucket-${genUniqID()}`,
        versioning: false,
    },
    {
        bucketName: `version-bucket-${genUniqID()}`,
        versioning: true,
    },
    {
        bucketName: `ingestion-bucket-${genUniqID()}`,
        versioning: false,
        ingestion: 'ingest',
    },
];

function populateDB(s3Client, cb) {
    const put = (bucketName, cb) => {
        async.timesLimit(objCnt, 5,
        (n, next) => s3Client.putObject({
            Bucket: bucketName,
            Key: `key-${n}`,
            Body: body },
        next), cb);
    };

    async.eachLimit(testBuckets, 1, (b, next) => {
        const { bucketName, versioning, ingestion } = b;
        async.series({
            createBucket: done =>
                s3Client.createBucket({ Bucket: bucketName,
                    CreateBucketConfiguration: { LocationConstraint:
                    `us-east-1:${ingestion}` } }, done),
            putBucketVersioning: done => {
                if (!versioning || ingestion) {
                    return done();
                }
                return s3Client.putBucketVersioning({
                    Bucket: b.bucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }, done);
            },
            putObjects: done => put(bucketName, done),
            putVersions: done => {
                if (!versioning) {
                    return done();
                }
                return put(bucketName, done);
            },
        }, next);
    }, cb);
}

function cleanDB(bucketUtil, cb) {
    async.eachLimit(testBuckets, 1, (b, next) => {
        const { bucketName } = b;
        const emptyBucket =
            async.asyncify(bucketUtil.empty.bind(bucketUtil));
        const deleteBucket =
            async.asyncify(bucketUtil.deleteOne.bind(bucketUtil));
        async.series([
            done => emptyBucket(bucketName, done),
            done => deleteBucket(bucketName, done),
        ], next);
    }, cb);
}

const ownerCanonicalId =
    '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';

const refResults = {
    objects: testBuckets.length * objCnt,
    versions: objCnt,
    buckets: testBuckets.length,
    bucketList: testBuckets.map(b => ({
        name: b.bucketName,
        location: 'us-east-1',
        isVersioned: b.ingestion ? true : b.versioning,
        ownerCanonicalId,
        ingestion: b.ingestion ? b.ingestion === 'ingest' : false,
    })),
    dataManaged: {
        total: { curr: testBuckets.length * objCnt * bodySize,
            prev: objCnt * bodySize },
        byLocation: {
            'us-east-1':
                { curr: testBuckets.length * objCnt * bodySize,
                    prev: objCnt * bodySize },
        },
    },
};

const sortFn = (a, b) => {
    if (a.name < b.name) {
        return -1;
    }
    if (a.name > b.name) {
        return 1;
    }
    return 0;
};

function assertResults(res, expRes) {
    Object.keys(expRes).forEach(key => {
        if (Array.isArray(expRes[key]) && Array.isArray(res[key])) {
            assert.deepStrictEqual(
                res[key].sort(sortFn), expRes[key].sort(sortFn));
        } else {
            assert.deepStrictEqual(res[key], expRes[key]);
        }
    });
}

function createOutBuckets(buckets, cb) {
    async.eachLimit(buckets, 5, (b, next) => {
        mongoClient.createBucket(b, 'us-east-1', next);
    }, cb);
}

function deleteOutBuckets(buckets, cb) {
    async.eachLimit(buckets, 5, (b, next) => {
        mongoClient.deleteBucket(b, next);
    }, cb);
}

runIfMongo('reportHandler::countItems', function testSuite() {
    this.timeout(200000);
    const bucketUtil = new BucketUtility('default', {});

    before(done => populateDB(bucketUtil.s3, done));
    after(done => cleanDB(bucketUtil, done));

    it('should return correct countItems report', done => {
        async.series([
            next => metadata.setup(next),
            next => metadata.countItems(logger, (err, res) => {
                assertResults(res, refResults);
                next();
            }),
        ], done);
    });

    describe('with "out-of-band" bucket updates', () => {
        const outBuckets = Array.from(Array(outBucketCnt).keys()).map(
            () => `out-of-band-bucket${genUniqID()}`);

        before(done => {
            async.series([
                next => mongoClient.connectClient(next),
                next => createOutBuckets(outBuckets, next),
            ], done);
        });

        after(done => {
            async.series([
                next => deleteOutBuckets(outBuckets, next),
                next => mongoClient.disconnectClient(next),
            ], done);
        });

        it('should retrieve update bucket list', done => {
            async.series([
                next => metadata.setup(next),
                next => metadata.countItems(logger, (err, res) => {
                    const expResults = JSON.parse(JSON.stringify(refResults));
                    expResults.buckets += outBucketCnt;
                    outBuckets.forEach(b => {
                        expResults.bucketList.push({
                            name: b,
                            location: 'us-east-1',
                            isVersioned: false,
                            ownerCanonicalId,
                            ingestion: false,
                        });
                    });
                    assertResults(res, expResults);
                    next();
                }),
            ], done);
        });
    });
});
