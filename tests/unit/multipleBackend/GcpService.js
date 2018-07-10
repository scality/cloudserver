const assert = require('assert');
const http = require('http');
const { GCP } = require('../../../lib/data/external/GCP');

const httpPort = 8888;

// test values
const host = 'localhost:8888';
const Bucket = 'testrequestbucket';
const Key = 'testRequestKey';
const MultipartUpload = { Parts: [{ PartName: 'part' }] };
const CopySource = 'copyBucket/copyKey';
const accessKeyId = 'accesskey';
const secretAccessKey = 'secretaccesskey';

function handler(isPathStyle) {
    return (req, res) => {
        if (isPathStyle) {
            assert(req.headers.host, host);
            assert(req.url.includes(Bucket));
        } else {
            assert(req.headers.host, `${Bucket}.${host}`);
            assert(!req.url.includes(Bucket));
        }
        res.end();
    };
}

const invalidBucketNames = [
    '..',
    '.bucketname',
    'bucketname.',
    'bucketName.',
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    '256.256.256.256',
    '',
];

function badBucketNameHandler(req, res) {
    assert(req.headers.host, host);
    const bucketFromUrl = req.url.split('/')[1];
    assert.strictEqual(typeof bucketFromUrl, 'string');
    assert(invalidBucketNames.includes(bucketFromUrl));
    res.end();
}

const operations = [
    {
        op: 'headBucket',
        params: { Bucket },
    },
    {
        op: 'listObjects',
        params: { Bucket },
    },
    {
        op: 'listVersions',
        params: { Bucket },
    },
    {
        op: 'getBucketVersioning',
        params: { Bucket },
    },
    {
        op: 'headObject',
        params: { Bucket, Key },
    },
    {
        op: 'putObject',
        params: { Bucket, Key },
    },
    {
        op: 'getObject',
        params: { Bucket, Key },
    },
    {
        op: 'deleteObject',
        params: { Bucket, Key },
    },
    {
        op: 'composeObject',
        params: { Bucket, Key, MultipartUpload },
    },
    {
        op: 'copyObject',
        params: { Bucket, Key, CopySource },
    },
];

describe('GcpService request behavior', function testSuite() {
    this.timeout(120000);
    let httpServer;
    let client;

    before(done => {
        client = new GCP({
            endpoint: `http://${host}`,
            maxRetries: 0,
            s3ForcePathStyle: false,
            accessKeyId,
            secretAccessKey,
        });
        httpServer =
            http.createServer(badBucketNameHandler).listen(httpPort);
        httpServer.on('listening', done);
        httpServer.on('error', err => {
            process.stdout.write(`https server: ${err.stack}\n`);
            process.exit(1);
        });
    });

    after('Terminating Server', () => {
        httpServer.close();
    });


    invalidBucketNames.forEach(bucket => {
        it(`should not use dns-style if bucket isn't dns compatible: ${bucket}`,
        done => {
            client.headBucket({ Bucket: bucket }, err => {
                assert.ifError(err);
                done();
            });
        });
    });
});

describe('GcpService pathStyle tests', function testSuite() {
    this.timeout(120000);
    let httpServer;
    let client;

    before(done => {
        client = new GCP({
            endpoint: `http://${host}`,
            maxRetries: 0,
            s3ForcePathStyle: true,
            accessKeyId,
            secretAccessKey,
        });
        httpServer =
            http.createServer(handler(true)).listen(httpPort);
        httpServer.on('listening', done);
        httpServer.on('error', err => {
            process.stdout.write(`https server: ${err.stack}\n`);
            process.exit(1);
        });
    });

    after('Terminating Server', () => {
        httpServer.close();
    });

    operations.forEach(test => it(`GCP::${test.op}`, done => {
        client[test.op](test.params, err => {
            assert.ifError(err);
            done();
        });
    }));
});

describe('GcpService dnsStyle tests', function testSuite() {
    this.timeout(120000);
    let httpServer;
    let client;

    before(done => {
        client = new GCP({
            endpoint: `http://localhost:${httpPort}`,
            maxRetries: 0,
            s3ForcePathStyle: false,
            accessKeyId,
            secretAccessKey,
        });
        httpServer =
            http.createServer(handler(false)).listen(httpPort);
        httpServer.on('listening', done);
        httpServer.on('error', err => {
            process.stdout.write(`https server: ${err.stack}\n`);
            process.exit(1);
        });
    });

    after('Terminating Server', () => {
        httpServer.close();
    });

    operations.forEach(test => it(`GCP::${test.op}`, done => {
        client[test.op](test.params, err => {
            assert.ifError(err);
            done();
        });
    }));
});
