'use strict'; // eslint-disable-line strict

/*
 * The file contains multiple scenarios for measuring performance of single S3
 * 1. Lowest latency: single bucket
 * 2. Max number of operations/s: single & multiple buckets
 * 3. Max throughput: single & multiple buckets
 * Only sequential simulation is executed, i.e. at a time, a type of request
 *  with a single combination of (number of parallel requests, object size) is
 *  executed.
 */

const numCPUs = require('os').cpus().length;

const config = require('../../lib/Config');
const runS3Blaster = require('s3blaster').RunS3Blaster;

const numWorkers = Math.min(numCPUs, 8);
const S3Port = config.port;
const maxBktsNb = 30;
const outputDir = `${__dirname}/results`;

// params.paralReqs is an array of numbers of parallel requests sent from each
// worker. Hence, if there are multiple workers, total numbers of parallel
// requests are equal such numbers multipled with number of workers
const totalParalReqs = [32, 64, 128, 256, 512];
const paralReqs = totalParalReqs.map(num =>
                    Math.max(1, Math.floor(num / numWorkers)));

const params = {
    port: S3Port,
    forksNb: 1,
    bucketsNb: 1,
    bucketPrefix: 'bkts3std',
    objectsNb: 1e3,
    fillObjs: false,
    sizes: [0, 10],
    unit: 'KB',
    requests: 'put,get,delete',
    paralReqs: 1,
    schedule: 'each',
    simulDelay: 5,
    nextKey: 'seq',
    observationsNb: 1e6,
    workOnCurrObjs: false,
    runTime: 10,
    dontCleanDB: true,
    ssm: false,
    dirPath: outputDir,
    output: 's3standard',
    message: 'Performance measurement of S3 Server',
};

/* Find lowest latency */
describe('Single bucket, lowest latency', function fn() {
    this.timeout(0);

    before(() => {
        params.statsFolder = 'lowestLatency';
        params.forksNb = 1;
        params.paralReqs = [1];
    });

    it('Put, get, then delete', done => {
        params.output = 'lowestLatency';
        process.nextTick(runS3Blaster.start, params, done);
    });
});

/* Find max #operations/s */
describe('Single bucket, max ops/s', function fn() {
    this.timeout(0);

    before(() => {
        params.statsFolder = 'maxOps';
        params.forksNb = numWorkers;
        params.paralReqs = paralReqs;
    });

    it('Put, get, then delete', done => {
        params.output = 'singleBucket_maxOps';
        process.nextTick(runS3Blaster.start, params, done);
    });
});

describe('Multiple buckets, max ops/s', function fn() {
    this.timeout(0);

    before(() => {
        params.statsFolder = 'maxOps';
        params.bucketsNb = maxBktsNb;
    });

    it('Put, get, then delete', done => {
        params.output = `bkt${params.bucketsNb}_maxOps`;
        process.nextTick(runS3Blaster.start, params, done);
    });
});

/* Find throughput */
describe('Single bucket, throughput', function fn() {
    this.timeout(0);

    before(() => {
        params.statsFolder = 'throughput';
        params.sizes = [1];
        params.unit = 'MB';
        params.bucketsNb = 1;
    });

    it('Put, then get', done => {
        params.output = 'singleBucket_throughput';
        process.nextTick(runS3Blaster.start, params, done);
    });
});

describe('Multiple buckets, throughput', function fn() {
    this.timeout(0);

    before(() => {
        params.statsFolder = 'throughput';
        params.bucketsNb = maxBktsNb;
    });

    it('Put, get, then delete', done => {
        params.output = `bkt${params.bucketsNb}_throughput`;
        process.nextTick(runS3Blaster.start, params, done);
    });
});

/*
 * Clean databases
 */
describe('Clean databases of simulation', function fn() {
    this.timeout(0);

    before(() => {
        params.forksNb = 1;
        params.statsFolder = 'clean';
        params.paralReqs = [128];
        params.dontCleanDB = false;
        params.schedule = 'each';
        params.fillObjs = false;
        params.requests = 'delete';
        params.observationsNb = 1;
    });

    it('Clean databases', done => {
        params.output = 'cleanDB';
        process.nextTick(runS3Blaster.start, params, done);
    });
});
