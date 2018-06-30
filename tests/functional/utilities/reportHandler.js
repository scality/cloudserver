const assert = require('assert');
const async = require('async');
const { backbeat } = require('arsenal');
const { RedisClient } = require('arsenal').metrics;

const { ConfigObject } = require('../../../lib/Config');
const config = new ConfigObject();

const { DummyRequestLogger } = require('../../unit/helpers');

const logger = new DummyRequestLogger();

const testLocationConstraints = {
    test: { type: 'aws_s3' },
    noshow: { type: 'aws_s3' },
};
config.setReplicationEndpoints(testLocationConstraints);
config.redis = { host: 'localhost', port: 6379 };

const {
    _crrRequest,
    getCRRStats,
} = require('../../../lib/utilities/reportHandler');


const sites = ['test', 'noshow'];
const testDetails = {
    httpMethod: 'GET',
    category: 'metrics',
    type: 'all',
    extensions: { crr: [...sites, 'all'] },
    method: 'getAllMetrics',
    dataPoints: ['bb:crr:ops', 'bb:crr:opsdone', 'bb:crr:bytes',
        'bb:crr:bytesdone'],
};

const testCRRKeys = [
    ['noshow:bb:crr:ops', 10000],
    ['noshow:bb:crr:bytes', 10000],
    ['noshow:bb:crr:opsdone', 5000],
    ['noshow:bb:crr:bytesdone', 5000],
    ['noshow:bb:crr:failed', 0],
    ['test:bb:crr:ops', 10000],
    ['test:bb:crr:bytes', 10000],
    ['test:bb:crr:opsdone', 5000],
    ['test:bb:crr:bytesdone', 5000],
    ['test:bb:crr:failed', 0],
];

const INTERVAL = 300;

function _normalizeTimestamp(d) {
    const m = d.getMinutes();
    return d.setMinutes(m - m % (Math.floor(INTERVAL / 60)), 0, 0);
}

function _buildKey(name, d) {
    return `${name}:${_normalizeTimestamp(d)}`;
}

function populateRedis(redisClient, cb) {
    const cmdKeys = testCRRKeys.map(entry => {
        const [id, val] = entry;
        const key = _buildKey(`${id}:requests`, new Date());
        return ['set', key, val];
    });
    return redisClient.batch(cmdKeys, cb);
}

describe('reportHandler::_crrRequest', function testSuite() {
    this.timeout(20000);
    let redisClient;
    let backbeatMetrics;

    before(done => {
        redisClient = new RedisClient(config.redis, logger);
        async.series([
            next => redisClient.clear(next),
            next => populateRedis(redisClient, next),
        ], err => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            backbeatMetrics = new backbeat.Metrics({
                redisConfig: config.redis,
                validSites: sites,
                internalStart: Date.now() - 900000,
            }, logger);
            return done();
        });
    });

    after(done => {
        redisClient.clear(err => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            redisClient._client.disconnect();
            backbeatMetrics._redisClient._client.disconnect();
            return done();
        });
    });

    it('should retrieve CRR metrics for all sites', done => {
        _crrRequest(backbeatMetrics, testDetails, 'all', logger,
        (err, res) => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            assert.deepStrictEqual(res, {
                completions: { count: 10000, size: 10000 },
                backlog: { count: 10000, size: 10000 },
                throughput: { count: 11, size: 11 },
            });
            return done();
        });
    });

    it('should retrieve CRR metrics for specific site', done => {
        _crrRequest(backbeatMetrics, testDetails, 'test', logger,
        (err, res) => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            assert.deepStrictEqual(res, {
                completions: { count: 5000, size: 5000 },
                backlog: { count: 5000, size: 5000 },
                throughput: { count: 5, size: 5 },
            });
            done();
        });
    });
});

describe('reportHandler::getCRRStats', function testSuite() {
    this.timeout(20000);
    let redisClient;

    before(done => {
        redisClient = new RedisClient(config.redis, logger);
        async.series([
            next => redisClient.clear(next),
            next => populateRedis(redisClient, next),
        ], err => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            return done();
        });
    });

    after(done => {
        redisClient.clear(err => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            redisClient._client.disconnect();
            return done();
        });
    });

    it('should retrieve CRR metrics', done => {
        getCRRStats(logger, (err, res) => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            assert.deepStrictEqual(res, {
                completions: { count: 10000, size: 10000 },
                backlog: { count: 10000, size: 10000 },
                throughput: { count: 11, size: 11 },
                byLocation: {
                    test: {
                        completions: { count: 5000, size: 5000 },
                        backlog: { count: 5000, size: 5000 },
                        throughput: { count: 5, size: 5 },
                    },
                    noshow: {
                        completions: { count: 5000, size: 5000 },
                        backlog: { count: 5000, size: 5000 },
                        throughput: { count: 5, size: 5 },
                    },
                },
            });
            return done();
        }, config);
    });
});
