const assert = require('assert');
const async = require('async');
const http = require('http');
const Redis = require('ioredis');
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
    getReplicationStates,
} = require('../../../lib/utilities/reportHandler');


const sites = ['test', 'noshow'];
const testDetails = {
    httpMethod: 'GET',
    category: 'metrics',
    type: 'all',
    extensions: { crr: [...sites, 'all'] },
    method: 'getAllMetrics',
    dataPoints: ['bb:crr:ops', 'bb:crr:opsdone', 'bb:crr:opsfail',
        'bb:crr:bytes', 'bb:crr:bytesdone', 'bb:crr:bytesfail'],
};

const testCRRKeys = [
    ['noshow:bb:crr:ops', 10000],
    ['noshow:bb:crr:bytes', 10000],
    ['noshow:bb:crr:opsdone', 5000],
    ['noshow:bb:crr:bytesdone', 5000],
    ['noshow:bb:crr:opsfail', 1000],
    ['noshow:bb:crr:bytesfail', 1000],
    ['noshow:bb:crr:failed', 0],
    ['test:bb:crr:ops', 10000],
    ['test:bb:crr:bytes', 10000],
    ['test:bb:crr:opsdone', 5000],
    ['test:bb:crr:bytesdone', 5000],
    ['test:bb:crr:opsfail', 1000],
    ['test:bb:crr:bytesfail', 1000],
    ['test:bb:crr:failed', 0],
];

const INTERVAL = 300;
const EXPIRY = 86400;

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

function assertResults(res) {
    const testRes = res;
    delete testRes.clients;
    assert.deepStrictEqual(testRes, {
        completions: { count: 10000, size: 10000 },
        failures: { count: 2000, size: 2000 },
        backlog: { count: 10000, size: 10000 },
        throughput: { count: 11, size: 11 },
        byLocation: {
            test: {
                completions: { count: 5000, size: 5000 },
                failures: { count: 1000, size: 1000 },
                backlog: { count: 5000, size: 5000 },
                throughput: { count: 5, size: 5 },
            },
            noshow: {
                completions: { count: 5000, size: 5000 },
                failures: { count: 1000, size: 1000 },
                backlog: { count: 5000, size: 5000 },
                throughput: { count: 5, size: 5 },
            },
        },
    });
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
            assert.ifError(err);
            backbeatMetrics = new backbeat.Metrics({
                redisConfig: config.redis,
                validSites: sites,
                internalStart: Date.now() - (EXPIRY * 1000),
            }, logger);
            return done();
        });
    });

    after(done => {
        async.series({
            clearRedis: next =>
                redisClient.clear(next),
            disconnectRedisPopulator: next =>
                redisClient.disconnect(next),
            disconnectBackbeatMetrics: next =>
                backbeatMetrics.disconnect(next),
        }, done);
    });

    it('should retrieve CRR metrics for all sites', done => {
        _crrRequest(backbeatMetrics, testDetails, 'all', logger,
        (err, res) => {
            assert.ifError(err);
            assert.deepStrictEqual(res, {
                completions: { count: 10000, size: 10000 },
                failures: { count: 2000, size: 2000 },
                backlog: { count: 10000, size: 10000 },
                throughput: { count: 11, size: 11 },
            });
            return done();
        });
    });

    it('should retrieve CRR metrics for specific site', done => {
        _crrRequest(backbeatMetrics, testDetails, 'test', logger,
        (err, res) => {
            assert.ifError(err);
            assert.deepStrictEqual(res, {
                completions: { count: 5000, size: 5000 },
                failures: { count: 1000, size: 1000 },
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

    beforeEach(done => {
        redisClient = new RedisClient(config.redis, logger);
        async.series([
            next => redisClient.clear(next),
            next => populateRedis(redisClient, next),
        ], err => {
            assert.ifError(err);
            return done();
        });
    });

    afterEach(done => {
        async.series({
            clearRedis: next =>
                redisClient.clear(next),
            disconnectRedisPopulator: next =>
                redisClient.disconnect(next),
        }, done);
    });

    it('should disconnect backbeat.metrics client on report completion',
    done => {
        const redisChecker = new Redis({ host: 'localhost', port: 6379 });
        redisChecker.once('error', err => {
            redisClient.disconnect();
            done(err);
        });
        let preCheckCount;
        async.series({
            preCheck: next => {
                redisChecker.client('list', (err, res) => {
                    assert.ifError(err);
                    const clients = res.split('\n').filter(c => !!c);
                    preCheckCount = clients.length;
                    next();
                });
            },
            checkResults: next => {
                getCRRStats(logger, (err, res) => {
                    assert.ifError(err);
                    assert(res.clients);
                    const clients = res.clients.split('\n').filter(c => !!c);
                    assert.strictEqual(clients.length, preCheckCount + 1);
                    assertResults(res);
                    next();
                }, config);
            },
            postCheck: next => {
                redisChecker.client('list', (err, res) => {
                    assert.ifError(err);
                    const clients = res.split('\n').filter(c => !!c);
                    assert.strictEqual(clients.length, preCheckCount);
                    next();
                });
            },
            disconnectTestClient: next => redisChecker.quit(next),
        }, done);
    });

    it('should retrieve CRR metrics', done => {
        getCRRStats(logger, (err, res) => {
            assert.ifError(err);
            assertResults(res);
            return done();
        }, config);
    });
});

describe('reportHandler::getReplicationStates', function testSuite() {
    this.timeout(20000);
    const testPort = '4242';
    let httpServer;

    const expectedStatusResults = {
        location1: 'enabled',
        location2: 'disabled',
    };

    const expectedScheduleResults = {
        location1: 'none',
        location2: new Date(),
    };

    function requestHandler(req, res) {
        switch (req.url) {
        case '/_/crr/status':
            res.write(JSON.stringify(expectedStatusResults));
            break;
        case '/_/crr/resume/all':
            res.write(JSON.stringify(expectedScheduleResults));
            break;
        default:
            break;
        }
        res.end();
    }


    function requestFailHandler(req, res) {
        const testError = {
            code: 404,
            description: 'reportHandler test error',
        };
        // eslint-disable-next-line no-param-reassign
        res.statusCode = 404;
        res.write(JSON.stringify(testError));
        res.end();
    }

    describe('Test Request Failure Cases', () => {
        before(done => {
            httpServer = http.createServer(requestFailHandler).listen(testPort);
            httpServer.on('listening', done);
            httpServer.on('error', err => {
                process.stdout.write(`https server: ${err.stack}\n`);
                process.exit(1);
            });
        });

        after('Terminating Server', () => {
            httpServer.close();
        });

        it('should return empty object if a request error occurs', done => {
            getReplicationStates(logger, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {});
                done();
            }, { host: 'nonexisthost', port: testPort });
        });

        it('should return empty object if response status code is >= 400',
        done => {
            getReplicationStates(logger, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {});
                done();
            }, { host: 'localhost', port: testPort });
        });
    });

    describe('Test Request Success Cases', () => {
        before(done => {
            httpServer = http.createServer(requestHandler).listen(testPort);
            httpServer.on('listening', done);
            httpServer.on('error', err => {
                process.stdout.write(`https server: ${err.stack}\n`);
                process.exit(1);
            });
        });

        after('Terminating Server', () => {
            httpServer.close();
        });

        it('should return correct results', done => {
            getReplicationStates(logger, (err, res) => {
                const expectedResults = {
                    states: {
                        location1: 'enabled',
                        location2: 'disabled',
                    },
                    schedules: {
                        location2: expectedScheduleResults.location2,
                    },
                };
                assert.ifError(err);
                assert.deepStrictEqual(res, expectedResults);
                done();
            }, { host: 'localhost', port: testPort });
        });
    });
});
