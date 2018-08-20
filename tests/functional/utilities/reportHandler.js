const assert = require('assert');
const http = require('http');

const { ConfigObject } = require('../../../lib/Config');
const config = new ConfigObject();

const { DummyRequestLogger } = require('../../unit/helpers');

const logger = new DummyRequestLogger();

const testLocationConstraints = {
    site1: { type: 'aws_s3' },
    site2: { type: 'aws_s3' },
};
config.setReplicationEndpoints(testLocationConstraints);
config.backbeat = { host: 'localhost', port: 4242 };

const {
    _crrRequest,
    getCRRStats,
    getReplicationStates,
} = require('../../../lib/utilities/reportHandler');

const expectedResultsRef = {
    completions: { count: 10000, size: 10000 },
    failures: { count: 2000, size: 2000 },
    backlog: { count: 10000, size: 10000 },
    throughput: { count: 11, size: 11 },
    byLocation: {
        site1: {
            completions: { count: 5000, size: 5000 },
            failures: { count: 1000, size: 1000 },
            backlog: { count: 5000, size: 5000 },
            throughput: { count: 5, size: 5 },
        },
        site2: {
            completions: { count: 5000, size: 5000 },
            failures: { count: 1000, size: 1000 },
            backlog: { count: 5000, size: 5000 },
            throughput: { count: 5, size: 5 },
        },
    },
};

const requestResults = {
    all: {
        completions: { results: { count: 10000, size: 10000 } },
        failures: { results: { count: 2000, size: 2000 } },
        backlog: { results: { count: 10000, size: 10000 } },
        throughput: { results: { count: 11, size: 11 } },
    },
    site1: {
        completions: { results: { count: 5000, size: 5000 } },
        failures: { results: { count: 1000, size: 1000 } },
        backlog: { results: { count: 5000, size: 5000 } },
        throughput: { results: { count: 5, size: 5 } },
    },
    site2: {
        completions: { results: { count: 5000, size: 5000 } },
        failures: { results: { count: 1000, size: 1000 } },
        backlog: { results: { count: 5000, size: 5000 } },
        throughput: { results: { count: 5, size: 5 } },
    },
};

const expectedStatusResults = {
    location1: 'enabled',
    location2: 'disabled',
};

const expectedScheduleResults = {
    location1: 'none',
    location2: new Date(),
};

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

function requestHandler(req, res) {
    const { url } = req;
    if (url.startsWith('/_/metrics/crr/')) {
        const site = url.split('/_/metrics/crr/')[1] || '';
        if (requestResults[site]) {
            res.write(JSON.stringify(requestResults[site]));
        }
    } else {
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
    }
    res.end();
}

describe('reportHandler::_crrRequest', function testSuite() {
    this.timeout(20000);
    const testPort = '4242';
    let httpServer;

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
            const endpoint = 'http://nonexists:4242';
            _crrRequest(endpoint, 'all', logger, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {});
                done();
            });
        });

        it('should return empty object if response status code is >= 400',
        done => {
            const endpoint = 'http://localhost:4242';
            _crrRequest(endpoint, 'all', logger, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {});
                done();
            });
        });
    });

    describe('Test Request Success Cases', () => {
        const endpoint = 'http://localhost:4242';
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

        it('should return correct', done => {
            _crrRequest(endpoint, 'site1', logger, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(
                    res, expectedResultsRef.byLocation.site1);
                done();
            });
        });
    });
});

describe('reportHandler::getCRRStats', function testSuite() {
    this.timeout(20000);
    const testPort = '4242';
    let httpServer;

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
            getCRRStats(logger, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, expectedResultsRef);
                done();
            }, config);
        });
    });
});


describe('reportHandler::getReplicationStates', function testSuite() {
    this.timeout(20000);
    const testPort = '4242';
    let httpServer;

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
